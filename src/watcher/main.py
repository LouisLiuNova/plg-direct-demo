import os
import time
import json
import re
import logging
import requests
from datetime import datetime, timedelta
from prometheus_client import start_http_server, Gauge
from pathlib import Path

# --- 配置部分 ---
LOKI_URL = os.getenv("LOKI_URL", "http://loki:3100")
CHECK_INTERVAL_SECONDS = int(
    os.getenv("CHECK_INTERVAL_SECONDS", "300")
)  # 默认5分钟运行一次
# 检查窗口偏移量：例如设置为 300，表示检查 (当前时间-10分钟) 到 (当前时间-5分钟) 的数据
# 这样留出5分钟的缓冲期给日志上报
WINDOW_OFFSET_SECONDS = int(os.getenv("WINDOW_OFFSET_SECONDS", "300"))
# 扩展窗口大小,即为保证数据的完整性,扩展查询的时间区间
WINDOW_EXTEND_SECONDS = int(os.getenv("WINDOW_EXTEND_SECONDS", "120"))
assert (
    WINDOW_EXTEND_SECONDS <= WINDOW_OFFSET_SECONDS
)  # To sure will not query the future messages
REPORT_DIR = os.getenv("REPORT_DIR", "/data/reports")

# --- Prometheus Metrics ---
GAUGE_LOST_FILES = Gauge(
    "log_audit_lost_files_count", "Number of files forwarded but not processed"
)
GAUGE_TOTAL_FORWARD = Gauge(
    "log_audit_forward_count", "Total files forwarded in the window"
)
GAUGE_TOTAL_PROCESS = Gauge(
    "log_audit_process_count", "Total files processed in the window"
)

# --- 日志配置 ---
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# --- 正则表达式 ---
# 文件名示例: 20260128101647964993_tz01_91458258_.log
# 提取文件名中的时间戳 (前14位: YYYYMMDDHHmmss)
FILENAME_TS_PATTERN = re.compile(r"(\d{14})\d*_.+")

# Process Service: filePath=/cacheproxy/.../xxx.log成功
PROCESS_PATTERN = re.compile(r"filePath=([\w/.-]+)成功")

# Forward Service: Rename trigger hard link /cacheproxy/.../xxx.log to process
FORWARD_PATTERN = re.compile(r"Rename trigger hard link ([\w/.-]+) to process")


def get_loki_logs(query, start_ts, end_ts, limit=5000):
    """从Loki获取日志"""
    url = f"{LOKI_URL}/loki/api/v1/query_range"
    # Loki API 使用纳秒时间戳
    params = {
        "query": query,
        "start": int(start_ts * 1e9),
        "end": int(end_ts * 1e9),
        "limit": limit,
        "direction": "FORWARD",
    }

    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        return data.get("data", {}).get("result", [])
    except Exception as e:
        logger.error(f"Error querying Loki: {e}")
        return []


def extract_filename_and_ts(filepath):
    """从完整路径提取文件名和基于文件名的datetime对象"""
    basename = os.path.basename(filepath)
    match = FILENAME_TS_PATTERN.match(basename)
    if match:
        ts_str = match.group(1)
        try:
            return basename, datetime.strptime(ts_str, "%Y%m%d%H%M%S")
        except ValueError:
            return basename, None
    return basename, None


def run_audit():
    now = datetime.now()
    # 定义我们要审计的“文件时间”窗口
    # 比如现在是 10:10，间隔5分钟，偏移5分钟。
    # window_end = 10:05, window_start = 10:00
    window_end_dt = now - timedelta(seconds=WINDOW_OFFSET_SECONDS)
    window_start_dt = window_end_dt - timedelta(seconds=CHECK_INTERVAL_SECONDS)

    logger.info(
        f"Starting audit for file time window: {window_start_dt} to {window_end_dt}"
    )

    # --- 1. 获取 Forward Service 日志 ---
    # 为了确保不漏掉日志，Loki查询的时间范围要比文件时间窗口宽一点 (前后各加WINDOW_EXTEND_SECONDS秒buffer)
    loki_query_start = window_start_dt.timestamp() - WINDOW_EXTEND_SECONDS
    loki_query_end = window_end_dt.timestamp() + WINDOW_EXTEND_SECONDS

    forward_files = set()

    # 查询 Forward Service
    q_forward = '{service="forward_svc"} |= "Rename trigger hard link"'
    logs = get_loki_logs(q_forward, loki_query_start, loki_query_end)

    for stream in logs:
        for value in stream["values"]:
            log_line = value[1]  # value[0] is timestamp, value[1] is line
            try:
                # 解析 JSON
                log_json = json.loads(log_line)
                msg = log_json.get("msg", "")

                # 正则提取路径
                match = FORWARD_PATTERN.search(msg)
                if match:
                    filepath = match.group(1)
                    fname, ftime = extract_filename_and_ts(filepath)

                    # 关键逻辑：只统计文件名时间戳落在目标窗口内的文件
                    if ftime and window_start_dt <= ftime < window_end_dt:
                        forward_files.add(fname)
            except json.JSONDecodeError:
                continue  # 忽略非JSON行
            except Exception as e:
                logger.warning(f"Error parsing forward log: {e}")

    # --- 2. 获取 Process Service 日志 ---
    process_files = set()
    q_process = '{service="process_svc"} |= "处理文件" |= "成功"'
    logs = get_loki_logs(q_process, loki_query_start, loki_query_end)

    for stream in logs:
        for value in stream["values"]:
            log_line = value[1]
            # 正则提取路径 (非JSON格式)
            match = PROCESS_PATTERN.search(log_line)
            if match:
                filepath = match.group(1)
                fname, ftime = extract_filename_and_ts(filepath)

                if ftime and window_start_dt <= ftime < window_end_dt:
                    process_files.add(fname)

    # --- 3. 比对与统计 ---
    lost_files = forward_files - process_files
    lost_count = len(lost_files)

    logger.info(
        f"Audit Result: Forwarded={len(forward_files)}, Processed={len(process_files)}, Lost={lost_count}"
    )

    # --- 4. 更新 Prometheus Metrics ---
    GAUGE_LOST_FILES.set(lost_count)
    GAUGE_TOTAL_FORWARD.set(len(forward_files))
    GAUGE_TOTAL_PROCESS.set(len(process_files))

    # --- 5. 生成报告文件 ---
    if not os.path.exists(REPORT_DIR):
        os.makedirs(REPORT_DIR)

    timestamp_str = window_start_dt.strftime("%Y%m%d%H%M%S")

    # 只有当有丢失文件时，或者强制生成报告时写入
    report_data = {
        "audit_window_start": window_start_dt.isoformat(),
        "audit_window_end": window_end_dt.isoformat(),
        "forward_count": len(forward_files),
        "process_count": len(process_files),
        "lost_count": lost_count,
        "lost_files_path": str(
            Path(REPORT_DIR) / Path(f"lost_files_{timestamp_str}.txt")
        ),
    }

    # 写入 JSON 报告
    json_path = os.path.join(REPORT_DIR, f"report_{timestamp_str}.json")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(report_data, f, indent=2, ensure_ascii=False)

    # 如果有丢失，单独写一个列表文件方便核查
    if lost_count > 0:
        list_path = os.path.join(REPORT_DIR, f"lost_files_{timestamp_str}.txt")
        with open(list_path, "w", encoding="utf-8") as f:
            for item in lost_files:
                f.write(f"{item}\n")
        logger.warning(f"Found {lost_count} lost files! List saved to {list_path}")
    logger.info(
        f"Audit report: {len(forward_files)} forwarded, {len(process_files)} processed, {lost_count} lost. Report saved to {json_path}"
    )


if __name__ == "__main__":
    # 启动 Prometheus Metrics Server
    start_http_server(8000)
    logger.info("Metrics server started on port 8000")

    # 首次启动立即运行一次，或者先sleep
    logger.info(f"Service started. Interval: {CHECK_INTERVAL_SECONDS}s")
    logger.info(f"Sleep {CHECK_INTERVAL_SECONDS} secs first to wait for enough datas")
    time.sleep(CHECK_INTERVAL_SECONDS)
    logger.info(
        f"Sleep {WINDOW_EXTEND_SECONDS}*2 secs to wait for enough extend window"
    )
    time.sleep(WINDOW_EXTEND_SECONDS * 2)

    while True:
        logger.info(f"Start to run an audit")
        try:
            run_audit()
        except Exception as e:
            logger.error(f"Audit loop failed: {e}")
        logger.info(f"Audit completed. Sleeping for {CHECK_INTERVAL_SECONDS} seconds.")
        time.sleep(CHECK_INTERVAL_SECONDS)
