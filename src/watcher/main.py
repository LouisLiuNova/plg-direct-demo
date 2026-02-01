import os
import time
import json
import re
import logging
import requests
from datetime import datetime, timedelta
from prometheus_client import start_http_server, Gauge
from pathlib import Path
from sqlalchemy import create_engine, text, desc
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker
from dao import Base, WatcherDao

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


def run_audit(dao_handler: WatcherDao):
    """
    run_audit 的 Docstring

    :param dao_handler: 数据库操作对象
    :type dao_handler: WatcherDao
    """
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
    # --- 5. 生成审计报告 ---
    # 只有当有丢失文件时，或者强制生成报告时写入
    report_data = {
        "audit_window_start": window_start_dt.isoformat(),
        "audit_window_end": window_end_dt.isoformat(),
        "forward_count": len(forward_files),
        "process_count": len(process_files),
        "lost_count": lost_count
    }

    dao_handler.create_report_with_lost_files(
        report_data=report_data, lost_files_list=list(lost_files)
    )
    logger.info(
        f"Audit report: {len(forward_files)} forwarded, {len(process_files)} processed, {lost_count} lost. Report saved."
    )


if __name__ == "__main__":
    # 启动 Prometheus Metrics Server
    start_http_server(8000)
    logger.info("Metrics server started on port 8000")
    # Create the default db session
    db_root_password = os.getenv('DB_ROOT_PASSWORD', '')
    db_user = os.getenv('DB_USER', 'root')
    db_password = os.getenv('DB_PASSWORD', '')
    db_host = os.getenv('DB_HOST', '127.0.0.1')
    db_port = os.getenv('DB_PORT', '3306')
    db_name = os.getenv('DB_NAME', 'watcher_db')
    user_pass_part = f"{db_user}:{db_password}" if db_password else db_user

    # 首次连接：使用mysql系统数据库（用于创建目标数据库）
    DB_URL = f"mysql+pymysql://root:{db_root_password}@{db_host}:{db_port}/test_db?charset=utf8mb4"
    logger.info(f"Connecting to the base database by {DB_URL} for setup")

    base_engine = create_engine(DB_URL, echo=False)
    # 创建目标数据库（如果不存在）
    max_retries = 20  # 最多重试20次
    retry_interval = 3  # 每次重试间隔3秒
    retry_count = 0

    # 测试连接，若失败则重试
    while retry_count < max_retries:
        try:
            base_engine = create_engine(DB_URL, echo=False)
            with base_engine.connect():
                logger.info("[root] 成功连接到MySQL基础数据库！")
                break
        except SQLAlchemyError as e:
            retry_count += 1
            logger.warning(
                f"[root] root user连接MySQL失败（{retry_count}/{max_retries}）：{e}")
            if retry_count >= max_retries:
                logger.error("[root] 重试次数耗尽，无法连接MySQL，退出程序")
                raise
            time.sleep(retry_interval)
    # 连接成功，创建目标数据库
    try:
        with base_engine.connect() as conn:
            # 执行创建数据库的原生 SQL（IF NOT EXISTS 避免已存在时报错）
            # 指定字符集 utf8mb4，避免后续中文乱码
            create_db_sql = text(
                f"CREATE DATABASE IF NOT EXISTS {db_name} DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;")
            conn.execute(create_db_sql)
            # DDL 操作（创建数据库/表）自动提交，无需手动 commit
            grant_sql = text(
                f"GRANT ALL PRIVILEGES ON {db_name}.* TO 'user'@'%' WITH GRANT OPTION;"
            )
            conn.execute(grant_sql)
            conn.commit()  # 提交权限修改
            logger.info(
                f"[root] 目标数据库 {db_name} 检测/创建完成（存在则跳过，不存在则创建）")
    except SQLAlchemyError as e:
        logger.error(
            f"[root] 创建数据库失败：{str(e)}")
    finally:
        # 关闭基础引擎，释放连接
        base_engine.dispose()

    # Reconnect to the target database
    DB_URL = f"mysql+pymysql://{user_pass_part}@{db_host}:{db_port}/{db_name}?charset=utf8mb4"
    engine = create_engine(DB_URL, echo=True,
                           pool_size=5,
                           max_overflow=10)
    # 若表未创建，先创建表（仅首次执行）
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    db_session = Session()
    dao = WatcherDao(db_session)
    logger.info("[user] 数据库连接成功，WatcherDao 初始化完成")

    # 首次启动立即运行一次，或者先sleep
    logger.info(f"Service started. Interval: {CHECK_INTERVAL_SECONDS}s")
    logger.info(
        f"Sleep {CHECK_INTERVAL_SECONDS} secs first to wait for enough datas")
    time.sleep(CHECK_INTERVAL_SECONDS)
    logger.info(
        f"Sleep {WINDOW_EXTEND_SECONDS}*2 secs to wait for enough extend window"
    )
    time.sleep(WINDOW_EXTEND_SECONDS * 2)

    while True:
        logger.info(f"Start to run an audit")
        try:
            run_audit(dao)
        except Exception as e:
            logger.error(f"Audit loop failed: {e}")
        logger.info(
            f"Audit completed. Sleeping for {CHECK_INTERVAL_SECONDS} seconds.")
        time.sleep(CHECK_INTERVAL_SECONDS)
