import time
import random
from loguru import logger
import uuid
import os
import sys
from concurrent.futures import ThreadPoolExecutor
import json


try:
    # --- 配置读取 ---
    ROLE = os.getenv("APP_ROLE", "forwarder")  # forwarder 或 processor
    TPS = float(os.getenv("APP_TPS", "10.0"))
    LOSS_RATE = float(os.getenv("APP_LOSS_RATE", "0.1"))
    TPS = float(os.getenv("APP_TPS", "10.0"))
    LOSS_RATE = float(os.getenv("APP_LOSS_RATE", "0.1"))
    # INTERFERENCE_DELAY = float(os.getenv("APP_INTERFERENCE_DELAY", "0.01"))
    MIN_LATENCY = int(os.getenv("APP_MIN_LATENCY", "50"))
    MAX_LATENCY = int(os.getenv("APP_MAX_LATENCY", "500"))
except ValueError as e:
    print(f"配置错误: {e}")
    sys.exit(1)
# --- 日志初始化 (根据角色只初始化一个 Logger) ---
os.makedirs("/var/log/app", exist_ok=True)


# --- 日志模型 ---
def structured_format_forward(record):
    # 构造自定义的日志字典（只保留你需要的字段）
    time_str = record["time"].strftime("%Y-%m-%dT%H:%M:%S%z")
    time_str = f"{time_str[:-2]}:{time_str[-2:]}"
    log_data = {
        "timestamp": time_str,  # 时间戳
        "level": record["level"].name,  # 日志级别
        "msg": record["message"],  # 日志消息
        "caller": "ph",  # 占位符
        "version": "v1.0.9",  # 版本号
    }
    return json.dumps(log_data, ensure_ascii=False) + "\n"


logger.remove()
if ROLE == "forwarder":
    logger.add(
        "/var/log/app/forward.log",
        serialize=True,
        encoding="utf-8",
        format=structured_format_forward,
    )  # 启用JSON序列化
else:
    logger.add(
        "/var/log/app/process.log",
        serialize=True,
        encoding="utf-8",
        format="{time:YYYY-MM-DD HH:mm:ss.SSS} [Log-Producer] {level: <5} com.bigdata.tz.monitor.LogMonitor - {message}",
    )  # 启用JSON序列化


# --- 业务逻辑 ---
def run_forwarder():
    """转发服务逻辑 - 多线程版本"""
    print(f"启动转发服务: TPS={TPS}")
    interval = 1.0 / TPS if TPS > 0 else 0.1

    # 使用线程池处理转发任务
    with ThreadPoolExecutor(max_workers=20) as executor:
        while True:
            # 控制发送频率
            time.sleep(interval)
            # 提交转发任务到线程池
            executor.submit(forward_task)


def forward_task():
    """转发任务执行函数"""
    file_name = f"/data/upload/file_{uuid.uuid4().hex[:8]}.dat"
    # 模拟日志
    logger.info(f"Read {file_name}")
    time.sleep(0.01)
    logger.info(f"Rename trigger hard link {file_name}")
    # 模拟转发处理时间
    time.sleep(0.05)  # 可以根据需要调整


def run_processor():
    """处理服务逻辑"""
    print(f"启动处理服务: TPS={TPS}, LossRate={LOSS_RATE}")
    # 模拟并发处理
    with ThreadPoolExecutor(max_workers=20) as executor:
        while True:
            # 模拟上游流量到达的节奏
            time.sleep(1.0 / TPS)

            # 模拟丢包：决定是否处理这个“虚拟”请求
            if random.random() >= LOSS_RATE:
                executor.submit(process_task)


def process_task():
    # 模拟生成一个 ID (注意：分布式下无法与 Forwarder 严格对应，仅做统计模拟)
    file_name = f"/data/upload/file_{uuid.uuid4().hex[:8]}.dat"

    # 模拟耗时
    duration_ms = random.randint(50, 500)
    time.sleep(duration_ms / 1000.0)

    logger.info(f"处理文件filePath={file_name}成功，耗时{duration_ms}毫秒")


if __name__ == "__main__":
    if ROLE == "forwarder":
        run_forwarder()
    else:
        run_processor()
