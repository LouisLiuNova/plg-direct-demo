import time
import random
import logging
import uuid
import threading
import queue
import os
import sys
from concurrent.futures import ThreadPoolExecutor  # 引入线程池

# --- 读取环境变量 ---
try:
    TPS = float(os.getenv("APP_TPS", "10.0"))
    LOSS_RATE = float(os.getenv("APP_LOSS_RATE", "0.1"))
    INTERFERENCE_DELAY = float(os.getenv("APP_INTERFERENCE_DELAY", "0.01"))
    MIN_LATENCY = int(os.getenv("APP_MIN_LATENCY", "50"))
    MAX_LATENCY = int(os.getenv("APP_MAX_LATENCY", "500"))
except ValueError as e:
    print(f"配置错误: {e}")
    sys.exit(1)

print(f"启动模拟 (并发版): TPS={TPS}, LossRate={LOSS_RATE}")

# --- 日志初始化 ---
os.makedirs("/var/log/app/service-forward", exist_ok=True)
os.makedirs("/var/log/app/service-process", exist_ok=True)


def setup_logger(name, log_file):
    formatter = logging.Formatter(
        "%(asctime)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
    )
    handler = logging.FileHandler(log_file)
    handler.setFormatter(formatter)
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)
    return logger


logger_f = setup_logger("forward", "/var/log/app/service-forward/app.log")
logger_p = setup_logger("process", "/var/log/app/service-process/app.log")

msg_queue = queue.Queue()

# --- 业务逻辑 ---


def service_forward():
    """模拟转发服务 (生产者)"""
    interval = 1.0 / TPS if TPS > 0 else 0.1
    while True:
        file_name = f"/data/upload/file_{uuid.uuid4().hex[:8]}.dat"

        # 干扰日志
        logger_f.info(f"Read {file_name}")
        time.sleep(INTERFERENCE_DELAY)
        logger_f.info(f"Write /tmp/{file_name.split('/')[-1]}")
        time.sleep(INTERFERENCE_DELAY)

        # 关键成功日志
        logger_f.info(f"Rename trigger hard link {file_name}")

        msg_queue.put({"file": file_name})

        # 控制生产速度
        time.sleep(max(interval - INTERFERENCE_DELAY * 2, 0))


def process_task(item):
    """单个文件的处理任务 (将在线程池中运行)"""
    file_name = item["file"]

    # 模拟处理耗时 (这只会阻塞当前线程，不会阻塞主循环)
    duration_ms = random.randint(MIN_LATENCY, MAX_LATENCY)
    time.sleep(duration_ms / 1000.0)

    # 模拟丢包逻辑
    if random.random() >= LOSS_RATE:
        logger_p.info(f"处理文件filePath={file_name}成功，耗时{duration_ms}毫秒")


def service_process():
    """模拟处理服务 (消费者 - 主分发线程)"""
    # 创建一个包含 50 个线程的线程池，模拟高并发处理能力
    # 这样即使每个任务耗时 500ms，吞吐量也能达到 100 TPS (50 / 0.5)
    with ThreadPoolExecutor(max_workers=50) as executor:
        while True:
            try:
                item = msg_queue.get(timeout=5)
                # 将任务提交给线程池，立即返回，不阻塞
                executor.submit(process_task, item)
            except queue.Empty:
                continue


if __name__ == "__main__":
    t1 = threading.Thread(target=service_forward)
    t2 = threading.Thread(target=service_process)
    t1.start()
    t2.start()
    t1.join()
    t2.join()
