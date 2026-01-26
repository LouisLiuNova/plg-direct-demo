import time
import random
from loguru import logger
import uuid
import os
import sys
from concurrent.futures import ThreadPoolExecutor
from .log import structured_format_forward


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
    file_name = f"/cacheproxy/proxy/map/tz01/log/20251028225234929405_tz01_0000_log.zip"
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

            # 模拟丢包：决定是否成功处理这个“虚拟”请求
            executor.submit(process_task, random.random() >= LOSS_RATE)


def process_task(status: bool = True):
    # 模拟生成一个 ID (注意：分布式下无法与 Forwarder 严格对应，仅做统计模拟)
    file_name = f"/data/upload/file_{uuid.uuid4().hex[:8]}.dat"

    # 模拟耗时
    duration_ms = random.randint(50, 500)
    time.sleep(duration_ms / 1000.0)

    logger.info(
        f"处理文件filePath={file_name}{"成功" if status else "失败"}，耗时{duration_ms}毫秒"
    )


if __name__ == "__main__":
    # --- 日志初始化 (根据角色只初始化一个 Logger) ---
    os.makedirs("/var/log/app", exist_ok=True)
    logger.remove()
    if ROLE == "forwarder":
        logger.add(
            "/var/log/app/forward.log",
            encoding="utf-8",
            enqueue=True,
            format=structured_format_forward,
        )
        run_forwarder()
    else:
        logger.add(
            "/var/log/app/process.log",
            encoding="utf-8",
            enqueue=True,
            format="{time:YYYY-MM-DD HH:mm:ss.SSS} [Log-Producer] {level: <5} com.bigdata.tz.monitor.LogMonitor - {message}",
        )
        run_processor()
