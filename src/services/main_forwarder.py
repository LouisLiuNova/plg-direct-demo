"""
main_forwarder.py

This module implements an asynchronous load testing tool using a token bucket algorithm
to control transactions per second (TPS) when sending HTTP requests to a specified target URL.

Classes:
    TokenBucket: Implements a token bucket algorithm for rate limiting.
    LoadTester: Conducts load testing by sending HTTP requests at a controlled rate.

Usage:
    To use this module, set the environment variables APP_PROCESSOR_URL and APP_TPS,
    then run the script. It will create a LoadTester instance and start sending requests
    to the specified target URL at the defined TPS.

Dependencies:
    - asyncio: For asynchronous programming.
    - httpx: For making HTTP requests.
    - os: For environment variable access and directory management.
    - time: For time-related functions.
    - typing: For type hinting.
    - forwarder.log: Custom logging utility.
    - forwarder.utils: Utility functions for generating timestamps and random file IDs.
    - public.models: Data model for the payload structure.

Note:
    This module is designed to be run as a standalone script. It will create a directory
    for logs if it does not exist and will handle exceptions during HTTP requests to ensure
    that the load testing process is robust.

"""

import asyncio
import os
import time
from typing import Dict, Any

import httpx
from forwarder.log import logger
from forwarder.utils import generate_timestamp, generate_random_file_id
from public.models import Payload


class TokenBucket:
    """
    异步令牌桶算法实现，用于精确控制TPS。
    """

    def __init__(self, rate: float, capacity: float):
        """
        :param rate: 令牌生成速率 (每秒生成的令牌数，即目标 TPS)
        :param capacity: 桶的容量 (允许的最大突发量)
        """
        self.rate = rate
        self.capacity = capacity
        self.tokens = capacity
        self.last_update = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self):
        """
        尝试获取令牌。如果桶空了，则异步等待直到有令牌可用。
        """
        while True:
            async with self._lock:
                now = time.monotonic()
                # 计算自上次更新以来经过的时间
                elapsed = now - self.last_update
                # 添加新生成的令牌，但不超过容量
                new_tokens = elapsed * self.rate
                self.tokens = min(self.capacity, self.tokens + new_tokens)
                self.last_update = now

                if self.tokens >= 1:
                    self.tokens -= 1
                    return  # 成功获取令牌，退出循环

                # 如果令牌不足，计算需要等待的时间
                wait_time = (1 - self.tokens) / self.rate

            # 释放锁并在循环外等待，避免阻塞其他协程
            await asyncio.sleep(wait_time)


class LoadTester:
    """
    Load testing utility for sending HTTP requests at a controlled rate.
    This class implements a load tester that sends HTTP POST requests to a target URL
    at a specified transactions per second (TPS) rate. It uses token bucket rate limiting
    to control request throughput and employs a fire-and-forget pattern with asyncio
    for non-blocking concurrent request handling.
    Attributes:
        target_url (str): The target URL endpoint to send requests to.
        tps (int): Target transactions per second rate for request throughput.
        limiter (TokenBucket): Rate limiter to control request frequency.
        client (httpx.AsyncClient): Async HTTP client for sending requests.
        running (bool): Flag indicating whether the load test is currently running.
        tasks (set): Set containing references to active async tasks to prevent
            garbage collection during high concurrency scenarios.
    Example:
        >>> tester = LoadTester(target_url="http://localhost:8000/api", tps=100)
        >>> await tester.start()
        This implementation uses a fire-and-forget pattern, meaning requests may still
        be in transit when the main loop exits. The start() method ensures all tasks
        are properly awaited before closing the client connection.
    """

    def __init__(self, target_url: str, tps: int):
        self.target_url = target_url
        self.tps = tps
        # 初始化令牌桶，容量设为TPS相同，允许1秒内的突发，但在持续压力下会平滑到TPS
        self.limiter = TokenBucket(rate=tps, capacity=tps)

        # 优化 httpx 连接池配置
        # max_connections: 允许的最大并发连接数 (应大于 TPS 以防止连接耗尽)
        # max_keepalive_connections: 保持活跃的连接数
        limits = httpx.Limits(max_keepalive_connections=tps, max_connections=tps * 2)
        self.client = httpx.AsyncClient(limits=limits, timeout=10.0)

        self.running = False
        self.tasks = (
            set()
        )  # 用于持有任务引用，防止被垃圾回收（虽然在fire-and-forget中不是必须等待，但保持引用是好习惯）

    def prepare_payload(self) -> Payload:
        """
        【Payload 准备阶段】
        构建请求数据
        """
        base_path = "/cacheproxy/proxy/map/tz01/log/"

        file_path = (
            f"{base_path}{generate_timestamp()}_tz01_{generate_random_file_id()}_.log"
        )
        logger.info(f"Read {file_path}")
        payload = Payload(ts=time.time(), file=file_path)
        return payload

    async def _send_request(self, payload: Dict[str, Any]):
        """
        实际发送 HTTP 请求的 Worker。
        """
        try:
            # 发起 POST 请求
            # 这里的 await 只是等待网络IO，不会阻塞主循环的发送频率
            await self.client.post(self.target_url, json=payload)

            # 这里可以添加简单的日志，或者直接忽略
            # print(f"Status: {response.status_code}")

        except (httpx.RequestError, httpx.HTTPError, asyncio.TimeoutError) as e:
            # 捕获网络异常，防止单个请求失败导致程序崩溃
            print(f"Request failed: {e}")
        finally:
            # 任务完成后，从集合中移除自身引用（可选，用于清理）

            pass

    async def start(self):
        """
        Start the load test and send requests at a controlled rate.
        This method initiates a continuous loop that sends HTTP requests to the target URL
        at a specified TPS (transactions per second) rate. It uses a rate limiter to control
        the request throughput and implements a fire-and-forget pattern with asyncio tasks.
        The method:
        - Acquires tokens from the rate limiter to control request rate
        - Prepares payload data for each request
        - Creates asynchronous tasks to send requests without blocking the main loop
        - Tracks all created tasks to prevent garbage collection during high concurrency
        - Handles keyboard interrupts gracefully
        - Waits for all pending requests to complete before closing the client connection
        Raises:
            KeyboardInterrupt: Caught internally to gracefully stop the load test when
                              the user presses Ctrl+C.
        Note:
            This is a fire-and-forget implementation, meaning requests may still be
            in transit when the main loop exits. The finally block ensures all tasks
            are awaited before closing the client connection.
        """

        print(f"Starting load test: Target={self.target_url}, TPS={self.tps}")
        request_count = 0

        try:
            while True:  # Run for a fixed duration if needed
                # 1. 获取令牌 (限流)
                await self.limiter.acquire()

                # 2. 准备数据
                payload = self.prepare_payload()

                # 3. Fire-and-Forget (并行发送)
                # create_task 会立即调度协程执行，不会阻塞当前循环
                logger.info(f"Rename trigger hard link {payload.file} to process")
                task = asyncio.create_task(self._send_request(payload.model_dump()))

                # 保存任务引用以防被 Python GC 意外回收（针对极高并发场景的防御性编程）
                self.tasks.add(task)
                task.add_done_callback(self.tasks.discard)

                request_count += 1

                # 可选：每秒打印一次进度
                # if request_count % self.tps == 0:
                #    print(f"Sent {request_count} requests...")

        except KeyboardInterrupt:
            print("\nStopping load test...")
        finally:
            print(f"Load test finished. Total requests sent: {request_count}")

            # 注意：因为是 Fire-and-Forget，主循环结束时，可能还有请求在网络上传输。
            # 如果希望脚本立即结束，可以直接退出。
            # 如果希望等待所有已发出的请求完成，取消下面这行的注释：
            await asyncio.gather(*self.tasks, return_exceptions=True)

            await self.client.aclose()


# 使用示例
if __name__ == "__main__":
    os.makedirs("/var/log/app", exist_ok=True)
    TARGET_URL = os.getenv("APP_PROCESSOR_URL", "http://app-processor:8000/receive")
    TARGET_TPS = int(os.getenv("APP_TPS", "10"))

    tester = LoadTester(TARGET_URL, TARGET_TPS)

    # 运行异步主程序
    asyncio.run(tester.start())
