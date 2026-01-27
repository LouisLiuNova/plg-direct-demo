import asyncio
import time
import httpx
from typing import Optional, Dict, Any
import os
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
        file_path = f"/cacheproxy/proxy/map/tz01/log/{generate_timestamp()}_tz01_{generate_random_file_id()}_.log"
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
            response = await self.client.post(self.target_url, json=payload)

            # 这里可以添加简单的日志，或者直接忽略
            # print(f"Status: {response.status_code}")

        except Exception as e:
            # 捕获网络异常，防止单个请求失败导致程序崩溃
            print(f"Request failed: {e}")
        finally:
            # 任务完成后，从集合中移除自身引用（可选，用于清理）

            pass

    async def start(self):
        print(f"Starting load test: Target={self.target_url}, TPS={self.tps}")
        start_time = time.monotonic()
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
