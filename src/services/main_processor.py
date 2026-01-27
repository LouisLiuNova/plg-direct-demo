import asyncio
import uvicorn
from fastapi import FastAPI
from starlette.responses import Response
from pydantic import BaseModel
from contextlib import asynccontextmanager
from public.models import Payload
import time
from processor.log import logger
import os
import random


# 全局计数器，用于统计接收到的请求数量
class RequestStats:
    count = 0


stats = RequestStats()
LOSS_RATE = float(os.getenv("APP_LOSS_RATE", "0.2"))


async def monitor_tps():
    """
    后台监控任务：每10秒打印一次当前的 TPS
    """
    print("启动 TPS 监控...")
    while True:
        # 记录当前时间点的计数
        start_count = stats.count
        # 等待 10 秒
        await asyncio.sleep(10)
        # 计算增量
        tps = stats.count - start_count

        # 只有当有流量时才打印，避免刷屏
        if tps > 0:
            print(f"🔥 [Server] 实时接收 TPS: {tps}/10s | 总接收: {stats.count}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    FastAPI 生命周期管理：在应用启动时运行监控任务
    """
    # 启动监控协程
    task = asyncio.create_task(monitor_tps())
    yield
    # 应用关闭时取消任务
    task.cancel()


# 初始化 FastAPI 应用
app = FastAPI(lifespan=lifespan)


@app.post("/receive")
async def receive_data(payload: Payload) -> Response:
    """
    接收请求的接口
    """
    # 1. 简单的数据校验 (Pydantic 会自动处理)

    # 2. 计数器加一 (原子操作在 Python GIL 下对于简单 += 是安全的，但在极高并发下建议用 ContextVar 或其他方式，这里 300 TPS 足够安全)
    stats.count += 1

    # 3. 模拟业务处理
    file_name = payload.file
    duration_ms = random.randint(50, 500)
    await asyncio.sleep(duration_ms / 1000.0)
    logger.info(
        f"处理文件filePath={file_name}{"成功" if random.random() >= LOSS_RATE else "失败"}，耗时{duration_ms}毫秒"
    )
    # 4. 快速返回，不阻塞客户端
    return Response(status_code=200)


if __name__ == "__main__":
    # 使用 uvicorn 启动服务
    # log_level="warning" 可以减少控制台日志输出，提高性能测试时的观察体验
    uvicorn.run(
        "__main__:app", host="0.0.0.0", port=8000, log_level="warning", workers=1
    )
