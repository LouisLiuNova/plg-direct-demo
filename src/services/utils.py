import random
from starlette.responses import JSONResponse
from starlette.requests import Request
from threading import Lock
import json
from datetime import datetime, timezone, timedelta


def structured_format_forward(record):
    try:
        # 安全构造带时区的时间戳
        time_obj: datetime = record["time"]
        # 统一格式为 "YYYY-MM-DDTHH:MM:SS+HH:MM"
        time_str = time_obj.strftime("%Y-%m-%dT%H:%M:%S%z")
        if len(time_str) >= 2:
            time_str = f"{time_str[:-2]}:{time_str[-2:]}"
        else:
            time_str = time_obj.strftime("%Y-%m-%dT%H:%M:%S+08:00")

        # 构造日志字典
        log_data = {
            "ts": time_str,
            "level": record["level"].name,
            "msg": record["message"],
            "caller": "ph",
            "version": "v1.0.9",
        }

        # 1. 序列化 JSON
        serialized = json.dumps(log_data, ensure_ascii=False)

        # 2. 将序列化后的字符串存入 record["extra"]
        # 注意：record 是可变的，Loguru 允许我们在 formatter 中修改它
        record["extra"]["serialized"] = serialized

        # 3. 返回一个安全的格式化模板
        # Loguru 会将 {extra[serialized]} 替换为我们上面生成的 JSON 字符串
        # 这样避免了 Loguru 尝试解析 JSON 内部的大括号
        return "{extra[serialized]}\n"

    except Exception as e:
        # 兜底逻辑
        error_data = {
            "ts": datetime.now(tz=timezone(timedelta(hours=8))).strftime(
                "%Y-%m-%dT%H:%M:%S+08:00"
            ),
            "level": "ERROR",
            "msg": f"日志格式化异常: {str(e)} | 原消息: {record['message']}",
            "caller": "ph",
            "version": "v1.0.9",
        }
        record["extra"]["serialized"] = json.dumps(
            error_data, ensure_ascii=False)
        return "{extra[serialized]}\n"


def generate_random_file_id() -> str:
    """生成8位随机数字ID"""
    return str(random.randint(0, 99999999)).zfill(8)


# 全局存储 + 并发锁（避免多请求同时操作同一ID）
file_id_store = {}
store_lock = Lock()

# TODO: Move this to `run` method


async def create_file(request: Request, logger):
    """生成ID + 模拟创建文件（逻辑不变）"""
    file_id = generate_random_file_id()
    filename = f'data_{file_id}.txt'

    # 模拟文件创建
    with open(filename, 'w', encoding='utf-8') as f:
        f.write(f'流水线初始数据 - 文件ID: {file_id}')

    # 加锁存储，避免并发写入冲突
    with store_lock:
        file_id_store[file_id] = filename

    logger.info(f'生成文件ID: {file_id}，文件名: {filename}，已存入内存')
    return JSONResponse({
        'code': 200,
        'file_id': file_id,
        'filename': filename,
        'msg': '文件创建成功'
    })
