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
        record["extra"]["serialized"] = json.dumps(error_data, ensure_ascii=False)
        return "{extra[serialized]}\n"
