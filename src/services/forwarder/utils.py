import random
from datetime import datetime


def generate_timestamp() -> str:
    # 获取当前时间
    current_time = datetime.now()

    # 格式化为目标字符串
    current_timestamp = current_time.strftime("%Y%m%d%H%M%S%f")

    return current_timestamp


def generate_random_file_id() -> str:
    """生成8位随机数字ID"""
    return str(random.randint(0, 99999999)).zfill(8)
