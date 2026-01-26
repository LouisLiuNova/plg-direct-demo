import random


def deterministic_random_stream(seed: int):
    """
    生成确定性的可迭代随机流，支持随时取用
    :param seed: 统一种子（两个节点必须相同）
    :param random_type: 随机数类型，支持 float/int/choice
    :param args: 对应类型的参数：
                 - float: 无需额外参数（生成0-1浮点数）
                 - int: 需要2个参数（min, max），生成[min, max]整数
                 - choice: 需要1个参数（列表），从列表随机选元素
    :return: 生成器对象，可通过next()随时获取下一个随机数
    """
    # 初始化随机数生成器（关键：固定种子）
    random.seed(seed)

    # 无限生成随机数（按需取用，直到主动停止）
    while True:
        if random_type == "float":
            yield random.random()  # 0-1浮点数
        elif random_type == "int":
            if len(args) != 2:
                raise ValueError("int类型需要传入min和max两个参数")
            min_val, max_val = args
            yield random.randint(min_val, max_val)
        elif random_type == "choice":
            if len(args) != 1 or not isinstance(args[0], list):
                raise ValueError("choice类型需要传入1个列表参数")
            choice_list = args[0]
            yield random.choice(choice_list)
        else:
            raise ValueError(f"不支持的类型：{random_type}")
