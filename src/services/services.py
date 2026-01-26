from concurrent.futures import ThreadPoolExecutor
from base_service import ServiceBase
from loguru import logger
from .utils import structured_format_forward
import time


class ForwardService(ServiceBase):
    """
    A class to define the forward service.
    """

    def __init__(self, config: dict):
        super().__init__(config)
        self.filelogger.add(
            "/var/log/app/forward.log",
            encoding="utf-8",
            enqueue=True,
            format=structured_format_forward,
        )
        self.tps = self.config.tps

        # use the normal global logger for running logs
        logger.info(f"ForwardService initialized with TPS={self.tps}")

    def run(self):
        """
        Method to run the forward service.
        """
        interval = 1.0 / self.tps if self.tps > 0 else 0.1

        # 使用线程池处理转发任务
        with ThreadPoolExecutor(max_workers=20) as executor:
            while True:
                # 控制发送频率
                time.sleep(interval)
                # 提交转发任务到线程池
                executor.submit(self.run_task)

    def run_task(self):
        # TODO: Implement the forward task logic here
        return super().run_task()

    def create_file(self):
        pass
