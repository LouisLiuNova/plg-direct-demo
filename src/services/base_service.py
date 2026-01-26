from abc import ABC, abstractmethod
from .config import Config
from loguru._logger import Logger


class ServiceBase(ABC):
    """
    A class to define the base service.
    """

    def __init__(self, config: Config):
        self.config = config
        # the logger used to write log to file. must configure before using
        self.filelogger = Logger()

    @abstractmethod
    def run(self):
        """
        Abstract method to run the service.
        This method should be implemented by all subclasses and used for starting the service.
        """
        pass

    @abstractmethod
    def run_task(self):
        """
        A placeholder method for running tasks.
        This method can be overridden by subclasses if needed.
        The task which is submitted to thread pool executor should be implemented here.
        """
        pass

    @abstractmethod
    def create_file(self):
        """
        A placeholder method for creating a file.
        This method can be overridden by subclasses if needed.
        To create a file by its rule, implement the logic here.
        """
        pass
