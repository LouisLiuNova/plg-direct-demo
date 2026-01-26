from abc import ABC, abstractmethod


class ServiceBase(ABC):
    """
    A class to define the base service.
    """

    def __init__(self, config: dict):
        self.config = config

    @abstractmethod
    def run(self):
        """
        Abstract method to run the service.
        """
        pass

    def generate_random_flow(self):
        """
        Generate random flow based on configuration.
        """
        import random

        tps = self.config.get("tps", 10)
        loss_rate = self.config.get("loss_rate", 0.1)
        return random.uniform(0, tps), random.uniform(0, loss_rate)
