import os


class Config(object):
    """
    The configuration to be used in the services.
    """
    role: str = os.getenv("APP_ROLE", None)  # forwarder or processor
    tps: float = float(os.getenv("APP_TPS", "10.0"))
    loss_rate: float = float(os.getenv("APP_LOSS_RATE", "0.2"))
    min_latency: int = int(
        os.getenv("APP_MIN_LATENCY", "50"))  # in milliseconds
    max_latency: int = int(
        os.getenv("APP_MAX_LATENCY", "500"))  # in milliseconds
    rand_seed: int = int(os.getenv("APP_RAND_SEED", "42"))
    log_path: str = os.getenv("APP_LOG_PATH", "/var/log/app/")
