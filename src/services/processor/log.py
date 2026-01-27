from loguru import logger
import json
from datetime import datetime, timezone, timedelta


logger.remove()

logger.add(
    "/var/log/app/process.log",
    encoding="utf-8",
    enqueue=True,
    format="{time:YYYY-MM-DD HH:mm:ss.SSS} [Log-Producer] {level: <5} com.bigdata.tz.monitor.LogMonitor - {message}",
)
