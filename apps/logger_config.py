import logging
from pythonjsonlogger import json as jsonlogger
from contextvars import ContextVar

correlation_id:ContextVar[str] = ContextVar('corellation_id',default='-')

class CorrelationFilter(logging.Filter):
    def filter(self, record):
        record.correlation_id = correlation_id.get()
        return True

def get_logger(name : str) -> logging.Logger:
    logger = logging.getLogger(name)

    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)

    handler = logging.StreamHandler()
    formatter = jsonlogger.JsonFormatter(
        fmt="%(asctime)s %(levelname)s %(name)s %(message)s %(correlation_id)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
        rename_fields={"asctime": "timestamp", "levelname": "level", "name": "logger"},
    )
    handler.setFormatter(formatter)
    handler.addFilter(CorrelationFilter())
    logger.addHandler(handler)
    logger.propagate = False

    return logger