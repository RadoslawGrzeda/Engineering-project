import logging
from pythonjsonlogger import json as jsonlogger
from contextvars import ContextVar

correlation_id:ContextVar[str] = ContextVar('corellation_id',default='-')

class CorrelationFilter(logging.Filter):
    def filter(self, record):
        record.correlation_id = correlation_id.get()
        return True

def get_logger(name : str, service: str = "unknown") -> logging.Logger:
    logger = logging.getLogger(name)

    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)

    handler = logging.StreamHandler()
    formatter = jsonlogger.JsonFormatter(
        fmt="%(asctime)s %(levelname)s %(name)s %(message)s %(correlation_id)s %(service)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
        rename_fields={"asctime": "timestamp", "levelname": "level", "name": "logger"},
    )
    handler.setFormatter(formatter)
    handler.addFilter(CorrelationFilter())
    handler.addFilter(_ServiceFilter(service))
    logger.addHandler(handler)
    logger.propagate = False

    return logger


class _ServiceFilter(logging.Filter):
    def __init__(self, service: str):
        super().__init__()
        self._service = service

    def filter(self, record):
        record.service = self._service
        return True