import logging
import sys


logging.basicConfig(
    format="'%(asctime)s %(levelname)s %(module)s "
    "- %(filename)s:%(lineno)d -- %(message)s'",
    stream=sys.stdout,
    level=logging.INFO,
    datefmt="%Y-%m-%dT%H:%M:%S%z",
)


class LoggerMixin:
    def __init__(self, name: str) -> None:
        self._name = name

    @property
    def logger(self) -> logging.Logger:
        name = ".".join([self._name, __name__, self.__class__.__name__])
        return logging.getLogger(name)
