import logging
import sys


class Logger:
    def __init__(self, logger_name: str, verbose: bool = True) -> None:
        self._logger = logging.getLogger(logger_name)
        self._logger.propagate = False
        self._formatter = logging.Formatter(
            "%(asctime)s %(name)s %(levelname)s %(lineno)s: %(message)s"
        )
        if verbose:
            self._logger.setLevel(logging.DEBUG)
        else:
            self._logger.setLevel(logging.ERROR)
        self._logger.addHandler(self._get_console_handler())

    def _get_console_handler(self) -> logging.StreamHandler:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(self._formatter)
        return console_handler

    def debug(self, *args, **kwargs) -> None:
        self._logger.debug(*args, **kwargs)

    def info(self, *args, **kwargs) -> None:
        self._logger.info(*args, **kwargs)

    def warning(self, *args, **kwargs) -> None:
        self._logger.warning(*args, **kwargs)

    def error(self, *args, **kwargs) -> None:
        self._logger.error(*args, **kwargs)

    def exception(self, *args, **kwargs) -> None:
        self._logger.exception(*args, **kwargs)
