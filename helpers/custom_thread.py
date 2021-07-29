import threading
from typing import Callable, Optional


class CustomThread(threading.Thread):
    def __init__(
        self,
        function: Callable,
        message: str,
        message_id: str,
        *args,
        **kwargs
    ) -> None:
        super().__init__(*args, **kwargs)
        self._function = function
        self._message = message
        self._message_id = message_id
        self._exc = None

    @property
    def message(self):
        return self._message

    @property
    def message_id(self):
        return self._message_id

    def run(self) -> None:
        try:
            _ = self._function()
        except BaseException as e:
            self._exc = e  # type: ignore

    def join(self, timeout: Optional[float] = 0.0) -> None:
        threading.Thread.join(self, timeout=timeout)
        if self._exc:
            raise self._exc
