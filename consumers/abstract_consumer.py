from abc import ABC, abstractmethod
import typing as t


class AbsConsumer(ABC):

    @abstractmethod
    def get_message(self) -> t.Optional[str]:
        ...

    @abstractmethod
    def acknowledge_message(self, message_id: str) -> None:
        ...
