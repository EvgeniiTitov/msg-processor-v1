from abc import ABC, abstractmethod
import typing as t


class AbsConsumer(ABC):

    @abstractmethod
    def get_message(self) -> t.Optional[str]:
        ...
