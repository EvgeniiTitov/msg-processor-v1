from abc import ABC, abstractmethod


class AbsPublisher(ABC):
    @abstractmethod
    def send_message(self, message: str) -> None:
        ...
