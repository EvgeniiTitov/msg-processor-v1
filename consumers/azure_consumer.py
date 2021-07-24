import typing as t
import os

from azure.servicebus import ServiceBusClient, ServiceBusReceiver

from consumers.abstract_consumer import AbsConsumer
from helpers import LoggerMixin


# TODO: Reformat the consumer
'''
Why the fuck I cannot reuse the same receiver instance? 
'''

class AzureConsumer(LoggerMixin, AbsConsumer):

    def __init__(
            self,
            connection_str: t.Optional[str] = None,
            queue_name: t.Optional[str] = None,
            timeout: int = 10,
            logging_enable: bool = False
    ) -> None:
        LoggerMixin.__init__(self, "AzureConsumer")

        if connection_str:
            self._conn_str = connection_str
        else:
            conn_str = os.environ.get("CONNECTION_STR")
            if not conn_str:
                raise Exception(
                    "Provide CONNECTION_STR env variable or pass directly"
                )
            self._conn_str = conn_str

        if queue_name:
            self._queue_name = queue_name
        else:
            queue = os.environ.get("QUEUE_NAME")
            if not queue:
                raise Exception(
                    "Provide QUEUE_NAME env variable or pass directly"
                )
            self._queue_name = queue

        if not isinstance(timeout, int) or timeout < 0:
            raise ValueError("Timeout must be a positive integer")
        self._timeout = timeout

        try:
            self._bus_client = ServiceBusClient.from_connection_string(
                conn_str=self._conn_str, logging_enable=logging_enable
            )
        except Exception as e:
            self.logger.exception("Exception instantiating AzureConsumer")
            raise e
        self._being_processed_messages = {}
        self.logger.info("AzureConsumer initialized")

    def get_message(self) -> t.Tuple[t.Optional[str], t.Optional[str]]:
        """
        Attempts to get a message from the queue
        Returns message content and its ID for subsequent acknowledgement if
        it was successfully processed
        """
        # TODO: Creating an instance of receiver every time I need a message is
        #       super fucking annoying. Fix
        with self._bus_client:
            receiver: ServiceBusReceiver = self._bus_client.get_queue_receiver(
                self._queue_name, max_wait_time=self._timeout
            )
            # Attempt getting a message from the queue, if empty - return
            try:
                msg = next(iter(receiver))
            except StopIteration:
                return None, None
            message_id = str(msg.message_id)
            message_content = str(msg.message)
            self._being_processed_messages[message_id] = msg
            return message_content, message_id

    def acknowledge_message(self, message_id: str) -> None:
        """
        If the message was successfully processed, delete it from the queue
        """
        if message_id not in self._being_processed_messages:
            raise KeyError(
                f"The key {message_id} doesn't belong to any messages!"
            )
        # TODO: Creating an instance of receiver every time I need to use it is
        #       super fucking annoying. Fix
        with self._bus_client:
            receiver: ServiceBusReceiver = self._bus_client.get_queue_receiver(
                self._queue_name, max_wait_time=self._timeout
            )
            receiver.complete_message(
                self._being_processed_messages[message_id]
            )
            self._being_processed_messages.pop(message_id)
            self.logger.info(f"Acknowledged message {message_id}")
