import typing as t
import os

from azure.servicebus import (
    ServiceBusClient,
    ServiceBusReceiver,
    AutoLockRenewer,
    ServiceBusMessage,
)

from consumers.abstract_consumer import AbsConsumer
from helpers import LoggerMixin


class CustomServiceBusClient(ServiceBusClient):
    """
    To avoid using context managers (bloody azure python sdk) and keep the
    bus client and the queue receiver in the constructor, so that they
    can be reused in both get_message() and acknowledge_message() methods
    """

    def enter(self):
        if self._connection_sharing:
            self._create_uamqp_connection()

    def exit(self):
        self.close()


class AzureConsumer(LoggerMixin, AbsConsumer):
    def __init__(
        self,
        connection_str: t.Optional[str] = None,
        queue_name: t.Optional[str] = None,
        timeout: int = 10,
        logging_enable: bool = False,
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

        self._logging_enable = logging_enable
        self._being_processed_messages: t.Dict[str, ServiceBusMessage] = {}

        try:
            self._client = CustomServiceBusClient.from_connection_string(
                conn_str=self._conn_str, logging_enable=self._logging_enable
            )
        except Exception as e:
            self.logger.exception("Failed to instantiate ServiceBusClient")
            raise e
        else:
            self._client.enter()

        # When a message is taken from the queue, there is a lock reflecting
        # how long this message wouldn't be able to get consumed by another
        # receiver connected to the queue. AutoLockRenewer ensures the lock
        # is not expired while a message is being processed
        self._lock_renewer = AutoLockRenewer(max_workers=2)

        self._receiver: ServiceBusReceiver = self._client.get_queue_receiver(
            self._queue_name, max_wait_time=self._timeout
        )
        self.logger.info("AzureConsumer initialized")

    def get_message(self) -> t.Tuple[t.Optional[str], t.Optional[str]]:
        """
        Attempts to get a message from the queue
        Returns message content and its ID for subsequent acknowledgement if
        it was successfully processed
        """
        # Attempt getting a message from the queue, if empty - return
        try:
            msg = next(iter(self._receiver))
        except StopIteration:
            return None, None
        # Register the message with the lock renewer
        self._lock_renewer.register(
            self._receiver, msg, max_lock_renewal_duration=6000.0
        )
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
        self._receiver.complete_message(
            self._being_processed_messages.pop(message_id)
        )
        self.logger.info(f"Acknowledged message {message_id}")

    def __del__(self):
        if self._client:
            self._client.exit()
        if self._lock_renewer:
            self._lock_renewer.close()
