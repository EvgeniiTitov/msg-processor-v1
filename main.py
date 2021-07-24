import sys
import time
import threading

from helpers import LoggerMixin, SlackMixin
from runner import RunnerV1
from consumers import AzureConsumer
from publishers import AzurePublisher
from message_validator import validate_message
from message_processor import process_message


class App(LoggerMixin, SlackMixin):

    def __init__(
            self,
            sleep_time_between_health_reports: int,
            concur_messages: int
    ) -> None:
        LoggerMixin.__init__(self, "App")
        SlackMixin.__init__(self, webhook_url="hook")

        if not isinstance(concur_messages, int) or concur_messages <= 0:
            raise ValueError("Concurrent messages to be a positive integer")
        self._concurrent_messages = concur_messages
        if (
                not isinstance(sleep_time_between_health_reports, int) or
                sleep_time_between_health_reports <= 0
        ):
            raise ValueError("Sleeping time to be a positive integer")
        self._sleep = sleep_time_between_health_reports

        consumer = AzureConsumer()
        self.logger.info("Consumer initialized")

        publisher = AzurePublisher()
        self.logger.info("Publisher initialized")

        # Any essential part of the runner be it consumer/publisher or
        # validator/processor could be swapped out to modify the logic as long
        # as the objects conform to the interfaces
        self._processor = RunnerV1(
            concur_messages=concur_messages,
            consumer=consumer,
            publisher=publisher,
            message_validator=validate_message,
            message_processor=process_message
        )
        self.logger.info(f"Runner initialized")

        self._processor_thread = threading.Thread(
            target=self._processor.process_messages
        )
        self._processor_thread.start()
        self.logger.info("Runner thread started")

    def run(self) -> None:
        while True:
            time.sleep(self._sleep)
            self._report_health()

    def _report_health(self) -> None:
        processor_healthy = self._processor.is_healthy
        messages_processed = self._processor.messages_processed
        msg = (
            f"Feeling good. Processed {messages_processed} messages"
            if processor_healthy else
            "Something's wrong with the processor"
        )
        # self.slack_msg(msg)
        print(msg)

    def stop_processor(self) -> None:
        self._processor.stop()
        self._processor_thread.join()
        self.logger.info("Processor stopped")


if __name__ == '__main__':
    app = App(
        sleep_time_between_health_reports=10,
        concur_messages=1
    )
    try:
        app.run()
    except KeyboardInterrupt:
        app.stop_processor()
        sys.exit(0)
