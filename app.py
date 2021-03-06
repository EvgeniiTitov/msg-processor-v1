import time
import threading

from helpers import LoggerMixin, SlackMixin
from runner import RunnerV1
from consumers import AzureConsumer
from publishers import AzurePublisher
from message_validator import validate_message
from message_processor import process_message_using_docker_image_sample_1


class App(LoggerMixin, SlackMixin):
    """
    Application class requires 4 keys elements:
    - Consumer,
    - Publisher,
    - Message Validator and
    - Message Processor.
    Different implementations of the same element could be used provided that
    they implement appropriate interfaces that the Runner relies on
    """

    def __init__(
        self,
        sleep_time_between_health_reports: int,
        concur_processing_jobs: int,
        acknowledgement_required: bool,
    ) -> None:
        LoggerMixin.__init__(self, "App")
        SlackMixin.__init__(self, project_name="PostConvValidation")

        if (
            not isinstance(concur_processing_jobs, int)
            or concur_processing_jobs <= 0
        ):
            raise ValueError("Concurrent messages to be a positive integer")
        self._concurrent_messages = concur_processing_jobs

        if (
            not isinstance(sleep_time_between_health_reports, int)
            or sleep_time_between_health_reports <= 0
        ):
            raise ValueError("Sleeping time to be a positive integer")
        self._sleep = sleep_time_between_health_reports

        self._consumer = AzureConsumer()
        self.logger.info("Consumer initialized")

        self._publisher = AzurePublisher()
        self.logger.info("Publisher initialized")

        self._runner = RunnerV1(
            concur_processing_jobs=concur_processing_jobs,
            acknowledgement_required=acknowledgement_required,
            consumer=self._consumer,
            publisher=self._publisher,
            message_validator=validate_message,
            message_processor=process_message_using_docker_image_sample_1,
        )
        self.logger.info(f"Runner initialized")

        self._processor_thread = threading.Thread(
            target=self._runner.process_messages_loop
        )
        self._processor_thread.start()
        self.logger.info("Runner thread started")

    def run(self) -> None:
        while True:
            time.sleep(self._sleep)
            self._report_health()
            if not self._runner._is_running:
                break
        self.logger.info("The app has completed")

    def _report_health(self) -> None:
        processor_healthy = self._runner.is_healthy
        messages_processed = self._runner.messages_processed
        latest_issues = self._runner.latest_issues
        msg = (
            f"\nFeeling good. Processed {messages_processed} messages"
            if processor_healthy
            else f"Feeling bad. The  following issues were encountered:\n"
            f"\n{' '.join(latest_issues)}"
        )
        self.slack_msg(msg)

    def stop_processor(self) -> None:
        self._runner.stop()
        self._processor_thread.join()
        del self._consumer  # Force resource release
        self.logger.info("Runner stopped")
