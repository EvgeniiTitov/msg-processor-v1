import time
import threading
import typing as t

from helpers import LoggerMixin
from consumers import AbsConsumer
from publishers import AbsPublisher


# TODO: What is a provided validator or processing function throws an exception
# TODO: Use publisher to signal message processing completion
# TODO: Use publisher to direct logs from the processing container?


class RunnerV1(LoggerMixin):
    """
    The class that processes messages.

    Both consumer and producer can be swapped out provided that they implement
    the appropriate interfaces.

    Message validator - validates the received image is correct and what we
    expect to get

    Message processor - processed the received and validated message

    N messages could be processed at the same time. For each image a new thread
    gets created as the processing is heavily IO bound and comes down to
    pulling and running appropriate docker containers with parameters received
    in the message.

    Message processing logic:
        - Using the consumer provided, get a message by calling get_message()
        - Validate the image
        - Process the image
        - Report completion of message processing using the publisher
    """

    def __init__(
            self,
            concur_messages: int,
            consumer: AbsConsumer,
            publisher: AbsPublisher,
            message_validator: t.Callable[[str], bool],
            message_processor: t.Callable[[str], None]
    ) -> None:
        LoggerMixin.__init__(self, "RunnerV1")

        self._concur_messages = concur_messages
        if not isinstance(consumer, AbsConsumer):
            raise TypeError(
                "Provide a consumer implementing the AbsConsumer interface"
            )
        self._consumer = consumer

        if not isinstance(publisher, AbsPublisher):
            raise TypeError(
                "Provide a publisher implementing the AbsPublisher interface"
            )
        self._publisher = publisher

        self._message_validator = message_validator
        self._message_processor = message_processor

        self._healthy = True
        self._to_stop = False
        self._total_messages_processed = 0
        self._currently_being_processed = 0
        self._running_threads: t.List[threading.Thread] = []
        self._attempted_starts = 0
        self._max_attempts = 1
        self.logger.info("RunnerV1 initialized")

    @property
    def is_healthy(self) -> bool:
        return self._healthy

    @property
    def messages_processed(self) -> int:
        return self._total_messages_processed

    def process_messages(self) -> None:
        time.sleep(1)
        while True:
            # Check if there are available slots to start a new processing job
            if (
                    self._currently_being_processed < self._concur_messages
                    and not self._to_stop
            ):
                self.logger.info("Attempting to start a new processing job")
                slots = self._concur_messages - self._currently_being_processed
                while slots and (self._attempted_starts < self._max_attempts):
                    started = self._start_processing_job()
                    if started:
                        slots -= 1
                        self._currently_being_processed += 1
                    else:
                        self.logger.info("Failed start attempt")
                        self._attempted_starts += 1
                self._attempted_starts = 0

            # Check if any running jobs have completed
            if len(self._running_threads):
                self.logger.info("Attempting to complete any running jobs")
                for thread in self._running_threads:
                    completed = self._complete_processing_job(thread)
                    if completed:
                        self.logger.info(
                            f"Processing job {thread.name} has completed"
                        )
                        self._running_threads.remove(thread)
                        self._currently_being_processed -= 1
                    else:
                        self.logger.info(
                            f"Job for {thread.name} is still running"
                        )
            # Run a quick check to make sure everything works as intended
            # Remove after debugging?
            self._check_errors()

            if self._to_stop:
                if self._currently_being_processed:
                    self.logger.info(
                        "Waiting for running jobs to complete before quitting"
                    )
                    continue
                else:
                    break
            else:
                time.sleep(0.5)

    def _start_processing_job(self) -> bool:
        self.logger.info("Attempting to receive a message")
        # Attempt to get a message from the consumer
        message = self._consumer.get_message()
        if not message:
            return False
        self.logger.info(f"Got message: {message}")
        # Validate the message to ensure it is what we expect
        validated = self._message_validator(message)
        if not validated:
            self.logger.warning(
                f"Failed to validate the message: {message}"
            )
            return False
        self.logger.info("Message validated")
        # Start processing the message in a separate thread
        processing_thread = threading.Thread(
            target=lambda: self._message_processor(message), name=message
        )
        processing_thread.start()
        self._running_threads.append(processing_thread)
        self.logger.info(f"Started processing the message: {message}")
        return True

    def _complete_processing_job(self, thread: threading.Thread) -> bool:
        thread.join(timeout=0.5)
        if thread.is_alive():  # timed out
            return False
        else:
            return True

    def stop(self) -> None:
        self._to_stop = True

    def _check_errors(self) -> None:
        if self._currently_being_processed < 0:
            self.logger.error("Processing negative number of jobs!")
            self._healthy = False

        running_threads = len(self._running_threads)
        if running_threads != self._currently_being_processed:
            self.logger.error(
                f"Number of running threads {running_threads} do not match the"
                f" number of running jobs {self._currently_being_processed}"
            )
            self._healthy = False
