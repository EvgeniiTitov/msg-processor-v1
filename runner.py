import time
import typing as t

from helpers import LoggerMixin, CustomThread, SlackMixin
from consumers import AbsConsumer
from publishers import AbsPublisher


# TODO: Direct logs coming from containers to Comet
# TODO: If state not healthy, stop processing messages and ask for fixing on slack

ProcessingRes = t.Tuple[bool, t.Optional[bool], t.Optional[str]]


class RunnerV1(LoggerMixin, SlackMixin):
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
            concur_processing_jobs: int,
            consumer: AbsConsumer,
            publisher: AbsPublisher,
            message_validator: t.Callable[[str], bool],
            message_processor: t.Callable[[str], None]
    ) -> None:
        LoggerMixin.__init__(self, "RunnerV1")

        self._concur_messages = concur_processing_jobs
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
        # Custom validation and processing functions
        self._message_validator = message_validator
        self._message_processor = message_processor

        self._healthy = True
        self._to_stop = False
        self._total_messages_processed = 0
        self._currently_being_processed = 0
        self._messages_being_processed = {}
        self._running_threads: t.List[CustomThread] = []
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
            # and then start one
            if (
                    self._currently_being_processed < self._concur_messages
                    and not self._to_stop
            ):
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

            # Try finalizing any completed processing jobs
            if len(self._running_threads):
                self.logger.info("Attempting to complete any running jobs")
                for thread in self._running_threads:
                    message = thread.message
                    # Attempt joining the processing thread and checking if
                    # it ran successfully
                    (
                        completed,
                        success,
                        err
                    ) = self._complete_processing_job(thread)
                    # If the thread was joined, reflect the changes
                    if completed:
                        self.logger.info(
                            f"Processing of the msg {message} has completed"
                        )
                        self._running_threads.remove(thread)
                        self._currently_being_processed -= 1
                        # Reflect on the processing status
                        if success:
                            self._total_messages_processed += 1
                            self._consumer.acknowledge_message(
                                self._messages_being_processed[message]
                            )
                            self._messages_being_processed.pop(message)
                            self._publisher.send_message(message)
                        else:
                            self.slack_msg(
                                f"Failed to process message {message}. "
                                f"Error: {err}"
                            )
                            self._messages_being_processed.pop(message)
                    else:
                        self.logger.info(
                            f"Job for {message} is still running"
                        )

            # Run a quick check to make sure everything works as intended
            # Remove after debugging?
            self._check_runtime_errors()

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
        message, message_id = self._consumer.get_message()
        if not message:
            return False

        # Validate the message to ensure it is what we expect
        try:
            validated = self._message_validator(message)
        except Exception as e:
            msg = f"Provided custom message validator throws an error: {e}"
            self.logger.exception(msg)
            self.slack_msg(msg)
            self._healthy = False
            return False

        if not validated:
            self.logger.warning(
                f"Failed to validate the message: {message}"
            )
            return False

        # Start processing the message
        self._messages_being_processed[message] = message_id
        processing_thread = CustomThread(
            function=lambda: self._message_processor(message),
            message=message
        )
        processing_thread.start()
        self._running_threads.append(processing_thread)
        self.logger.info(f"Started processing the message: {message}")
        return True

    def _complete_processing_job(self, thread: CustomThread) -> ProcessingRes:
        """
        Returns: has the job completed, did the code run successfully
        aka didn't throw any exceptions, error message if any occurred
        """
        # A thread running custom message processor could potentially throw
        # an error, handle it
        try:
            thread.join(timeout=0.5)
        except Exception as e:
            msg = f"For message {thread.message} provided message " \
                  f"processor function has thrown an error: {e}"
            self.logger.exception(msg)
            if thread.is_alive():
                return False, False, None
            else:
                return True, False, msg

        if thread.is_alive():  # timed out
            return False, None, None  # Result is not available yet
        else:
            return True, True, None

    def stop(self) -> None:
        self._to_stop = True

    def _check_runtime_errors(self) -> None:
        # TODO: Remove me after testing
        running_threads = len(self._running_threads)
        if running_threads != self._currently_being_processed:
            self.logger.error(
                f"Number of running threads {running_threads} do not match the"
                f" number of running jobs {self._currently_being_processed}"
            )
            self._healthy = False

        if self._currently_being_processed < 0:
            self.logger.error("Processing negative number of jobs!")
            self._healthy = False

        if self._currently_being_processed != len(self._messages_being_processed):
            self.logger.error(
                "Number of message keys != counter "
                "of messages being processed"
            )
