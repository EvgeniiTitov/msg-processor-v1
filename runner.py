import time
import typing as t

from helpers import LoggerMixin, CustomThread
from consumers import AbsConsumer
from publishers import AbsPublisher


ProcessingRes = t.Tuple[bool, t.Optional[bool], t.Optional[str]]


class RunnerV1(LoggerMixin):
    """
    The runner class that uses Consumer, Publisher and message processor
    and validator provided to process messages.

    Both consumer and producer can be swapped out by a different type (say
    RabbitMQ or whatever) provided that they implement the appropriate
    interfaces that the RunnerV1 relies on.

    Message validator - validates the received message format is correct
    Message processor - processes a received and validated message

    N messages could be processed at the same time. For each message a new
    thread gets created as the processing is heavily IO bound and comes down to
    pulling and running appropriate docker containers with parameters received
    in the message.

    A message does not get acknowledged (deleted from the queue) unless it
    was successfully processed
    """
    def __init__(
            self,
            concur_processing_jobs: int,
            acknowledgement_required: bool,
            *,
            consumer: AbsConsumer,
            publisher: AbsPublisher,
            message_validator: t.Callable[[str], bool],
            message_processor: t.Callable[[str], None]
    ) -> None:
        LoggerMixin.__init__(self, "RunnerV1")

        self._concur_msg_limit = concur_processing_jobs
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

        self._acknowledge_required = acknowledgement_required
        self._healthy = True
        self._to_stop = False
        self._total_messages_processed = 0
        self._currently_being_processed = 0
        self._messages_being_processed = {}
        self._running_threads: t.List[CustomThread] = []
        self._attempted_starts = 0
        self._max_attempts = 1
        self._latest_issues = []
        self.logger.info("RunnerV1 initialized")

    @property
    def is_healthy(self) -> bool:
        return self._healthy

    @property
    def messages_processed(self) -> int:
        return self._total_messages_processed

    @property
    def latest_issues(self) -> t.Optional[t.List[str]]:
        return self._latest_issues if len(self._latest_issues) else None

    def process_messages(self) -> None:
        time.sleep(1)
        while True:
            # Check if there is capacity to start processing a new message if
            # any. Attempt receiving a message without blocking using timeout
            if (
                    self._currently_being_processed < self._concur_msg_limit
                    and not self._to_stop
                    and self._healthy
            ):
                self._attempt_processing_new_messages()

            # Check if any processing jobs have completed
            if len(self._running_threads):
                self._finalize_processing_jobs()

            # Run a quick check to make sure everything works as intended
            # Remove after debugging?
            self._check_runtime_errors()

            # If a runtime issue happened, attempt stopping the runner
            if not self._healthy:
                self.stop()
            if self._to_stop:
                if self._currently_being_processed:
                    self.logger.info(
                        "Waiting for running jobs to complete before quitting"
                    )
                    continue
                else:
                    break
            else:
                time.sleep(3)

    def _attempt_processing_new_messages(self) -> None:
        slots = self._concur_msg_limit - self._currently_being_processed
        while slots and (self._attempted_starts < self._max_attempts):
            # If the queue is empty or received message was not validated
            # the processing won't start
            ok = self._receive_and_launch_processing()
            if ok:
                slots -= 1
                self._currently_being_processed += 1
            else:
                self.logger.info("Failed to start processing job")
                self._attempted_starts += 1
        self._attempted_starts = 0

    def _receive_and_launch_processing(self) -> bool:
        self.logger.info("Checking for messages in the queue")
        # Attempt to get a message and its ID from the consumer within timeout
        # ID is required to acknowledge the message if processed successfully
        message, message_id = self._consumer.get_message()
        if not message:
            return False

        # Validate the message using the provided validator callback function
        # to ensure it is what we expect
        try:
            validated = self._message_validator(message)
        except Exception as e:
            msg = f"Provided custom message validator throws the error: {e}"
            self.logger.exception(msg)
            self._add_new_issue(msg)
            return False

        if not validated:
            self.logger.info(
                f"Failed to validate the message: {message}"
            )
            return False

        # Launch message processing using the provided message processor func
        self._messages_being_processed[message] = message_id
        processing_thread = CustomThread(
            function=lambda: self._message_processor(message),
            message=message
        )
        processing_thread.start()
        self._running_threads.append(processing_thread)
        self.logger.info(f"Started processing the message: {message}")
        return True

    def _finalize_processing_jobs(self) -> None:
        self.logger.info("Checking if any jobs have completed")
        for thread in self._running_threads:
            message = thread.message

            # Attempt joining the processing thread and checking if
            # it ran successfully
            (
                completed,
                success,
                err
            ) = self._join_thread(thread)

            # If the thread has finished, process the results
            if completed:
                self.logger.info(f"Processing of msg {message} completed")
                self._running_threads.remove(thread)
                self._currently_being_processed -= 1

                # Reflect on the processing status
                if success:
                    self._total_messages_processed += 1
                    message_id = self._messages_being_processed.pop(message)

                    # Give consumer message ID to acknowledge its completion
                    if self._acknowledge_required:
                        self._consumer.acknowledge_message(message_id)

                    self._publisher.send_message(message)
                else:
                    self._add_new_issue(
                        f"Failed to process message {message}. Error: {err}"
                    )
                    self._messages_being_processed.pop(message)
            else:
                self.logger.info(
                    f"Job for {message} is still running"
                )

    def _join_thread(self, thread: CustomThread) -> ProcessingRes:
        """
        Returns: has the thread finished, did the processing run successfully,
        error message if any occurred
        """
        # TODO: This is ugly AF, I don't like it
        # A thread running custom message processor could potentially throw
        # an error, handle it
        try:
            thread.join(timeout=0.5)
        except Exception as e:
            msg = f"For message {thread.message} provided message " \
                  f"processor function has thrown an error: {e}"
            self.logger.exception(msg)

            if thread.is_alive():
                return False, False, msg
            else:
                return True, False, msg

        if thread.is_alive():  # timed out
            return False, None, None  # Result is not available yet, running
        else:
            return True, True, None

    def stop(self) -> None:
        self._to_stop = True

    def _add_new_issue(self, issue: str) -> None:
        if self._healthy:
            self._healthy = False
        if len(self._latest_issues) < 10:
            self._latest_issues.append(issue)

    def _check_runtime_errors(self) -> None:
        # TODO: Remove me after testing
        running_threads = len(self._running_threads)
        if running_threads != self._currently_being_processed:
            msg = f"N of running threads {running_threads} != " \
                  f"N of running jobs {self._currently_being_processed}"
            self.logger.error(msg)
            self._add_new_issue(msg)

        if self._currently_being_processed < 0:
            msg = "Processing negative number of jobs!"
            self.logger.error(msg)
            self._add_new_issue(msg)

        if self._currently_being_processed != len(self._messages_being_processed):
            msg = "Number of message IDs != N of messages being processed"
            self.logger.error(msg)
            self._add_new_issue(msg)
