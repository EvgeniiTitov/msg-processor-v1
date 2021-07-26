import argparse
import sys

from app import App


# TODO: 1. Receiver doesnt release resources if fails
#       2. Test the target set test script manually on the TPU
#       3. Work on the message_processor - debug a container fails
#       4. Push code to the new repo
#       5. Update README


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--concurrent_messages",
        type=int,
        default=1,
        help="Number of messages that could be processed concurrently",
    )
    parser.add_argument(
        "--report_every",
        type=int,
        default=60,
        help="How often a slack message will be sent reporting the condition",
    )
    parser.add_argument(
        "--acknowledge_messages",
        action="store_true",
        help="Delete messages from the queue upon successful processing",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    app = App(
        sleep_time_between_health_reports=args.report_every,
        concur_processing_jobs=args.concurrent_messages,
        acknowledgement_required=args.acknowledge_messages,
    )
    try:
        app.run()
    except KeyboardInterrupt:
        app.stop_processor()
        sys.exit(0)

    app.stop_processor()

    return 0


if __name__ == "__main__":
    main()
