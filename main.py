import argparse

from app import App


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--concurrent_messages",
        type=int,
        default=1,
        help="Number of messages that could be processed concurrently"
    )
    parser.add_argument(
        "--report_every",
        type=int,
        default=30,
        help="How often a slack message will be sent reporting the condition"
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    app = App(
        sleep_time_between_health_reports=args.report_every,
        concur_processing_jobs=args.concurrent_messages
    )
    try:
        app.run()
    except KeyboardInterrupt:
        app.stop_processor()

    return 0


if __name__ == '__main__':
    main()
