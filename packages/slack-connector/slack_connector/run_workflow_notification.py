"""Module for sending Slack notifications from command-line arguments."""

import argparse

try:
    from slack_connector.slack_notification import send_slack_notification
except ModuleNotFoundError:  # Needed when running from serverless compute in Databricks
    from slack_notification import send_slack_notification


def main() -> None:
    """Parse command-line arguments and send Slack notification."""
    parser = argparse.ArgumentParser(description="Send Slack notifications")
    parser.add_argument(
        "-hm",
        "--header_message",
        help="The header message to send",
    )
    parser.add_argument(
        "-bm",
        "--body_message",
        help="The body message to send",
    )
    parser.add_argument(
        "-p",
        "--relevant_people",
        help="Comma-separated list of relevant people short names",
    )
    parser.add_argument(
        "-e",
        "--environment",
        default="local_dev",
        choices=["local_dev", "dev", "test", "prod"],
        required=True,
        help="Environment (local_dev, dev, test, prod)",
    )
    parser.add_argument(
        "-ie",
        "--is_error",
        choices=["true", "false"],
        help="Whether the notification is an error (true, false)",
    )
    # Add an argument to accept multiple URL results from upstream tasks
    parser.add_argument("--urls", type=str, nargs="+", help="The URL values passed from upstream tasks")

    args = parser.parse_args()

    url_list = []
    if args.urls:
        # Process each result
        for _, url in enumerate(args.urls, start=1):
            url_list.append(url)

    # Convert is_error string 'true'/'false' to boolean
    is_error = args.is_error.lower() == "true" if args.is_error is not None else False

    send_slack_notification(
        header_message=args.header_message,
        body_message=args.body_message,
        relevant_people=args.relevant_people,
        environment=args.environment,
        is_error=is_error,
        urls=url_list,
    )


if __name__ == "__main__":
    main()
