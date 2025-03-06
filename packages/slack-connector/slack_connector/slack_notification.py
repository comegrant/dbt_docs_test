import logging
from datetime import datetime
from typing import Literal

import pytz

try:
    from slack_connector.constants import (
        databricks_workspace_urls,
        github_url,
        power_bi_urls,
        webhook_urls,
    )
except ModuleNotFoundError:  # Needed when running from serverless compute in Databricks
    from constants import (
        databricks_workspace_urls,
        github_url,
        power_bi_urls,
        webhook_urls,
    )
try:
    from slack_connector.slack_connector import (
        get_slack_user_ids_from_short_names,
        send_slack_message_via_webhook,
    )
except ModuleNotFoundError:  # Needed when running from serverless compute in Databricks
    from slack_connector import (
        get_slack_user_ids_from_short_names,
        send_slack_message_via_webhook,
    )


def ensure_https(url: str) -> str:
    """
    Ensures the given URL starts with https:// instead of http://

    Args:
        url: The URL to be checked

    Returns:
        A URL with https:// as the scheme if the given URL does not have it

    """
    return "https://" + url if not url.startswith("http") else url


def send_slack_notification(
    environment: Literal["local_dev", "dev", "test", "prod"],
    header_message: str | None = None,
    body_message: str | None = None,
    relevant_people: list[str] | str | None = None,
    is_error: bool = False,
    urls: list[str] | None = None,
) -> None:
    """
    Sends a Slack notification with the given message and optional owner mentions.

    Args:
        environment: The environment to use for the webhook and workspace URL ("local_dev", "dev", "test", or "prod").
        header_message: The header message content to send.
        body_message: The body message content to send.
        relevant_people: A comma-separated string or list of relevant people short names or group names.
        is_error: Whether the notification is an error (True or False).
        urls: The task url of the job that failed
    Raises:
        ValueError: If the environment is not one of the expected values.
    """
    blocks = []

    # Add header message section only if it's provided and not blank
    if header_message and header_message.strip():
        blocks.append(
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": header_message[:150],  # Limit to 150 characters
                },
            }
        )

    # Add current time if error
    if is_error:
        oslo_tz = pytz.timezone("Europe/Oslo")
        current_time = datetime.now(oslo_tz).strftime("%Y-%m-%d %H:%M:%S")
        blocks.append(
            {
                "type": "context",
                "elements": [
                    {
                        "type": "mrkdwn",
                        "text": f"⏰ {current_time}",
                    }
                ],
            }
        )

    # Add body message section only if it's provided and not blank
    if body_message and body_message.strip():
        blocks.append(
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": body_message,
                },
            }
        )

    # Add task url section only if it's provided and not blank
    if urls and any(urls):
        urls_with_https = [
            ensure_https(url.strip()) for sublist in urls for url in sublist.split(",")
        ]  # list comprehension ensures all elements are iterated over
        url_text = "\n".join(f"<{url}|Link_{i}>" for i, url in enumerate(urls_with_https))
        blocks.append(
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*🔗 URLs:*\n {url_text}",
                },
            }
        )

    # Add link to Databricks workspace
    if is_error:
        blocks.append(
            {
                "type": "actions",
                "elements": [
                    {
                        "type": "button",
                        "text": {
                            "type": "plain_text",
                            "emoji": True,
                            "text": "🧱 Databricks",
                        },
                        "url": databricks_workspace_urls[environment],
                    },
                    {
                        "type": "button",
                        "text": {
                            "type": "plain_text",
                            "emoji": True,
                            "text": "📝 GitHub",
                        },
                        "url": github_url,
                    },
                    {
                        "type": "button",
                        "text": {
                            "type": "plain_text",
                            "emoji": True,
                            "text": "🔍 PowerBI",
                        },
                        "url": power_bi_urls[environment],
                    },
                ],
            }
        )

    # Add the relevant people section if provided
    if relevant_people and environment not in ["local_dev", "dev"]:
        if isinstance(relevant_people, str):
            relevant_people = [owner.strip() for owner in relevant_people.split(",")]
        else:
            relevant_people = [owner.strip() for owner in relevant_people]

        relevant_people_mentions = get_slack_user_ids_from_short_names(relevant_people)

        blocks.append(
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*🧑‍🍳 Relevant people:*\n {relevant_people_mentions}",
                },
            }
        )

    if is_error:
        blocks.append(
            {
                "type": "context",
                "elements": [
                    {
                        "type": "mrkdwn",
                        "text": (
                            "React with 👀 if you're looking into this\n"
                            "React with ✅ if you've resolved the issue\n"
                            "Create an issue in Linear by clicking the three dots and then `Create new issue... Linear`"
                        ),
                    }
                ],
            }
        )

    # Only add blocks if there's content
    if blocks:
        send_slack_message_via_webhook(blocks, webhook_urls[environment])
    else:
        logging.warning("No content provided for Slack notification. Skipping send.")
