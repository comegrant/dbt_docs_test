import requests

try:
    from slack_connector.constants import slack_users
except ModuleNotFoundError: #Needed when running from severless compute in Databricks
    from constants import slack_users


def send_slack_message_via_webhook(blocks: list, webhook_url: str) -> None:
    """
    Sends blocks to Slack using the provided webhook URL.

    Args:
        blocks (list): A list of message content to send to Slack.
        webhook_url (str): The Slack Incoming Webhook URL.

    Raises:
        ValueError: If the response from Slack is not successful.
    """

    payload = {
        "blocks": blocks
    }
    headers = {
        "Content-Type": "application/json"
    }
    response = requests.post(webhook_url, json=payload, headers=headers)
    if response.status_code != 200: #noqa: PLR2004
        raise ValueError(f"Error sending Slack messages: {response.text}")


def get_slack_user_ids_from_short_names(short_names: list) -> str:
    """
    Get Slack mentions for the given short names or group names.

    Args:
        short_names (list): A list of user short names or group names.

    Returns:
        str: A string of Slack mentions for the users or groups.
    """
    mentions = []
    for short_name in short_names:
        normalized_name = short_name.lower().strip()
        if normalized_name in slack_users:
            if isinstance(slack_users[normalized_name], list):
                # It's a group, mention all users in the group
                mentions.extend([f"<@{user_id}>" for user_id in slack_users[normalized_name]])
            else:
                # It's an individual user
                mentions.append(f"<@{slack_users[normalized_name]}>")
        else:
            mentions.append(f"@{short_name}")  # Fallback if name not found
    return " ".join(mentions)
