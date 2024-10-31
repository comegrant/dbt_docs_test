from unittest.mock import patch

from slack_connector.constants import slack_user_ids, slack_users
from slack_connector.slack_connector import get_slack_user_ids_from_short_names
from slack_connector.slack_notification import send_slack_notification


def test_get_slack_user_ids_from_short_names() -> None:
    """Test the get_slack_user_ids_from_short_names function."""
    # Test individual user
    assert get_slack_user_ids_from_short_names(["stephen"]) == f"<@{slack_user_ids['stephen']}>"

    # Test group
    engineering_ids = " ".join(f"<@{uid}>" for uid in slack_users["engineering"])
    assert get_slack_user_ids_from_short_names(["engineering"]) == engineering_ids

    # Test mixed input
    mixed_expected = f"<@{slack_user_ids['stephen']}> " + engineering_ids
    assert get_slack_user_ids_from_short_names(["stephen", "engineering"]) == mixed_expected

    # Test unknown user
    assert get_slack_user_ids_from_short_names(["unknown_user"]) == "@unknown_user"


@patch("slack_connector.slack_notification.send_slack_message_via_webhook")
def test_send_slack_notification(mock_send_slack_message) -> None: #NOQA: ANN001
    """Test the send_slack_notification function."""
    send_slack_notification(
        environment="dev",
        header_message="Test Header",
        body_message="Test Body",
        relevant_people=["stephen", "stevo", "engineering"],  # Each name should be a separate string
        is_error=False,
    )

    mock_send_slack_message.assert_called_once()
    args, _ = mock_send_slack_message.call_args
    blocks = args[0]

    # Check if the correct number of blocks are present
    assert len(blocks) == 3, "Expected 3 blocks in the Slack message"

    # Check the header block
    assert blocks[0]["type"] == "header"
    assert blocks[0]["text"]["text"] == "Test Header"

    # Check the body block
    assert blocks[1]["type"] == "section"
    assert blocks[1]["text"]["text"] == "Test Body"

    # Check the relevant people block
    assert blocks[2]["type"] == "section"
    message_text = blocks[2]["text"]["text"]

    # Check for stephen's ID
    assert f"<@{slack_user_ids['stephen']}>" in message_text
    # Check for engineering group members
    for eng_id in slack_users["engineering"]:
        assert f"<@{eng_id}>" in message_text
    # Check for unknown user
    assert "@stevo" in message_text
