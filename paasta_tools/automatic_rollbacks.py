def get_slack_blocks_for_initial_deployment(message):
    blocks = [
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": message,
            },
        },
        {
            "type": "actions",
            "block_id": "rollback_block1",
            "elements": [
                {
                    "type": "button",
                    "text": {
                            "type": "plain_text",
                            "text": "Roll Back (Not Implemented)",
                    },
                    "value": "rollback",
                },
                {
                    "type": "button",
                    "text": {
                            "type": "plain_text",
                            "text": "Continue (Not Implemented)",
                    },
                    "value": "continue",
                },
            ],
        },
    ]
    return blocks
