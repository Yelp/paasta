"""Record the Slack thread of each deploy in DynamoDB, so other tools can reply in it.

Recording is best-effort: any failure is logged and swallowed, so it can never
affect a deploy.
"""
import logging
import os
import time
from typing import Dict
from typing import List
from typing import Optional
from typing import Sequence
from typing import Union

import boto3
from botocore.config import Config

from paasta_tools.utils import SystemPaastaConfig
from paasta_tools.utils import get_username

log = logging.getLogger(__name__)

# Lets a manual run target another table (e.g. a test table) without changing system config
TABLE_NAME_ENV_VAR = "PAASTA_DEPLOY_SLACK_THREADS_TABLE"
DEFAULT_TTL_DAYS = 30

# A DynamoDB attribute value as sent to the low-level client, e.g. {"S": "foo"}
AttributeValue = Dict[str, Union[str, List[Dict[str, str]]]]


def build_item(
    service: str,
    deploy_group: str,
    commit: str,
    channel_id: str,
    channel_name: Optional[str],
    thread_ts: str,
    authors: Optional[Sequence[str]],
    ttl_days: int,
    now_ms: int,
) -> Dict[str, AttributeValue]:
    jenkins_build_url = os.environ.get("BUILD_URL")
    created_at = now_ms // 1000
    item: Dict[str, AttributeValue] = {
        "service_deploy_group": {"S": f"{service}#{deploy_group}"},
        "created_at_ms": {"N": str(now_ms)},
        "service": {"S": service},
        "deploy_group": {"S": deploy_group},
        "channel": {"S": channel_id},
        "thread_ts": {"S": str(thread_ts)},
        "sha": {"S": commit},
        "initiator": {"S": "jenkins" if jenkins_build_url else "human"},
        "initiated_by": {"S": jenkins_build_url or get_username()},
        "created_at": {"N": str(created_at)},
        "expires_at": {"N": str(created_at + ttl_days * 24 * 60 * 60)},
    }
    if channel_name:
        item["channel_name"] = {"S": channel_name}
    if authors:
        item["authors"] = {"L": [{"S": author} for author in authors]}
    return item


def record_deploy_slack_thread(
    system_paasta_config: SystemPaastaConfig,
    service: str,
    deploy_group: str,
    commit: str,
    channel_id: str,
    channel_name: Optional[str],
    thread_ts: str,
    authors: Optional[Sequence[str]] = None,
) -> None:
    """Write a record of the deploy's Slack thread. Never raises."""
    try:
        config = system_paasta_config.get_deploy_slack_threads_config() or {}
        table_name = os.environ.get(TABLE_NAME_ENV_VAR) or config.get("table_name")
        if not table_name:
            return

        client = boto3.client(
            "dynamodb",
            region_name=config.get("region"),
            # keep this fast: a slow or unreachable DynamoDB must not hold up a deploy
            config=Config(
                connect_timeout=2,
                read_timeout=2,
                retries={"max_attempts": 2, "mode": "standard"},
            ),
        )
        client.put_item(
            TableName=table_name,
            Item=build_item(
                service=service,
                deploy_group=deploy_group,
                commit=commit,
                channel_id=channel_id,
                channel_name=channel_name,
                thread_ts=thread_ts,
                authors=authors,
                ttl_days=config.get("ttl_days", DEFAULT_TTL_DAYS),
                now_ms=int(time.time() * 1000),
            ),
            ConditionExpression="attribute_not_exists(service_deploy_group)",
        )
    except Exception:
        log.warning(
            "Failed to record the deploy Slack thread; continuing with the deploy",
            exc_info=True,
        )
