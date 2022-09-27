import argparse
import asyncio
import json
import logging
import traceback

import aiohttp

from paasta_tools import marathon_tools
from paasta_tools import mesos_tools
from paasta_tools import tron_tools
from paasta_tools import utils

log = logging.getLogger(__name__)

LOG_COMPONENT = "task_lifecycle"


class UnknownFrameworkError(Exception):
    pass


def interpret_task_updated(task_updated) -> str:
    message = task_updated["status"].get("message")
    healthy = task_updated["status"].get("healthy")
    reason = task_updated["status"].get("reason")
    state = task_updated["state"]

    if state == "TASK_STARTING":
        return state
    if state == "TASK_RUNNING":
        if reason == "REASON_TASK_HEALTH_CHECK_STATUS_UPDATED":
            return f"Health check update: healthy={healthy}"
        elif reason is None:
            return f"Task running, no healthchecks defined."
        else:
            return "Unknown: TASK_RUNNING but unrecognized status.reason"
    if state == "TASK_KILLED":
        if healthy is False:
            message = task_updated["status"]["message"]
            if message == "Container exited with status 143":
                return "Killed by Mesos due to healthcheck failures"
            else:
                return f"Unknown: TASK_KILLED with healthy={healthy!r} but unrecognized status.message"
        elif healthy is None:
            return "Killed by Paasta (bounce? autoscaling?)"
        else:
            return f"Unknown: TASK_KILLED but unrecognized status.healthy={healthy!r}"
    elif state == "TASK_FAILED":
        if message == "Container exited with status 137":
            return f"Probable OOM: {message}"
        elif message is None:
            return f"Unknown: TASK_FAILED but status.message is None. This seems to happen when a task exits very quickly on startup. Mesos usually follows up with a corrected message shortly."
        elif message.startswith("Container exited with status "):
            return f"TASK_FAILED: {message}"
        else:
            return f"Unknown: TASK_FAILED but unrecognized status.message"
    elif state == "TASK_FINISHED":
        if message is None:
            return f"Unknown: TASK_FINISHED but status.message is None. This seems to happen when a task exits very quickly on startup. Mesos usually follows up with a corrected message shortly."
        return f"TASK_FINISHED: {message}"
    else:
        return f"Unknown: unrecognized state"


class MesosEventSubscriber:
    def __init__(self, cluster):
        self.framework_id_to_name = {}
        self.framework_id_task_id_to_git_sha = {}
        self.cluster = cluster

    async def subscribe(self):
        # This connection should live ~forever, so disable some timeouts.
        timeout = aiohttp.ClientTimeout(
            total=None,
            sock_read=None,
            connect=30,
            sock_connect=30,
        )
        async with aiohttp.ClientSession(timeout=timeout) as session:
            payload = '{"type":"SUBSCRIBE"}'
            master_host_port = mesos_tools.find_mesos_leader(cluster=self.cluster)
            async with session.post(
                f"http://{master_host_port}/api/v1",
                data=payload,
                # allow_redirects=True,
                headers={"Content-Type": "application/json"},
                timeout=timeout,
            ) as resp:
                while True:
                    _size = await resp.content.readline()
                    if not _size:
                        break
                    size = int(_size)
                    record = await resp.content.readexactly(size)
                    yield json.loads(record)

    def determine_service_instance(self, framework_name, status):
        executor_id = status.get("executor_id", {}).get("value")
        task_id = status.get("task_id", {}).get("value")

        if framework_name.startswith("marathon"):
            return marathon_tools.parse_service_instance_from_executor_id(
                executor_id or task_id
            )
        elif framework_name.startswith("tron"):
            return tron_tools.decompose_executor_id(executor_id)[:2]
        elif framework_name.startswith("paasta-remote "):
            # sorta gross, but it's the same format.
            return marathon_tools.parse_service_instance_from_executor_id(executor_id)
        else:
            raise UnknownFrameworkError(
                f"don't know how to parse task IDs for framework {framework_name}"
            )

    def skip_updates_from_framework(self, framework_name) -> bool:
        if framework_name.startswith("jupyterhub_"):
            return True
        if framework_name.startswith("jenkins"):
            return True
        if framework_name.startswith("paasta_spark_run"):
            return True
        return False

    def handler_task_updated(self, event):
        task_updated = event["task_updated"]
        try:
            del task_updated["status"]["data"]
        except KeyError:
            pass

        framework_name = self.framework_id_to_name[
            task_updated["framework_id"]["value"]
        ]
        if self.skip_updates_from_framework(framework_name):
            return

        service, instance = self.determine_service_instance(
            framework_name, task_updated["status"]
        )
        git_sha = self.framework_id_task_id_to_git_sha.get(
            (
                task_updated["framework_id"]["value"],
                task_updated["status"]["task_id"]["value"],
            )
        )

        self.log_task_updated(
            service=service,
            instance=instance,
            git_sha=git_sha,
            task_updated=task_updated,
        )

    def log_task_updated(self, service, instance, git_sha, task_updated):
        message = {
            "type": "mesos_task_updated",
            "is_terminal": task_updated["state"] in mesos_tools.TERMINAL_STATES,
            "interpretation": interpret_task_updated(task_updated),
            "git_sha": git_sha,
            "task_updated": task_updated,
        }
        utils._log(
            service=service,
            instance=instance,
            component=LOG_COMPONENT,
            cluster=self.cluster,
            line=json.dumps(message),
        )

    def handler_subscribed(self, event):
        state = event["subscribed"]["get_state"]
        for framework in state["get_frameworks"]["frameworks"]:
            framework_info = framework["framework_info"]
            self.register_framework(framework_info)
        for task in state["get_tasks"]["tasks"]:
            self.register_task(task)

    def register_framework(self, framework_info):
        self.framework_id_to_name[framework_info["id"]["value"]] = framework_info[
            "name"
        ]

    def register_task(self, task):
        framework_name = self.framework_id_to_name[task["framework_id"]["value"]]
        if self.skip_updates_from_framework(framework_name):
            return
        git_sha = self.get_git_sha_from_task_dict(task)
        self.framework_id_task_id_to_git_sha[
            task["framework_id"]["value"], task["task_id"]["value"]
        ] = git_sha

    def get_git_sha_from_task_dict(self, task):
        try:
            docker_image = task["container"]["docker"]["image"]
        except KeyError:
            try:
                docker_image = task["container"]["mesos"]["image"]["docker"]["name"]
            except KeyError:
                log.debug("Could not figure out what docker image task uses: {task}")
                return None

        return utils.get_git_sha_from_dockerurl(docker_image)

    def handler_framework_added(self, event):
        self.register_framework(event["framework_added"]["framework"]["framework_info"])

    def handler_task_added(self, event):
        self.register_task(event["task_added"]["task"])

    async def follow(self):
        async for event in self.subscribe():
            try:
                func = {
                    "SUBSCRIBED": self.handler_subscribed,
                    "TASK_UPDATED": self.handler_task_updated,
                    "FRAMEWORK_ADDED": self.handler_framework_added,
                    "TASK_ADDED": self.handler_task_added,
                }[event["type"]]
            except KeyError:
                log.debug(f"Unknown event type {event['type']}")
                continue

            try:
                func(event)
            except Exception:
                log.error(traceback.format_exc())


def main():
    system_paasta_config = utils.load_system_paasta_config()
    try:
        cluster = system_paasta_config.get_cluster()
    except utils.PaastaNotConfiguredError:
        cluster = None

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--cluster", type=str, default=cluster, required=(cluster is None)
    )
    args = parser.parse_args()

    loop = asyncio.get_event_loop()
    subscriber = MesosEventSubscriber(cluster=args.cluster)

    loop.run_until_complete(subscriber.follow())


if __name__ == "__main__":
    main()
