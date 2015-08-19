import shutil

from docker import Client

from paasta_tools.utils import get_docker_host


def after_scenario(context, scenario):
    if getattr(context, "tmpdir", None):
        shutil.rmtree(context.tmpdir)
    if getattr(context, "running_container_id", None):
        base_docker_url = get_docker_host()
        docker_client = Client(base_url=base_docker_url)
        docker_client.stop(container=context.running_container_id)
        docker_client.remove_container(container=context.running_container_id)
