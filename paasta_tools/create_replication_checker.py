from paasta_tools.api import api
from paasta_tools import kubernetes_tools
from paasta_tools.smartstack_tools import KubeSmartstackEnvoyReplicationChecker

from paasta_tools.fake_zipkin import fake_zipkin


api.setup_paasta_api()

nodes = kubernetes_tools.get_all_nodes(api.settings.kubernetes_client)

with fake_zipkin("create checker"):
    KubeSmartstackEnvoyReplicationChecker(
        nodes=nodes,
        system_paasta_config=api.settings.system_paasta_config,
    )
