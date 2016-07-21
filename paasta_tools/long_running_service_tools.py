import logging

from paasta_tools.utils import InstanceConfig

log = logging.getLogger(__name__)
logging.getLogger('marathon').setLevel(logging.WARNING)


class LongRunningServiceConfig(InstanceConfig):
    def __init__(self, service, cluster, instance, config_dict, branch_dict):
        super(LongRunningServiceConfig, self).__init__(
            cluster=cluster,
            instance=instance,
            service=service,
            config_dict=config_dict,
            branch_dict=branch_dict,
        )
