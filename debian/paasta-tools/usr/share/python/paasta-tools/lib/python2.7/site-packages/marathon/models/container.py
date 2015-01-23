from ..exceptions import InvalidChoiceError
from .base import MarathonObject


class MarathonContainer(MarathonObject):
    """Marathon health check.

    See https://mesosphere.github.io/marathon/docs/native-docker.html

    :param docker: docker field (e.g., {"image": "mygroup/myimage"})'
    :type docker: :class:`marathon.models.container.MarathonDockerContainer` or dict
    :param str type:
    :param volumes:
    :type volumes: list[:class:`marathon.models.container.MarathonContainerVolume`] or list[dict]
    """

    TYPES = ['DOCKER']
    """Valid container types"""

    def __init__(self, docker=None, type='DOCKER', volumes=None):
        if not type in self.TYPES:
            raise InvalidChoiceError('type', type, self.TYPES)
        self.type = type
        self.docker = docker if isinstance(docker, MarathonDockerContainer) \
            else MarathonDockerContainer().from_json(docker)
        self.volumes = [
            v if isinstance(v, MarathonContainerVolume) else MarathonContainerVolume().from_json(v)
            for v in (volumes or [])
        ]


class MarathonDockerContainer(MarathonObject):
    """Docker options.

    See https://mesosphere.github.io/marathon/docs/native-docker.html

    :param str image: docker image
    :param str network:
    :param port_mappings:
    :type port_mappings: list[:class:`marathon.models.container.MarathonContainerPortMapping`] or list[dict]
    """

    NETWORK_MODES=['BRIDGE', 'HOST']
    """Valid network modes"""

    def __init__(self, image=None, network='HOST', port_mappings=None):
        self.image = image
        if network:
            if not network in self.NETWORK_MODES:
                raise InvalidChoiceError('network', network, self.NETWORK_MODES)
            self.network = network
        self.port_mappings = [
            pm if isinstance(pm, MarathonContainerPortMapping) else MarathonContainerPortMapping().from_json(pm)
            for pm in (port_mappings or [])
        ]


class MarathonContainerPortMapping(MarathonObject):
    """Container port mapping.

    See https://mesosphere.github.io/marathon/docs/native-docker.html

    :param int container_port:
    :param int host_port:
    :param str protocol:
    """

    PROTOCOLS=['tcp', 'udp']
    """Valid protocols"""

    def __init__(self, container_port=None, host_port=0, service_port=None, protocol='tcp'):
        self.container_port = container_port
        self.host_port = host_port
        self.service_port = service_port
        if not protocol in self.PROTOCOLS:
            raise InvalidChoiceError('protocol', protocol, self.PROTOCOLS)
        self.protocol = protocol


class MarathonContainerVolume(MarathonObject):
    """Volume options.

    See https://mesosphere.github.io/marathon/docs/native-docker.html

    :param str container_path: container path
    :param str host_path: host path
    :param str mode: one of ['RO', 'RW']
    """

    MODES=['RO', 'RW']

    def __init__(self, container_path=None, host_path=None, mode='RW'):
        self.container_path = container_path
        self.host_path = host_path
        if not mode in self.MODES:
            raise InvalidChoiceError('mode', mode, self.MODES)
        self.mode = mode
