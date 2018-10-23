import collections
import hashlib
import io
import ipaddress
import itertools
import json
import logging
import os.path
import re
from contextlib import contextmanager

from paasta_tools import iptables
from paasta_tools.cli.utils import get_instance_config
from paasta_tools.marathon_tools import get_all_namespaces_for_service
from paasta_tools.utils import get_running_mesos_docker_containers
from paasta_tools.utils import load_system_paasta_config
from paasta_tools.utils import NoConfigurationForServiceError
from paasta_tools.utils import timed_flock


PRIVATE_IP_RANGES = (
    '127.0.0.0/255.0.0.0',
    '10.0.0.0/255.0.0.0',
    '172.16.0.0/255.240.0.0',
    '192.168.0.0/255.255.0.0',
    '169.254.0.0/255.255.0.0',
)
DEFAULT_SYNAPSE_SERVICE_DIR = '/var/run/synapse/services'
DEFAULT_FIREWALL_FLOCK_PATH = '/var/lib/paasta/firewall.flock'
DEFAULT_FIREWALL_FLOCK_TIMEOUT_SECS = 5

RESOLV_CONF = '/etc/resolv.conf'
# not exactly correct, but sufficient to filter out ipv6 or other weird things
IPV4_REGEX = re.compile('[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}$')


log = logging.getLogger(__name__)


class ServiceGroup(collections.namedtuple(
    'ServiceGroup', (
        'service',
        'instance',
    ),
)):
    """A service group.

    :param service: service name
    :param instance: instance name
    """
    __slots__ = ()

    @property
    def chain_name(self):
        """Return iptables chain name.

        Chain names are limited to 28 characters, so we have to trim quite a
        bit. To attempt to ensure we don't have collisions due to shortening,
        we append a hash to the end.
        """
        chain = 'PAASTA.{}'.format(self.service[:10])
        chain += '.' + hashlib.sha256(
            json.dumps(self).encode('utf8'),
        ).hexdigest()[:10]
        assert len(chain) <= 28, len(chain)
        return chain

    def get_rules(self, soa_dir, synapse_service_dir):
        try:
            conf = get_instance_config(
                self.service, self.instance,
                load_system_paasta_config().get_cluster(),
                load_deployments=False,
                soa_dir=soa_dir,
            )
        except NotImplementedError:
            # PAASTA-11414: new instance types may not provide this configuration information;
            # we don't want to break all of the firewall infrastructure when that happens
            return ()
        except NoConfigurationForServiceError:
            # PAASTA-12050: a deleted service may still have containers running on PaaSTA hosts
            # for several minutes after the directory disappears from soa-configs.
            return ()

        if not conf.get_outbound_firewall():
            return ()

        rules = list(_default_rules(conf, self.log_prefix))
        rules.extend(_well_known_rules(conf))
        rules.extend(_smartstack_rules(conf, soa_dir, synapse_service_dir))
        rules.extend(_cidr_rules(conf))
        return tuple(rules)

    def update_rules(self, soa_dir, synapse_service_dir):
        iptables.ensure_chain(self.chain_name, self.get_rules(soa_dir, synapse_service_dir))
        iptables.reorder_chain(self.chain_name)

    @property
    def log_prefix(self):
        # log-prefix is limited to 29 characters total
        # space at the end is necessary to separate it from the rest of the line
        # no restrictions on any particular characters afaict
        return f'paasta.{self.service}'[:28] + ' '


def _default_rules(conf, log_prefix):
    log_rule = iptables.Rule(
        protocol='ip',
        src='0.0.0.0/0.0.0.0',
        dst='0.0.0.0/0.0.0.0',
        target='LOG',
        target_parameters=(
            ('log-prefix', (log_prefix,)),
        ),
        matches=(
            (
                'limit', (
                    ('limit', ('1/sec',)),
                    ('limit-burst', ('1',)),
                ),
            ),
        ),
    )

    policy = conf.get_outbound_firewall()
    if policy == 'block':
        return (
            iptables.Rule(
                protocol='ip',
                src='0.0.0.0/0.0.0.0',
                dst='0.0.0.0/0.0.0.0',
                target='REJECT',
                matches=(),
                target_parameters=(
                    (('reject-with', ('icmp-port-unreachable',))),
                ),
            ),
            log_rule,
        )
    elif policy == 'monitor':
        return (log_rule,)
    else:
        raise AssertionError(policy)


def _well_known_rules(conf):
    # Allow access to certain resources for all services by default.
    yield iptables.Rule(
        protocol='ip',
        src='0.0.0.0/0.0.0.0',
        dst='0.0.0.0/0.0.0.0',
        target='PAASTA-COMMON',
        matches=(),
        target_parameters=(),
    )

    for dep in conf.get_dependencies() or ():
        resource = dep.get('well-known')
        if resource == 'internet':
            yield iptables.Rule(
                protocol='ip',
                src='0.0.0.0/0.0.0.0',
                dst='0.0.0.0/0.0.0.0',
                target='PAASTA-INTERNET',
                matches=(),
                target_parameters=(),
            )
        elif resource is not None:
            # TODO: handle better
            raise AssertionError(resource)


def _synapse_backends(synapse_service_dir, namespace):
    # Return the contents of the synapse JSON file for a particular service namespace
    # e.g. /var/run/synapse/services/example_happyhour.main.json
    with open(os.path.join(synapse_service_dir, namespace + '.json')) as synapse_backend_file:
        synapse_backend_json = json.load(synapse_backend_file)
        return synapse_backend_json


def _yocalhost_rule(port, comment, protocol='tcp'):
    """Return an iptables rule allowing access to a yocalhost port."""
    return iptables.Rule(
        protocol=protocol,
        src='0.0.0.0/0.0.0.0',
        dst='169.254.255.254/255.255.255.255',
        target='ACCEPT',
        matches=(
            (
                'comment',
                (
                    ('comment', (comment,)),
                ),
            ),
            (
                protocol,
                (
                    ('dport', (str(port),)),
                ),
            ),
        ),
        target_parameters=(),
    )


def _smartstack_rules(conf, soa_dir, synapse_service_dir):
    for dep in conf.get_dependencies() or ():
        namespace = dep.get('smartstack')
        if namespace is None:
            continue

        # TODO: support wildcards

        # synapse backends
        try:
            backends = _synapse_backends(synapse_service_dir, namespace)
        except (OSError, IOError, ValueError):
            # Don't fatal if something goes wrong loading the synapse files
            log.exception(f'Unable to load backend {namespace}')
            backends = ()

        for backend in backends:
            yield iptables.Rule(
                protocol='tcp',
                src='0.0.0.0/0.0.0.0',
                dst='{}/255.255.255.255'.format(backend['host']),
                target='ACCEPT',
                matches=(
                    (
                        'comment',
                        (
                            ('comment', ('backend ' + namespace,)),
                        ),
                    ),
                    (
                        'tcp',
                        (
                            ('dport', (str(backend['port']),)),
                        ),
                    ),
                ),
                target_parameters=(),
            )

        # synapse-haproxy proxy_port
        service, _ = namespace.split('.', 1)
        service_namespaces = get_all_namespaces_for_service(service, soa_dir=soa_dir)
        port = dict(service_namespaces)[namespace]['proxy_port']
        yield _yocalhost_rule(port, 'proxy_port ' + namespace)


def _ports_valid(ports):
    for port in ports:
        try:
            port = int(port)
        except ValueError:
            log.exception(f'Unable to parse port: {port}')
            return False

        if not 1 <= port <= 65535:
            log.error(f'Bogus port number: {port}')
            return False
    else:
        return True


def _cidr_rules(conf):
    for dep in conf.get_dependencies() or ():
        cidr = dep.get('cidr')
        port_str = dep.get('port')

        if cidr is None:
            continue

        try:
            network = ipaddress.IPv4Network(cidr)
        except ipaddress.AddressValueError:
            log.exception(f'Unable to parse IP network: {cidr}')
            continue

        if port_str is not None:
            # port can be either a single port like "443" or a range like "1024:65535"
            ports = str(port_str).split(':')

            if len(ports) > 2:
                log.error(f'"port" must be either a single value or a range like "1024:65535": {port_str}')
                continue

            if not _ports_valid(ports):
                continue

        # Set up an ip rule if no port, or a tcp/udp rule if there is a port
        dst = f'{network.network_address.exploded}/{network.netmask}'
        if port_str is None:
            yield iptables.Rule(
                protocol='ip',
                src='0.0.0.0/0.0.0.0',
                dst=dst,
                target='ACCEPT',
                matches=(
                    (
                        'comment',
                        (
                            ('comment', (f'allow {network}:*',)),
                        ),
                    ),
                ),
                target_parameters=(),
            )
        else:
            for proto in ('tcp', 'udp'):
                yield iptables.Rule(
                    protocol=proto,
                    src='0.0.0.0/0.0.0.0',
                    dst=dst,
                    target='ACCEPT',
                    matches=(
                        (
                            'comment',
                            (
                                ('comment', (f'allow {network}:{port_str}',)),
                            ),
                        ),
                        (
                            proto,
                            (
                                ('dport', (str(port_str),)),
                            ),
                        ),
                    ),
                    target_parameters=(),
                )


def services_running_here():
    """Generator helper that yields (service, instance, mac address) of both
    marathon and chronos tasks.
    """
    for container in get_running_mesos_docker_containers():
        if container['HostConfig']['NetworkMode'] != 'bridge':
            continue

        service = container['Labels'].get('paasta_service')
        instance = container['Labels'].get('paasta_instance')

        if service is None or instance is None:
            continue

        network_info = container['NetworkSettings']['Networks']['bridge']

        mac = network_info['MacAddress']
        ip = network_info['IPAddress']
        yield service, instance, mac, ip


def active_service_groups():
    """Return active service groups."""
    service_groups = collections.defaultdict(set)
    for service, instance, mac, ip in services_running_here():
        # TODO: only include macs that start with MAC_ADDRESS_PREFIX?
        service_groups[ServiceGroup(service, instance)].add(mac)
    return service_groups


def _dns_servers():
    with io.open(RESOLV_CONF) as f:
        for line in f:
            parts = line.split()
            if (
                    len(parts) == 2 and
                    parts[0] == 'nameserver' and
                    IPV4_REGEX.match(parts[1])
            ):
                yield parts[1]


def ensure_shared_chains():
    _ensure_dns_chain()
    _ensure_internet_chain()
    _ensure_common_chain()


def _ensure_common_chain():
    """The common chain allows access for all services to certain resources."""
    iptables.ensure_chain(
        'PAASTA-COMMON',
        (
            # Allow return traffic for incoming connections
            iptables.Rule(
                protocol='ip',
                src='0.0.0.0/0.0.0.0',
                dst='0.0.0.0/0.0.0.0',
                target='ACCEPT',
                matches=(
                    ('conntrack', (('ctstate', ('ESTABLISHED',)),)),
                ),
                target_parameters=(),
            ),
            _yocalhost_rule(1463, 'scribed'),
            _yocalhost_rule(8125, 'metrics-relay', protocol='udp'),
            _yocalhost_rule(3030, 'sensu'),
            iptables.Rule(
                protocol='ip',
                src='0.0.0.0/0.0.0.0',
                dst='0.0.0.0/0.0.0.0',
                target='PAASTA-DNS',
                matches=(),
                target_parameters=(),
            ),
        ),
    )


def _ensure_dns_chain():
    iptables.ensure_chain(
        'PAASTA-DNS',
        tuple(itertools.chain.from_iterable(
            (
                iptables.Rule(
                    protocol='udp',
                    src='0.0.0.0/0.0.0.0',
                    dst=f'{dns_server}/255.255.255.255',
                    target='ACCEPT',
                    matches=(
                        ('udp', (('dport', ('53',)),)),
                    ),
                    target_parameters=(),
                ),
                # DNS goes over TCP sometimes, too!
                iptables.Rule(
                    protocol='tcp',
                    src='0.0.0.0/0.0.0.0',
                    dst=f'{dns_server}/255.255.255.255',
                    target='ACCEPT',
                    matches=(
                        ('tcp', (('dport', ('53',)),)),
                    ),
                    target_parameters=(),
                ),
            )
            for dns_server in _dns_servers()
        )),
    )


def _ensure_internet_chain():
    iptables.ensure_chain(
        'PAASTA-INTERNET',
        (
            iptables.Rule(
                protocol='ip',
                src='0.0.0.0/0.0.0.0',
                dst='0.0.0.0/0.0.0.0',
                target='ACCEPT',
                matches=(),
                target_parameters=(),
            ),
        ) + tuple(
            iptables.Rule(
                protocol='ip',
                src='0.0.0.0/0.0.0.0',
                dst=ip_range,
                target='RETURN',
                matches=(),
                target_parameters=(),
            )
            for ip_range in PRIVATE_IP_RANGES
        ),
    )


def ensure_service_chains(service_groups, soa_dir, synapse_service_dir):
    """Ensure service chains exist and have the right rules.

    service_groups is a dict {ServiceGroup: set([mac_address..])}

    Returns dictionary {[service chain] => [list of mac addresses]}.
    """
    chains = {}
    for service, macs in service_groups.items():
        service.update_rules(soa_dir, synapse_service_dir)
        chains[service.chain_name] = macs
    return chains


def dispatch_rule(chain, mac):
    return iptables.Rule(
        protocol='ip',
        src='0.0.0.0/0.0.0.0',
        dst='0.0.0.0/0.0.0.0',
        target=chain,
        matches=(
            ('mac', (('mac-source', (mac.upper(),)),)),
        ),
        target_parameters=(),
    )


def ensure_dispatch_chains(service_chains):
    paasta_rules = set(itertools.chain.from_iterable(
        (
            dispatch_rule(chain, mac)
            for mac in macs
        )
        for chain, macs in service_chains.items()

    ))
    iptables.ensure_chain('PAASTA', paasta_rules)

    jump_to_paasta = iptables.Rule(
        protocol='ip',
        src='0.0.0.0/0.0.0.0',
        dst='0.0.0.0/0.0.0.0',
        target='PAASTA',
        matches=(),
        target_parameters=(),
    )
    iptables.ensure_rule('INPUT', jump_to_paasta)
    iptables.ensure_rule('FORWARD', jump_to_paasta)


def garbage_collect_old_service_chains(desired_chains):
    current_paasta_chains = {
        chain
        for chain in iptables.all_chains()
        if chain.startswith('PAASTA.')
    }
    for chain in current_paasta_chains - set(desired_chains):
        iptables.delete_chain(chain)


def general_update(soa_dir, synapse_service_dir):
    """Update iptables to match the current PaaSTA state."""
    ensure_shared_chains()
    service_chains = ensure_service_chains(active_service_groups(), soa_dir, synapse_service_dir)
    ensure_dispatch_chains(service_chains)
    garbage_collect_old_service_chains(service_chains)


def prepare_new_container(soa_dir, synapse_service_dir, service, instance, mac):
    """Update iptables to include rules for a new (not yet running) MAC address
    """
    ensure_shared_chains()  # probably already set, but just to be safe
    service_group = ServiceGroup(service, instance)
    service_group.update_rules(soa_dir, synapse_service_dir)
    iptables.insert_rule('PAASTA', dispatch_rule(service_group.chain_name, mac))


@contextmanager
def firewall_flock(flock_path=DEFAULT_FIREWALL_FLOCK_PATH):
    """ Grab an exclusive flock to avoid concurrent iptables updates
    """
    with io.FileIO(flock_path, 'w') as f:
        with timed_flock(f, seconds=DEFAULT_FIREWALL_FLOCK_TIMEOUT_SECS):
            yield
