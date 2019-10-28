from clusterman.args import add_cluster_arg
from clusterman.args import subparser
from clusterman.util import get_cluster_name_list
from clusterman.util import get_pool_name_list


def list_clusters(args):  # pragma: no cover
    print('\n'.join(get_cluster_name_list()))


@subparser('list-clusters', 'list available clusters', list_clusters)
def add_mesos_list_clusters_parser(subparser, required_named_args, optional_named_args):  # pragma: no cover
    pass


def list_pools(args):  # pragma: no cover
    print('\n'.join(get_pool_name_list(args.cluster)))


@subparser('list-pools', 'list available pools in a cluster', list_pools)
def add_mesos_list_pools_parser(subparser, required_named_args, optional_named_args):  # pragma: no cover
    add_cluster_arg(required_named_args, required=True)
