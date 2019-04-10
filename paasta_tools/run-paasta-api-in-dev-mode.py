import json
import os
from distutils.dir_util import copy_tree

from paasta_tools.cli.utils import pick_random_port


def main():
    print('-------------------------------------------------------')
    print('Please run echo $$PAASTA_SYSTEM_CONFIG_DIR to continue')
    print("Please set environment variable PAASTA_TEST_CLUSTER to the cluster you want to use.")
    print("This is necessary for tron jobs")
    print('-------------------------------------------------------')
    cluster = os.environ.get('PAASTA_TEST_CLUSTER', 'norcal-devc')
    config_path = 'etc_paasta_for_development'

    copy_tree('/etc/paasta', os.path.join(os.getcwd(), config_path))
    # Generate tron.json
    tron_config = {'tron': {'url': f'http://tron-{cluster}:8089'}}
    with open(config_path + '/tron.json', 'w') as f:
        json.dump(tron_config, f)
    # find unused port
    port = pick_random_port('paasta-dev-api')
    # Generate api endpoints
    api_endpoints = {'api_endpoints': {cluster: f'http://localhost:{port}'}}
    api_endpoints_path = os.path.join(os.getcwd(), config_path, 'api_endpoints.json')
    os.chmod(api_endpoints_path, 0o777)
    with open(api_endpoints_path, 'w') as f:
        json.dump(api_endpoints, f)

    # export config path
    os.environ['PAASTA_SYSTEM_CONFIG_DIR'] = config_path
    os.execl(
        '.tox/py36-linux/bin/python', '.tox/py36-linux/bin/python',
        '-m', 'paasta_tools.api.api', *['-D', '-c', cluster, str(port)],
    )


if __name__ == '__main__':
    main()
