import json
import os
import shutil
import socket
import subprocess
import sys
from distutils.dir_util import copy_tree


def find_open_ports():
    for port in range(10000, 1, -1):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            if sock.connect_ex(('localhost', port)) != 0:
                return port


def main():
    print("Please set environment variable PAASTA_TEST_CLUSTER to the cluster you want to use.")
    print("This is necessary for tron jobs")
    cluster = os.environ.get('PAASTA_TEST_CLUSTER', 'norcal-devc')
    config_path = 'fake_config'

    copy_tree('/etc/paasta', config_path)
    # Generate tron.json
    tron_config = {'tron': {'url': f'http://tron-{cluster}:8089'}}
    with open(config_path + '/tron.json', 'w') as f:
        json.dump(tron_config, f)
    # find unused port
    port = find_open_ports()
    # Generate api endpoints
    api_endpoints = {'api_endpoints': {cluster: f'http://localhost:{port}'}}
    os.remove(config_path + '/api_endpoints.json')
    with open(config_path + '/api_endpoints.json', 'w') as f:
        json.dump(api_endpoints, f)

    try:
        # export config path
        os.environ['PAASTA_SYSTEM_CONFIG_DIR'] = config_path
        # execute paasta api command
        p = subprocess.Popen(
            ['paasta-api', '-D', '-c', cluster, str(port)],
            stderr=subprocess.STDOUT, stdout=subprocess.PIPE,
        )
        for line in iter(p.stdout.readline, b''):
            sys.stdout.write(line.decode(sys.stdout.encoding))
    finally:
        # clean up
        os.environ.pop('PAASTA_TEST_CLUSTER', None)
        shutil.rmtree(config_path)


if __name__ == '__main__':
    main()
