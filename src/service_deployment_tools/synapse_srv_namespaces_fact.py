#!/usr/bin/env python
import os
import sys
import marathon_tools
import service_configuration_lib


def main():
    ID_SPACER = marathon_tools.ID_SPACER
    soa_dir = service_configuration_lib.DEFAULT_SOA_DIR
    strings = []
    for srv_dir in os.listdir(os.path.abspath(soa_dir)):
        for namespace, config in marathon_tools.get_all_namespaces_for_service(srv_dir, soa_dir):
            if 'proxy_port' in config:
                strings.append('%s%s%s:%s' % (srv_dir, ID_SPACER, namespace, config['proxy_port']))
    strings = sorted(strings)
    print ','.join(strings)
    sys.exit(0)


if __name__ == "__main__":
    main()