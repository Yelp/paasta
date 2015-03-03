#!/usr/bin/env python
"""
A simple script to enumerate all smartstack namespaces and output
a /etc/services compatible file
"""
import os
import sys
import service_configuration_lib


def get_service_lines_for_service(service):
    lines = []
    config = service_configuration_lib.read_service_configuration(service)
    port = config.get('port', None)
    if port is not None:
        lines.append("%s (%d/tcp)" % (service, port))
    smartstack_config = service_configuration_lib.read_extra_service_information(service, 'smartstack')
    for namespace in smartstack_config:
        proxy_port = smartstack_config[namespace].get('proxy_port', None)
        if proxy_port is not None:
            lines.append("%s.%s (%d/tcp)" % (service, namespace, proxy_port))
    return lines


def main():
    strings = []
    for service in os.listdir(service_configuration_lib.DEFAULT_SOA_DIR):
        strings.extend(get_service_lines_for_service(service))
    print "\n".join(strings)
    sys.exit(0)


if __name__ == "__main__":
    main()
