import json
import sys

import toml

containerdcfg_file_path = sys.argv[1]
containerdcfg = toml.load(containerdcfg_file_path)
dockercfg = json.load(open("/nail/etc/docker-registry-ro"))
registry = list(dockercfg.keys())[0]

containerdcfg["plugins"]["io.containerd.grpc.v1.cri"]["registry"]["configs"] = {
    registry: {"auth": {"auth": dockercfg[registry]["auth"]}}
}

containerdcfg["plugins"]["io.containerd.grpc.v1.cri"]["registry"]["mirrors"][
    registry
] = {"endpoint": [f"https://{registry}"]}

with open(containerdcfg_file_path, "w") as containerdcfg_file:
    containerdcfg_file.write(toml.dumps(containerdcfg))
