import json
import sys

import tomli
import tomli_w

containerdcfg_file_path = sys.argv[1]
with open(containerdcfg_file_path, "rb") as containerdcfg_file:
    containerdcfg = tomli.load(containerdcfg_file)

with open("/nail/etc/docker-registry-ro") as dockercfg_file:
    dockercfg = json.load(dockercfg_file)

registry = list(dockercfg.keys())[0]

containerdcfg["plugins"]["io.containerd.grpc.v1.cri"]["registry"] = {
    "configs": {registry: {"auth": {"auth": dockercfg[registry]["auth"]}}},
    "mirrors": {registry: {"endpoint": [f"https://{registry}"]}},
}

with open(containerdcfg_file_path, "wb") as containerdcfg_file:
    tomli_w.dump(containerdcfg, containerdcfg_file)
