#!/usr/bin/python3.7
import argparse
from os import environ
from random import choice
from subprocess import run
from textwrap import indent
from typing import List
from urllib.parse import urlparse

import yaml

ALLOWED_CLUSTERS = ["kubestage"]


def inv_search(*filters: str) -> List[str]:
    """Search inventory service and return matching hosts"""
    _filters = [f"-F {item}" for item in filters]
    return run(["inv", "find"] + _filters, capture_output=True).stdout.decode().split()


class KubernetesResetter:
    """Reset the cluster."""

    dry: bool

    master_node_names: List[str]
    other_node_names: List[str]

    etcd_prefix: str
    etcd_host: str

    cluster_name: str
    cluster_ecosystem: str

    def __init__(self, dry=True) -> None:
        self.dry = dry
        self.master_node_names = []
        self.other_node_names = []
        with open("/nail/etc/ecosystem", "r+") as _eco:
            self.cluster_ecosystem = _eco.read().strip()
        self.prepare_static()

    def prepare_static(self):
        """Fallback if the API server is unreachable/broken/etc
        Does not have all features since we don't know which nodes exist"""
        with open("/etc/kubernetes/kubeadm-config.yaml", "r+") as _kubeadm_cfg:
            kubeadms = list(yaml.safe_load_all(_kubeadm_cfg.read()))
        # the kubeadm config contains multiple yaml documents, and to not ensure the order
        # we iterate through them to match by API Kind
        kubeadm = {}
        for doc in kubeadms:
            if doc.get("kind") != "ClusterConfiguration":
                continue
            kubeadm = doc
        # Same as with the API Server variant below
        self.etcd_prefix = (
            kubeadm.get("apiServer", {}).get("extraArgs", {}).get("etcd-prefix")
        )
        etcd_hosts_uri = kubeadm["etcd"]["external"]["endpoints"]
        self.etcd_host = urlparse(choice(etcd_hosts_uri)).hostname

        self.cluster_name = kubeadm["clusterName"]
        if self.cluster_name not in ALLOWED_CLUSTERS:
            raise ValueError(f"Current cluster of {self.cluster_name} is not allowed.")

        # try to find all nodes via inv
        self.master_node_names = inv_search(
            f"kube_cluster={self.cluster_name}",
            "kube_role=master",
            f"ecosystem={self.cluster_ecosystem}",
        )
        self.other_node_names = inv_search(
            f"kube_cluster={self.cluster_name}",
            "kube_role=node",
            f"ecosystem={self.cluster_ecosystem}",
        )

    def prepare_report(self):
        """Print a report of all variables we have gathered during preparation. This is to ensure
        that nothing gets deleted on accident"""
        print("#####################################################")
        print("Cluster reset summary:")
        print("\tKubernetes master nodes:")
        for node in self.master_node_names:
            print(f"\t\tNode '{node}'")
        print("\tKubernetes worker nodes:")
        for node in self.other_node_names:
            print(f"\t\tNode '{node}'")
        print(f"\tetcd Host:\t\t'{self.etcd_host}'")
        print(f"\tetcd Prefix:\t\t'{self.etcd_prefix}'")
        print(f"\tCluster name:\t\t'{self.cluster_name}'")
        print(f"\tCluster ecosystem:\t'{self.cluster_ecosystem}'")
        print("")
        print("If any of these fields don't make sense or are empty, abort.")
        print("")
        print("#####################################################")

    def cmd(self, hosts: List[str], command: str) -> None:
        """Wrapper around _run from paasta_tools,
        which runs commands over SSH using the correct user"""
        # Script needs to run as root so we can access the kubeconfig admin file
        # but we need the correct user to SSH into boxes
        actual_user = environ["SUDO_USER"]
        command = (
            ["sudo", "-u", actual_user, "ssh-list",]
            + hosts
            + ["--", "bash", "-c", f"'{command}'"]
        )
        command_string = " ".join(command)
        if self.dry:
            print(f"\tWould've ran '{command_string}'")
            return
        result = run(command, capture_output=True)
        if result.returncode != 0:
            print(f"Non-zero exit code {result.returncode}")
        print(f"\t---command output ({command_string})---")
        print(indent(result.stdout.decode(), "\t\t"))
        print("\t---end command output---")

    def run(self) -> None:
        """Actually reset the cluster"""
        # Stop kubernetes components on all master
        print("> Disabling puppet on kube masters...")
        self.cmd(
            self.master_node_names + self.other_node_names,
            'toggle-puppet disable --reason "Resetting cluster"',
        )
        print("> Disable monitoring...")
        self.cmd(
            self.master_node_names + self.other_node_names,
            'downtime $(hostname -f) 3h "Resetting cluster"',
        )
        print("> Stopping kubelet on masters...")
        self.cmd(self.master_node_names, "sudo systemctl stop kubelet")
        print("> Killing all containers...")
        self.cmd(self.master_node_names, "sudo docker kill $(sudo docker ps -q)")
        print("> Pruning all containers...")
        self.cmd(self.master_node_names, "sudo docker container prune -f")
        print("> Removing kubernetes_bootstrap_done.txt...")
        self.cmd(
            self.master_node_names,
            "sudo rm -f /etc/facter/facts.d/kubernetes_bootstrap_done.txt",
        )
        print("> Running kubeadm reset, this may take a while...")
        self.cmd(self.master_node_names, "sudo kubeadm reset -f")

        print("> Removing Puppet resource state in S3")
        self.cmd(
            [self.master_node_names[0]],
            (
                "aws s3 rm --recursive s3://yelp-kubernetes-puppet-resources-"
                f"state-{self.cluster_ecosystem}/{self.cluster_name}/"
            ),
        )

        print(
            f"> Deleting etcd prefix '/{self.etcd_prefix}' on host '{self.etcd_host}'"
        )
        self.cmd(
            [self.etcd_host], f"sudo etcdctl del '/{self.etcd_prefix}' --from-key=true"
        )

        print(f"> Running puppet (on {self.master_node_names[0]} node)")
        self.cmd([self.master_node_names[0]], "sudo run-puppet -f")
        print(f"> Running kube-init (on {self.master_node_names[0]} node)")
        # # -H is required to set the home directory to the one of root
        # # which has the .dockercfg file to pull k8s images
        self.cmd([self.master_node_names[0]], "sudo -H /nail/sys/bin/kube-init")
        print("> Running puppet (on all nodes)")
        self.cmd(
            self.master_node_names, "sudo run-puppet -f",
        )
        print("> Running kube-init")
        self.cmd(self.master_node_names, "sudo -H /nail/sys/bin/kube-init")
        print("> Untainting master nodes...")
        for host in self.master_node_names:
            self.cmd(
                [host],
                (
                    "sudo KUBECONFIG=/etc/kubernetes/admin.conf kubectl taint nodes "
                    f"{host} node-role.kubernetes.io/master:NoSchedule-"
                ),
            )

        print("> Restarting kubelet on all nodes...")
        self.cmd(self.other_node_names, "sudo systemctl restart kubelet")
        print("> Re-enabling puppet (on all nodes)")
        self.cmd(
            self.master_node_names + self.other_node_names,
            'sudo toggle-puppet enable --reason "Restarting cluster after reset"',
        )
        print("Successfully reset cluster.")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Nukes kubernetes clusters.")
    parser.add_argument("--dry-run", dest="dry_run", action="store_true")
    parser.add_argument("--no-dry-run", dest="dry_run", action="store_false")
    parser.set_defaults(dry_run=True)
    args = parser.parse_args()
    return args


def main() -> None:
    args = parse_args()
    resetter = KubernetesResetter(args.dry_run)
    resetter.prepare_report()
    if not args.dry_run:
        confirmation_check = "Yes I want to reset this cluster"
        print(f"Please entry '{confirmation_check}' to confirm and start nuking")
        confirmation = input()
        if confirmation != confirmation_check:
            raise ValueError()
    # always run since we check for dry run above
    resetter.run()


if __name__ == "__main__":
    main()
