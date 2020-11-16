#!/opt/venvs/paasta-tools/bin/python
import os
import sys
from filecmp import cmp
from shutil import copy
from subprocess import CalledProcessError
from subprocess import run
from traceback import print_exc

APPLIED_DIRECTORY = ".applied"


# This script expects the KUBECONFIG environment variable to be set correctly
def main(puppet_resource_root: str) -> int:
    exit_code = 0
    applied_resource_root = os.path.join(puppet_resource_root, APPLIED_DIRECTORY)

    # Loop through everything in the puppet resource path
    for root, dirs, files in os.walk(puppet_resource_root):
        # modifying the 'dirs' variable in-place will update the order that os.walk visits things
        if APPLIED_DIRECTORY in dirs:
            dirs.remove(APPLIED_DIRECTORY)
        dirs.sort()  # Need to apply things in the correct order

        # Check to see if there's a difference between what Puppet created and
        # what's been previously applied
        for filename in sorted([f for f in files if f.endswith(".yaml")]):
            path = os.path.join(root, filename)
            applied_path = os.path.join(
                applied_resource_root, os.path.relpath(path, puppet_resource_root)
            )
            print(f"comparing {path} and {applied_path}")
            if not os.path.exists(applied_path) or not cmp(
                path, applied_path, shallow=False
            ):
                # This is idempotent; if something gets out of sync and a resource gets applied
                # a second time, kubectl just won't make any changes
                try:
                    run(["kubectl", "apply", "-f", path], check=True)
                    os.makedirs(os.path.dirname(applied_path), exist_ok=True)
                    copy(path, applied_path)
                except CalledProcessError:
                    print(f"There was a problem applying {path}:\n")
                    print_exc(
                        file=sys.stdout
                    )  # keep all messages on the same stream so they're in order
                    exit_code = 1
                    continue

    # Loop through all the files that have been previously applied and see
    # if Puppet has removed any of them
    for root, dirs, files in os.walk(applied_resource_root):
        dirs.sort(reverse=True)  # for deleting things, we need to go in reverse order
        for filename in sorted([f for f in files if f.endswith(".yaml")], reverse=True):
            path = os.path.join(root, filename)
            puppet_path = os.path.join(
                puppet_resource_root, os.path.relpath(path, applied_resource_root)
            )
            if not os.path.exists(puppet_path):
                print(f"Deleting resource {path}...")
                try:
                    run(
                        ["kubectl", "delete", "--ignore-not-found=true", "-f", path],
                        check=True,
                    )
                    os.remove(path)
                except CalledProcessError:
                    print(f"There was a problem deleting {path}:\n")
                    print_exc(
                        file=sys.stdout
                    )  # keep all messages on the same stream so they're in order
                    exit_code = 1
                    continue

    return exit_code


if __name__ == "__main__":
    sys.exit(main(sys.argv[1]))
