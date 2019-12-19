import subprocess
from subprocess import PIPE


def cmd(args, capture_output=True):
    if capture_output:
        ret = subprocess.run(args.split(" "), stdout=PIPE, stderr=PIPE)
        ret.stdout = ret.stdout.decode("utf-8")
        ret.stderr = ret.stderr.decode("utf-8")
        return ret
    else:
        return subprocess.run(args.split(" "))
