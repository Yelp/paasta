import cProfile, pstats
import json
import sys
import time
from contextlib import contextmanager

from paasta_tools.api import api
from paasta_tools.fake_zipkin import fake_zipkin
from paasta_tools.instance import kubernetes as pik


def main():
    api.setup_paasta_api()

    with fake_zipkin("everything"):
        instance_status = pik.instance_status(
            service=sys.argv[1],
            instance=sys.argv[2],
            verbose=0,
            include_smartstack=True,
            include_envoy=True,
            use_new=True,
            instance_type="kubernetes",
            settings=api.settings,
        )


if __name__ == '__main__':
    # profiler = cProfile.Profile()
    # profiler.enable()
    main()
    # profiler.disable()
    # profiler.dump_stats('status_cprofile')
