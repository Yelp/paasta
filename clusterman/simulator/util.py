import arrow
from staticconf.testing import PatchConfiguration


def patch_join_delay(mean=0, stdev=0):
    return PatchConfiguration({
        'join_delay_mean_seconds': mean,
        'join_delay_stdev_seconds': stdev,
    })


class SimulationMetadata:  # pragma: no cover
    def __init__(self, name, cluster, pool, scheduler):
        self.name = name
        self.cluster = cluster
        self.pool = pool
        self.scheduler = scheduler
        self.sim_start = None
        self.sim_end = None

    def __enter__(self):
        self.sim_start = arrow.now()

    def __exit__(self, type, value, traceback):
        self.sim_end = arrow.now()

    def __str__(self):
        return f'({self.cluster}, {self.pool}, {self.sim_start}, {self.sim_end})'
