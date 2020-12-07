from typing import Optional
from typing import Tuple

from paasta_tools.util.const import SPACER


def long_job_id_to_short_job_id(long_job_id: str) -> str:
    service, instance, _, __ = decompose_job_id(long_job_id)
    return compose_job_id(service, instance)


class InvalidJobNameError(Exception):
    pass


def compose_job_id(
    name: str,
    instance: str,
    git_hash: Optional[str] = None,
    config_hash: Optional[str] = None,
    spacer: str = SPACER,
) -> str:
    """Compose a job/app id by concatenating its name, instance, git hash, and config hash.

    :param name: The name of the service
    :param instance: The instance of the service
    :param git_hash: The git_hash portion of the job_id. If git_hash is set,
                     config_hash must also be set.
    :param config_hash: The config_hash portion of the job_id. If config_hash
                        is set, git_hash must also be set.
    :returns: <name><SPACER><instance> if no tag, or <name><SPACER><instance><SPACER><hashes>...
              if extra hash inputs are provided.

    """
    composed = f"{name}{spacer}{instance}"
    if git_hash and config_hash:
        composed = f"{composed}{spacer}{git_hash}{spacer}{config_hash}"
    elif git_hash or config_hash:
        raise InvalidJobNameError(
            "invalid job id because git_hash (%s) and config_hash (%s) must "
            "both be defined or neither can be defined" % (git_hash, config_hash)
        )
    return composed


def decompose_job_id(job_id: str, spacer: str = SPACER) -> Tuple[str, str, str, str]:
    """Break a composed job id into its constituent (service name, instance,
    git hash, config hash) by splitting with ``spacer``.

    :param job_id: The composed id of the job/app
    :returns: A tuple (service name, instance, git hash, config hash) that
        comprise the job_id
    """
    decomposed = job_id.split(spacer)
    if len(decomposed) == 2:
        git_hash = None
        config_hash = None
    elif len(decomposed) == 4:
        git_hash = decomposed[2]
        config_hash = decomposed[3]
    else:
        raise InvalidJobNameError("invalid job id %s" % job_id)
    return (decomposed[0], decomposed[1], git_hash, config_hash)
