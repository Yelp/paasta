import json

import boto3
import staticconf
from clusterman_metrics.util.constants import CONFIG_NAMESPACE


_metrics_session = None


def _setup_session():
    with open(staticconf.read_string('access_key_file', namespace=CONFIG_NAMESPACE)) as boto_cfg_file:
        boto_cfg = json.load(boto_cfg_file)
        _session = boto3.session.Session(
            aws_access_key_id=boto_cfg['accessKeyId'],
            aws_secret_access_key=boto_cfg['secretAccessKey'],
        )
    return _session


def get_metrics_session():
    global _metrics_session

    if not _metrics_session:
        _metrics_session = _setup_session()

    return _metrics_session
