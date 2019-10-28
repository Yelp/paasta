# Copyright 2019 Yelp Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
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
