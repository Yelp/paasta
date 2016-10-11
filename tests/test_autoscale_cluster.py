#!/usr/bin/env python
# Copyright 2015-2016 Yelp Inc.
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
import mock

from paasta_tools.autoscale_cluster import main


@mock.patch('paasta_tools.autoscale_cluster.logging', autospec=True)
@mock.patch('paasta_tools.autoscale_cluster.autoscale_local_cluster', autospec=True)
@mock.patch('paasta_tools.autoscale_cluster.parse_args', autospec=True)
def test_main(mock_parse_args, mock_autoscale_local_cluster, logging):
    mock_parse_args.return_value = mock.Mock(dry_run=True, autoscaler_configs='/nail/blah')
    main()
    mock_autoscale_local_cluster.assert_called_with(dry_run=True, config_folder='/nail/blah')
