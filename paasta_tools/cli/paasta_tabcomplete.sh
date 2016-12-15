#!/bin/bash
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

if [[ -n ${ZSH_VERSION-} ]]; then
	autoload -U +X bashcompinit && bashcompinit
fi

# This magic eval enables tab-completion for the "paasta" command
# http://argcomplete.readthedocs.io/en/latest/index.html#synopsis
# This comes from the paasta-tools system package
eval "$(/opt/venvs/paasta-tools/bin/register-python-argcomplete paasta)"
