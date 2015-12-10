#!/bin/bash
# Copyright 2015 Yelp Inc.
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


#..... so aparently we can't run paasta tools at all unless
# this dir exists
mkdir -p /nail/etc/services

mkdir -p docs/man/
. .tox/manpages/bin/activate

VERSION=`./paasta_tools/cli/cli.py --version 2>&1 | cut -f 2 -d ' '`

function build_man() {
    COMMAND=$1
    echo "paasta $COMMAND --help"
    help2man --name=$COMMAND --version-string=$VERSION "./paasta_tools/cli/cli.py $COMMAND" > docs/man/paasta-$COMMAND.1
}

for FILE in paasta_tools/cli/cmds/*.py
do
    BASE=`basename $FILE`
    COMMAND=`echo "${BASE%.*}" | tr '_' '-'`
    if [[ $COMMAND == '--init--' ]]; then
        continue
    fi
    if [[ $COMMAND == 'start-stop-restart' ]]; then
        continue
    fi
    build_man $COMMAND
done

# Start / stop / restart are munged in one file
for COMMAND in start stop restart; do
    build_man $COMMAND
done

# And then finally the "main" paasta command
help2man --name='paasta' --version-string=$VERSION "./paasta_tools/cli/cli.py" > docs/man/paasta.1
