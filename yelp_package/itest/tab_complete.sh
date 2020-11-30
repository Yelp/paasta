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

set -e

tab_complete_pass() {
    pre_typed="$1"
    echo "Tab completion with '$pre_typed' returned what we expected"
}
tab_complete_fail() {
    pre_typed="$1"
    actual="$2"
    expected="$3"
    echo "Tab completion for '$pre_typed' did not return what we expected"
    echo "Actual:"
    echo $actual
    echo "Expected:"
    echo $expected
    exit 1
}

# The expected output of tab completion is the words we should complete to
# separated by the vertical tab char
# This test will need to be modified if we add any new subcommands that start with
# with the provided pre_typed:
pre_typed='st'
expected=`echo -e "start\vstatus\vstop"`
# We feed the special env variables available at tab completion time
# to the paasta command to make it return back the tab completion output to
# fd 8, which we redirect to 1 so we can capture it
# See https://github.com/kislyuk/argcomplete#debugging
actual=`COMP_LINE="paasta $pre_typed" COMP_POINT=99 _ARGCOMPLETE=1 paasta 8>&1 9>/dev/null`
if [[ $expected == $actual ]]; then
    tab_complete_pass "$pre_typed"
else
    tab_complete_fail "$pre_typed" "$actual" "$expected"
fi

# Test tab completion in zsh
zsh_actual=$(zsh -c "COMP_LINE='paasta $pre_typed' COMP_POINT=99 _ARGCOMPLETE=1 paasta 8>&1 9>/dev/null")
if [[ $expected == $zsh_actual ]]; then
    tab_complete_pass "$pre_typed"
else
    tab_complete_fail "$pre_typed" "$zsh_actual" "$expected"
fi

# laziness test
#
# Our tab completion code uses lazy_choices_completer to defer collection of
# expensive tab-complete options, e.g. scraping yelpsoa-configs. Make sure we
# are being sufficiently lazy.
num_opens_in_nail_etc_services=`strace -e open paasta 2>&1 | grep '/nail/etc/services'  | wc -l`
if [[ $num_opens_in_nail_etc_services == 0 ]]; then
    echo "laziness test ok."
else
    echo "laziness test failure."
    echo "Running 'paasta' caused $num_opens_in_nail_etc_services file opens in /nail/etc/services"
    exit 1
fi


echo "Everything worked!"
