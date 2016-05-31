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


diff_yaml_files() {
    FILE_RELATIVE_PATH=$1
    BASE_DIR_EXPECTED=$2
    BASE_DIR_ACTUAL=$3

    diff <(sort $BASE_DIR_EXPECTED/$FILE_RELATIVE_PATH) <(tail -n +10 $BASE_DIR_ACTUAL/$FILE_RELATIVE_PATH | sort)
}

if dpkg -i /work/dist/*.deb; then
  echo "Looks like it installed correctly: OK"
else
  echo "Dpkg install failed: FAIL"
  exit 1
fi


/usr/share/python/paasta-tools/bin/setup_chronos_job.py --chronos-dir=/chronos-config --soa-dir=/yelpsoa-configs --ecosystem=testecosystem

/usr/bin/chronos-sync.rb --uri http://chronos:8080 --config /chronos-config
mkdir /running-chronos-config
/usr/bin/chronos-sync.rb --uri http://chronos:8080 --config /running-chronos-config --update-from-chronos

# These tests are actually pretty brittle: there *is* some diff
# between the running and generated configs, because the
# running ones don't seem to include some of our fields.
# Also, if CPUs is a number that's impossible to perfectly
# represent as a float, the test will fail, because python
# and chronos have different ideas about float representation.
# Right now, the test data is 'cleverly' chosen to sidestep this
# but beware.
TEST_JOB_1_DIFF=`diff_yaml_files 'scheduled/test_job_1.yaml' '/chronos-config' '/running-chronos-config'`
TEST_JOB_1_FAILED=`echo $TEST_JOB_1_DIFF | grep \>`
if [ "${TEST_JOB_1_FAILED}" ]; then
    echo "Generated configuration for test_job_1 wasn't the same as running configuration: FAIL"
    echo $TEST_JOB_1_DIFF
    exit 1
else
    echo "generated test_job_1 matched running job_1: OK"
fi

TEST_JOB_2_DIFF=`diff_yaml_files 'scheduled/test_job_2.yaml' '/chronos-config' '/running-chronos-config'`
TEST_JOB_2_FAILED=`echo $TEST_JOB_2_DIFF | grep \>`
if [ "${TEST_JOB_2_FAILED}" ]; then
    echo "Generated configuration for test_job_2 wasn't the same as running configuration: FAIL"
    echo $TEST_JOB_2_DIFF
    exit 1
else
    echo "generated test_job_2 matched running job_2: OK"
fi
