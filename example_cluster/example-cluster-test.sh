#!/bin/bash
set -e

try_and_retry() {
   n=0
   until [ $n -ge 5 ]
   do
      echo "Running $1"
      $1 && echo "$1 PASSED" && return # substitute your command here
      n=$[$n+1]
      echo "$1 FAILED, sleeping 15 and retrying"
      sleep 15
   done
   exit 1
}

echo "exit" | /start.sh
cd /tmp
git clone root@git:dockercloud-hello-world
cd dockercloud-hello-world
/work/example_cluster/tests/start-new-service.sh
try_and_retry /work/example_cluster/tests/check-metastatus.sh
try_and_retry /work/example_cluster/tests/check-status.sh
try_and_retry /work/example_cluster/tests/check-api.sh
paasta stop -s hello-world -c testcluster -i main
paasta stop -s hello-world -c testcluster -i remote
sleep 30
