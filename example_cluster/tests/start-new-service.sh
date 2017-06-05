#!/bin/bash
paasta itest -s hello-world -c `git rev-parse HEAD`
paasta push-to-registry -s hello-world -c `git rev-parse HEAD` --force
paasta mark-for-deployment --git-url root@git:dockercloud-hello-world --commit `git rev-parse HEAD` --clusterinstance testcluster.everything --service hello-world
