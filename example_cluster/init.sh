#!/bin/sh

cd /tmp
git clone root@git:dockercloud-hello-world
cd dockercloud-hello-world
paasta itest -s hello-world -c `git rev-parse HEAD`
paasta push-to-registry --force -s hello-world -c `git rev-parse HEAD`
paasta mark-for-deployment \
  --git-url root@git:dockercloud-hello-world \
  --commit `git rev-parse HEAD` \
  --clusterinstance testcluster.everything \
  --service hello-world
