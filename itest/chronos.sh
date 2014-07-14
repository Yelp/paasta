#!/bin/bash

if dpkg -i /work/dist/*.deb; then
  echo "Looks like it installed correctly"
else
  echo "Dpkg install failed"
  exit 1
fi


/usr/share/python/service-deployment-tools/bin/setup_chronos_jobs.py --chronos-dir=/chronos-config --soa-dir=/yelpsoa-configs --ecosystem=testecosystem

/usr/bin/chronos-sync.rb --uri http://chronos:8080 --config /chronos-config
curl -L -X GET chronos:8080/scheduler/jobs
