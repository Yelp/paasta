#!/bin/bash

if dpkg -i /work/dist/*.deb; then
  echo "Looks like it installed correctly"
else
  echo "Dpkg install failed"
  exit 1
fi


/usr/share/python/service-deployment-tools/bin/setup_chronos_jobs.py > /config.json

/usr/bin/chronos-sync.rb --uri http://chronos:8080 --config /config.json
