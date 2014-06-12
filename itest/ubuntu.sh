#!/bin/bash

cd /

if dpkg -i /work/dist/*.deb; then
  echo "Looks like it installed correctly"
else
  echo "Dpkg install failed"
  exit 1
fi

if setup_marathon_job -h; then
  echo "Looks like it ran ok!"
else
  echo "setup_marathon_job returned $?"
  exit 1
fi

