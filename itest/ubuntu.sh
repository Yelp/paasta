#!/bin/bash

cd /

if dpkg -i /work/dist/*.deb; then
  echo "Looks like it installed correctly"
else
  echo "Dpkg install failed"
  exit 1
fi

if fab_repo; then
  echo "Looks like it ran ok!"
else
  echo "fab_repo returned $?"
  exit 1
fi

