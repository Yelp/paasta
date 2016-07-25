#!/bin/bash
#socat tcp-listen:5000,reuseaddr,fork tcp:registry:5000 &
ssh-keyscan mesosmaster >> /root/.ssh/known_hosts
ssh-keyscan git >> /root/.ssh/known_hosts
if [ ! -f /var/tmp/pip_cache/built_wheels ]; then
    pip wheel . --wheel-dir=/var/tmp/pip_cache
    touch /var/tmp/pip_cache/built_wheels
fi
pip install --no-index --find-links=/var/tmp/pip_cache -e .
# This is a hack because we're not creating a real package which would create symlinks for the .py scripts
while read link; do echo $link|sed -e 's/usr\/share\/python\/paasta-tools\//\/usr\/local\//'| sed -e 's/\ usr/\ \/usr/'| xargs ln -s; done < /work/debian/paasta-tools.links
/bin/bash
