#!/bin/bash
pip install ./extra-linux-requirements.txt
pip install -e .
# This is a hack because we're not creating a real package which would create symlinks for the .py scripts
while read link; do echo $link|sed -e 's/opt\/venvs\/paasta-tools\//\/usr\/local\//'| sed -e 's/\ usr/\ \/usr/'| xargs ln -s; done < /work/debian/paasta-tools.links
/bin/bash
