#!/bin/bash

#..... so aparently we can't run paasta tools at all unless
# this dir exists
mkdir -p /nail/etc/services

mkdir -p docs/man/
. .tox/manpages/bin/activate

VERSION=`./paasta_tools/paasta_cli/paasta_cli.py version`

function build_man() {
    COMMAND=$1
    echo "paasta $COMMAND --help"
    help2man --name=$COMMAND --version-string=$VERSION "./paasta_tools/paasta_cli/paasta_cli.py $COMMAND" > docs/man/paasta-$COMMAND.1
}

for FILE in paasta_tools/paasta_cli/cmds/*.py
do
    BASE=`basename $FILE`
    COMMAND=`echo "${BASE%.*}" | tr '_' '-'`
    if [[ $COMMAND == '--init--' ]]; then
        continue
    fi
    if [[ $COMMAND == 'start-stop-restart' ]]; then
        continue
    fi
    build_man $COMMAND
done

# Start / stop / restart are munged in one file
for COMMAND in start stop restart; do
    build_man $COMMAND
done

# And then finally the "main" paasta command
help2man --name='paasta' --version-string=$VERSION "./paasta_tools/paasta_cli/paasta_cli.py" > docs/man/paasta.1
