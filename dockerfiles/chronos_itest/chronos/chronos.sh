#! /bin/bash
[ ! -f /etc/default/chronos ]    || . /etc/default/chronos

set -e

#exec 2> >( /nail/sys/bin/stdin2scribe ${LOGNAME:=chronos_log} ) 1>&2

JAVA_OPTS="$JAVA_OPTS -Djava.library.path=${JAVA_LIBPATH:-/usr/lib/} -cp /etc/chronos:/usr/lib/chronos/chronos.jar:/usr/lib/chronos/lib/*"

exec java $JAVA_OPTS com.airbnb.scheduler.Main "$@"
