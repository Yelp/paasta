#!/bin/bash

set -e

count=1

host="("
shift
cmd="$@"

until [ $(curl -o /dev/null -s -w %{http_code} http://127.0.0.1:${PAASTA_API_PORT}/v1/version) = 200 ]; do
      >&2 echo "PaaSTA API is unavailable."
        sleep 1
        count=$((${count}+1))
        if [ ${count} -gt 9 ]; then
            echo "Timeout after trying ${count} times"
            exit 1
        fi
    done

    >&2 echo "PaaSTA API is up."
    exec $cmd
")"
