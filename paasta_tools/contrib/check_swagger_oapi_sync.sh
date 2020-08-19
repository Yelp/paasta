#!/bin/bash
set -euo pipefail

diff_names=$(git diff HEAD~ --name-only)
touched_schemas=$(
    echo "$diff_names" |
    grep 'paasta_tools/api/api_docs/\(swagger.json\|oapi.yaml\)' |
    wc -l)

if [ "$touched_schemas" = "1" ]; then
    echo "Please keep oapi.yaml and swagger.json in sync!" >&2
    exit 1
elif [ "$touched_schemas" = "0" ]; then
    if [ ! -z "$(echo "$diff_names" | grep paasta_tools/paastaapi)" ]; then
        echo "paasta_tools/paastaapi must not be modified manually"
        exit 1
    fi
    exit 0
fi

make openapi-codegen
diff=$(git diff --name-only | grep paasta_tools/paastaapi)
if [ ! -z "$diff" ]; then
    echo "paasta_tools/paastaapi codegen has a diff, either commit the changes or fix oapi.yaml:"
    echo $diff
    exit 1
fi
