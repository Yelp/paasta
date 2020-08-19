#!/bin/bash
set -euo pipefail

touched=$(git diff HEAD~ --name-only |
            grep 'paasta_tools/api/api_docs/\(swagger.json\|oapi.yaml\)' |
            wc -l)
if [ "$touched" = "1" ]; then
    echo "Please keep oapi.yaml and swagger.json in sync!" >&2
    exit 1
fi

if [ "$touched" = "0" ]; then
    exit 0
fi

make openapi-codegen
diff=$(git diff --name-only)
if [ ! -z "$diff" ]; then
    echo "paasta_tools/paastaapi codegen has a diff, either commit the changes or fix oapi.yaml:"
    echo $diff
    exit 1
fi
