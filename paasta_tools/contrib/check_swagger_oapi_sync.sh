#!/bin/bash
set -euo pipefail

touched=$(git diff HEAD~ --name-only |
            grep 'paasta_tools/api/api_docs/\(swagger.json\|oapi.yaml\)' |
            wc -l)
if [ "$touched" = "1" ]; then
    echo "Please keep oapi.yaml and swagger.json in sync!" >&2
    exit 1
fi
