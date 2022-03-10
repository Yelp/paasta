#!/bin/bash
set -uo pipefail

diff_names=$(git diff origin/master --name-only)
schema_touched=$(echo "$diff_names" | grep 'paasta_tools/api/api_docs/oapi.yaml')
api_touched=$(echo "$diff_names" | grep 'paasta_tools/paastaapi')

# this case will only be hit as part of `make test` or someone running pre-commit on
# all files manually since we've chosen to only run this hook on changes to the schema
# file
if [ ! -z "$api_touched" -a -z "$schema_touched" ]; then
    echo "paasta_tools/paastaapi must not be modified manually" >&2
    echo "Please revert your changes:" >&2
    echo -e "$api_touched" >&2
    exit 1
fi

make openapi-codegen &>/dev/null
diff=$(git diff --name-only | grep paasta_tools/paastaapi)
if [ ! -z "$diff" ]; then
    echo "paasta_tools/paastaapi codegen has a diff, either commit the changes or fix oapi.yaml:" >&2
    echo -e "$diff" >&2
    exit 1
fi
