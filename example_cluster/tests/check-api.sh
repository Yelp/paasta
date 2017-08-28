#!/bin/bash
test_url (){
    STATUSCODE=$(curl --silent --output /dev/stderr --write-out "%{http_code}" $1)
    if test $STATUSCODE -ne 200; then
        exit 1
    fi
}

test_url paasta_api:5054/v1/version
test_url paasta_api:5054/v1/services/hello-world
test_url paasta_api:5054/v1/services/hello-world
STATUS=$(test_url paasta_api:5054/v1/services/hello-world/main/status 2>&1 | jq -r '.["marathon"]["deploy_status"]')
if [ "$STATUS" != "Running" ]; then
    echo "Status should be Running not $STATUS"
    exit 1
fi
