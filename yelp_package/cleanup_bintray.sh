#!/bin/bash
BINTRAY_REPO='paasta'
BINTRAY_PACKAGE='paasta-tools'
VERSIONS_TO_KEEP=10

if [ -z $BINTRAY_USER ] || [ -z $BINTRAY_API_KEY ]; then
  echo "Please export BINTRAY_USER and BINTRAY_API_KEY"
  exit 1
fi

cleanOldNightlyVersions() {
    URL="https://api.bintray.com/packages/yelp/$BINTRAY_REPO/$BINTRAY_PACKAGE"
    versions=($(curl -X GET -H "Content-Type: application/json" -u$BINTRAY_USER:$BINTRAY_API_KEY $URL | jq -r '.versions'))
    for v in ${versions[@]:$VERSIONS_TO_KEEP}; do
        version=$(echo $v | sed -e 's/,//' -e 's/"//g')
        if [ $version !=  "]" ]; then
            echo "Deleting version: $version"
            DELETE_URL="$URL/versions/$version"
            RESPONSE_CODE=$(curl -X DELETE -H "Content-Type: application/json" -u$BINTRAY_USER:$BINTRAY_API_KEY $DELETE_URL -s -w "%{http_code}" -o /dev/null);
            if [[ "${RESPONSE_CODE:0:2}" != "20" ]]; then
                echo "Unable to delete version : $v, HTTP response code: $RESPONSE_CODE"
                exit 1
            fi
            echo "HTTP response code: $RESPONSE_CODE"
        fi
    done;
}

cleanOldNightlyVersions
