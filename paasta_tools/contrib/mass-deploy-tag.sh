#!/bin/bash
# This script was created to help migrate from branches to tags for deployments.
# While it is probably useless as-is now, it can hopefully serve as a reference for making
# future bulk changes to services.
set -e

WORK_DIR=$(mktemp -d --tmpdir=/nail/tmp)
pushd ${WORK_DIR}

function cleanup {
	popd
	rm -rf "${WORK_DIR}"
}
trap cleanup EXIT

services=$(paasta list)
#services="example_service"


for service in ${services} ; do
	echo "Processing ${service}"
	jq_output=$(jq -r '.v1 | to_entries | .[] | .key + " " + .value.docker_image' /nail/etc/services/${service}/deployments.json)
	if [ -z "$jq_output" ] ; then
		echo "${service} has no deployments. Skipping."
		continue
	fi
	# git_repo=$(paasta info -s ${service} | grep -oP 'Git Repo: \K.*$')
	git_repo=$(script -qc "paasta info -s ${service}" | sed -r "s/\x1B\[([0-9]{1,2}(;[0-9]{1,2})?)?[m|K]//g" | grep 'Git Repo: ' | cut -d' ' -f3)
	default_git_repo=git@git.yelpcorp.com:services/${service}.git
	echo git clone ${git_repo-${default_git_repo}} ${service}
	git clone ${git_repo} ${service}
	unset git_repo
	cd ${service}
	while read -r deploy_group sha; do
		deploy_group=$(echo ${deploy_group} | sed 's/^.*paasta-//')
		sha=$(echo ${sha} | sed 's/^.*paasta-//')
		echo "Mapping ${deploy_group} => ${sha}"
		# echo paasta mark-for-deployment --git-url ${git_repo} --commit ${sha} --deploy-group ${deploy_group} --service ${service}
		timestamp='00000000T000000'
		git tag paasta-${deploy_group}-${timestamp}-deploy ${sha} || true
        done <<< "$jq_output"
	git push --tags origin master
	cd -
done
