#!/bin/bash
# Prints environment variables needed to run PaaSTA CLI commands against the playground.
# Usage: source <(.claude/skills/paasta-playground/scripts/playground-env.sh)
#    or: eval $(.claude/skills/paasta-playground/scripts/playground-env.sh)

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"

echo "export PAASTA_SYSTEM_CONFIG_DIR=${REPO_ROOT}/etc_paasta_playground/"
echo "export KUBECONFIG=${REPO_ROOT}/k8s_itests/kubeconfig"
echo "export PAASTA_TEST_CLUSTER=kind-${USER}-k8s-test"

if [ -f "${REPO_ROOT}/etc_paasta_playground/api_endpoints.json" ]; then
    API_PORT=$(python3 -c "import json; print(json.load(open('${REPO_ROOT}/etc_paasta_playground/api_endpoints.json'))['api_endpoints']['kind-${USER}-k8s-test'].split(':')[-1])" 2>/dev/null)
    if [ -n "$API_PORT" ]; then
        echo "# API endpoint: http://localhost:${API_PORT}"
    fi
fi

echo "# Run CLI: .tox/py310-linux/bin/python -m paasta_tools.cli.cli <cmd> -s compute-infra-test-service -c kind-${USER}-k8s-test -d ${REPO_ROOT}/soa_config_playground/"
