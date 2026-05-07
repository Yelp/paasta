#!/bin/bash
# Checks the current state of the PaaSTA playground and reports what's ready vs what needs setup.
# Usage: .claude/skills/paasta-playground/scripts/playground-status.sh

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
KUBECONFIG="${REPO_ROOT}/k8s_itests/kubeconfig"
CLUSTER="kind-${USER}-k8s-test"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
NC='\033[0m'

ok() { echo -e "  ${GREEN}✓${NC} $1"; }
fail() { echo -e "  ${RED}✗${NC} $1"; }
warn() { echo -e "  ${YELLOW}!${NC} $1"; }

echo "PaaSTA Playground Status"
echo "========================"
echo ""

# Check tox env
echo "Build environment:"
if [ -d "${REPO_ROOT}/.tox/py310-linux" ]; then
    ok "tox virtualenv exists (.tox/py310-linux/)"
else
    fail "tox virtualenv missing — run: make dev"
fi
echo ""

# Check Kind cluster
echo "Kind cluster (${CLUSTER}):"
if KUBECONFIG="${KUBECONFIG}" kubectl cluster-info --context "${CLUSTER}" &>/dev/null 2>&1; then
    ok "cluster is running"
    NODE_COUNT=$(KUBECONFIG="${KUBECONFIG}" kubectl get nodes --no-headers 2>/dev/null | wc -l)
    ok "${NODE_COUNT} node(s) available"
else
    fail "cluster not running — run: make k8s_fake_cluster"
fi
echo ""

# Check playground configs
echo "Playground configs:"
if [ -d "${REPO_ROOT}/etc_paasta_playground" ]; then
    ok "etc_paasta_playground/ exists"
    if [ -f "${REPO_ROOT}/etc_paasta_playground/api_endpoints.json" ]; then
        API_URL=$(python3 -c "import json; print(json.load(open('${REPO_ROOT}/etc_paasta_playground/api_endpoints.json'))['api_endpoints'].get('${CLUSTER}', 'not configured'))" 2>/dev/null || echo "parse error")
        ok "api_endpoints.json → ${API_URL}"
    fi
else
    fail "etc_paasta_playground/ missing — run: make generate_deployments_for_service"
fi

if [ -d "${REPO_ROOT}/soa_config_playground" ]; then
    ok "soa_config_playground/ exists"
    if [ -f "${REPO_ROOT}/soa_config_playground/compute-infra-test-service/deployments.json" ]; then
        GIT_SHA=$(python3 -c "import json; d=json.load(open('${REPO_ROOT}/soa_config_playground/compute-infra-test-service/deployments.json')); print(d.get('v2',{}).get('deployments',{}).get('prod.main',{}).get('git_sha','unknown'))" 2>/dev/null || echo "parse error")
        ok "deployments.json present (prod.main sha: ${GIT_SHA:0:8})"
    else
        fail "deployments.json missing — run: make generate_deployments_for_service"
    fi
else
    fail "soa_config_playground/ missing — run: make generate_deployments_for_service"
fi
echo ""

# Check pods
echo "Workloads:"
if KUBECONFIG="${KUBECONFIG}" kubectl get ns paastasvc-compute-infra-test-service &>/dev/null 2>&1; then
    POD_STATUS=$(KUBECONFIG="${KUBECONFIG}" kubectl get pods -n paastasvc-compute-infra-test-service --no-headers 2>/dev/null)
    RUNNING=$(echo "$POD_STATUS" | grep -c "Running" || echo "0")
    TOTAL=$(echo "$POD_STATUS" | wc -l)
    if [ "$RUNNING" -gt 0 ]; then
        ok "${RUNNING}/${TOTAL} pod(s) Running in paastasvc-compute-infra-test-service"
    else
        warn "0 running pods (${TOTAL} total) — run: make setup-kubernetes-job"
    fi
else
    fail "namespace 'paastasvc-compute-infra-test-service' not found — run: make setup-kubernetes-job"
fi
echo ""

# Check API
echo "PaaSTA API:"
if [ -f "${REPO_ROOT}/etc_paasta_playground/api_endpoints.json" ]; then
    API_URL=$(python3 -c "import json; print(json.load(open('${REPO_ROOT}/etc_paasta_playground/api_endpoints.json'))['api_endpoints']['${CLUSTER}'])" 2>/dev/null)
    if [ -n "$API_URL" ] && curl -s --max-time 2 "${API_URL}/v1/version" &>/dev/null; then
        ok "API responding at ${API_URL}"
    else
        fail "API not responding at ${API_URL:-unknown} — run: make playground-api (in another terminal)"
    fi
else
    fail "api_endpoints.json missing — run: make generate_deployments_for_service first"
fi
echo ""

# Check zookeeper
echo "Zookeeper:"
ZK_CONTAINER="${USER}-paasta-zookeeper"
if docker ps --format '{{.Names}}' 2>/dev/null | grep -q "^${ZK_CONTAINER}$"; then
    ok "container '${ZK_CONTAINER}' running"
else
    warn "container '${ZK_CONTAINER}' not running — will be started by make generate_deployments_for_service"
fi
echo ""
