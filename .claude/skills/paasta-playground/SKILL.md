---
name: paasta-playground
description: >-
  How to test PaaSTA code changes against a local Kind Kubernetes cluster using the
  playground environment. The playground bridges unit tests and production.
when_to_use: >-
  Testing CLI commands locally, verifying API changes end-to-end, debugging PaaSTA
  behavior against real pods, running mark-for-deployment/status/logs locally, or the
  user mentions "playground", "kind cluster", "local testing", or "test against real k8s".
  Also use when the user wants to validate code changes beyond unit tests.
---

# PaaSTA Playground Testing

The playground is a local environment that runs PaaSTA against a real Kind Kubernetes
cluster on the devbox. It lets you test CLI commands, API endpoints, and deployment
logic against actual running pods — catching issues that unit tests with mocks miss.

## If the Playground Is Not Set Up

Run `.claude/skills/paasta-playground/scripts/playground-status.sh` to check what's
ready vs what's missing. If the cluster or configs don't exist:

1. **Cluster missing** — the user must run `! make k8s_fake_cluster` themselves (the
   `!` prefix runs the command in the current session). This requires interactive
   keyboard input for browser-based authentication to get docker registry credentials.
   You cannot run this non-interactively.

2. **tox virtualenv missing** — `make dev` can be run non-interactively, but takes
   several minutes. Run it in background or ask the user to run `! make dev`.

3. **Configs missing** (`etc_paasta_playground/`, `soa_config_playground/`) — run
   `make generate_deployments_for_service` (non-interactive, safe to run directly).

4. **Workloads not deployed** — run `make setup-kubernetes-job` (non-interactive).

5. **API not running** — start with `make playground-api` in a separate terminal, or
   ask the user to run it. The API must stay running for CLI commands that call it.

6. **Registry credentials expired** (pods stuck in `ImagePullBackOff` or
   `Back-off pulling image`) — credentials rotate and eventually expire. The user must
   re-run `! k8s_itests/scripts/set-paasta-registry-credentials.sh $USER-k8s-test`
   (requires interactive auth). The script is idempotent — safe to re-run anytime.
   It overwrites the existing credentials on all nodes and restarts containerd/kubelet.

## Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│  Kind Cluster (kind-$USER-k8s-test)                              │
│    └── namespace: paastasvc-<service>  (one per service)         │
│         └── <service>-<instance> pods                            │
├──────────────────────────────────────────────────────────────────┤
│  PaaSTA API (localhost:<dynamic-port>)                            │
│    reads: soa_config_playground/, etc_paasta_playground/          │
│    talks to: Kind cluster via k8s_itests/kubeconfig              │
├──────────────────────────────────────────────────────────────────┤
│  PaaSTA CLI                                                      │
│    env: PAASTA_SYSTEM_CONFIG_DIR=./etc_paasta_playground/         │
│    flag: -d ./soa_config_playground/                              │
│    talks to: PaaSTA API (endpoint from api_endpoints.json)       │
└──────────────────────────────────────────────────────────────────┘
```

The playground can run any PaaSTA service — not just the default
`compute-infra-test-service`. Add a directory under `soa_config_playground/` with the
standard PaaSTA config files and it will be picked up by `setup_kubernetes_job`.

**Key directories:**
- `etc_paasta_playground/` — system config: `api_endpoints.json` (API URL per cluster),
  `clusters.json`, `volumes.json`, `docker_registry.json`
- `soa_config_playground/` — service configs: `deploy.yaml`, `kubernetes-*.yaml`,
  `deployments.json`, `service.yaml`
- `k8s_itests/kubeconfig` — kubeconfig for the Kind cluster

**Namespace convention:** PaaSTA namespaces are `paastasvc-<service>`. Underscores in
service names become double hyphens (e.g., `my_service` → `paastasvc-my--service`).

**API port:** Determined dynamically by `pick_random_port("paasta-dev-api")` — a
deterministic hash per user (range 33000-58000). The port is written to
`etc_paasta_playground/api_endpoints.json` on API startup. Check it there if in doubt.

## Prerequisites

```bash
make dev                # builds .tox/py310-linux virtualenv (one-time)
make k8s_fake_cluster   # creates the Kind cluster (one-time, persists across sessions)
```

**Interactive terminal required:** `make k8s_fake_cluster` triggers browser-based
authentication to get registry credentials for pulling images. Run it in a terminal
where you can interact with the auth flow (`! make k8s_fake_cluster` in Claude Code).

See `references/cli-reference.md` → "Kind Cluster Management" for cluster sizing,
scaling, and registry credential details.

Verify the cluster exists:
```bash
KUBECONFIG=./k8s_itests/kubeconfig kubectl get nodes
```

## Setup Workflow

Run these in order. Each step depends on the previous:

### 1. Generate playground configs

```bash
make generate_deployments_for_service
```

This does three things:
1. Runs `create_paasta_playground.py` which starts a local zookeeper container and
   creates `etc_paasta_playground/` and `soa_config_playground/` from templates in
   `k8s_itests/deployments/paasta/`
2. Runs `generate_deployments_for_service` which reads git deploy tags for the test
   service and materializes them into `soa_config_playground/<service>/deployments.json`

The `deployments.json` file maps deploy groups to docker image + git SHA — this is
what `setup_kubernetes_job` reads to know which version to deploy.

### 2. Deploy workloads to the cluster

```bash
make setup-kubernetes-job
```

Reads `soa_config_playground/` and `deployments.json`, then creates/updates the
Kubernetes Deployment objects for all services configured in the playground.

**Important:** `setup_kubernetes_job` only creates or updates Kubernetes Deployments.
It does NOT remove stale ones. If you:
- Remove an instance from config
- Rename an instance
- Remove a service entirely

You must also run cleanup to reconcile:

```bash
make cleanup-kubernetes-jobs
```

`cleanup_kubernetes_jobs` lists all PaaSTA-managed Deployments/StatefulSets in the
cluster, compares them against what's defined in `soa_config_playground/`, and
`deep_delete`s anything that shouldn't exist. The Makefile passes `--force` to skip
the kill-threshold safety check.

**Full deploy cycle (mimics production reconciliation):**
```bash
make setup-kubernetes-job && make cleanup-kubernetes-jobs
```

Verify pods are running:
```bash
KUBECONFIG=./k8s_itests/kubeconfig kubectl get pods -n paastasvc-<service>
```

### 3. Start the PaaSTA API

```bash
make playground-api
```

Runs the API via `tox -e playground-api`. Leave this running in a separate terminal/tmux.
The API reads from `soa_config_playground/` (via `PAASTA_API_SOA_DIR` env var in tox.ini)
and talks to the Kind cluster.

Wait for the line: `[INFO] Booting worker with pid: ...` before testing CLI commands.

The API serves the same endpoints as production — `bounce_status`, `instance/status`,
etc. This is what `--wait-for-deployment` polls to determine if a bounce is complete.

## Running CLI Commands Against the Playground

See `references/cli-reference.md` for the full flag table, which commands work in the
playground, and known flag inconsistencies between commands.

Every CLI command needs these environment variables and flags:

```bash
export PAASTA_SYSTEM_CONFIG_DIR=./etc_paasta_playground/
export KUBECONFIG=./k8s_itests/kubeconfig

# Then run any CLI command with -d for soa_dir:
.tox/py310-linux/bin/python -m paasta_tools.cli.cli <command> \
  -s <service> \
  -c kind-$USER-k8s-test \
  -d ./soa_config_playground/
```

Always use `python -m` to invoke — never run scripts directly. The local
`paasta_tools/kubernetes/` package shadows the pip `kubernetes` package if you
use direct script invocation.

> **kubectl namespaces:** When running `kubectl` commands, translate service names to
> namespace format: `paastasvc-<service>` with underscores replaced by double hyphens.
> Example: `my_service` → `-n paastasvc-my--service`


### Common playground commands

**Status** (requires API running):
```bash
.tox/py310-linux/bin/python -m paasta_tools.cli.cli status \
  -s <service> \
  -c kind-$USER-k8s-test \
  -d ./soa_config_playground/
```

Expected output shows: Version (desired), State (Running/Bouncing), Kubernetes health
(Healthy with N/N instances), ReplicaSet details.

**Mark-for-deployment:**
```bash
.tox/py310-linux/bin/python -m paasta_tools.cli.cli mark-for-deployment \
  --service <service> \
  --deploy-group <deploy-group> \
  --commit <sha> \
  -d ./soa_config_playground/
```

**Run PaaSTA modules directly** (setup_kubernetes_job, cleanup, etc.):
```bash
.tox/py310-linux/bin/python -m paasta_tools.setup_kubernetes_job \
  -d ./soa_config_playground -c kind-$USER-k8s-test \
  <service>.<instance>
```

## Quick-Start Script

Source the helper to set environment variables:
```bash
source <(.claude/skills/paasta-playground/scripts/playground-env.sh)
```

Check what's running vs what needs setup:
```bash
.claude/skills/paasta-playground/scripts/playground-status.sh
```

## How the Playground Maps to Production

In production, a PaaSTA deploy flows through these stages:

1. **CI pipeline** runs `mark-for-deployment` which creates a git tag
   (`paasta-$deploy_group-$date-deploy`) and triggers deployment processing
2. **`generate_deployments_for_service`** reads all git tags for the service and
   materializes them into `deployments.json`
3. **`setup_kubernetes_job`** (runs periodically on each cluster) reads `deployments.json`
   + kubernetes config, creates/updates the K8s Deployment object
4. **Kubernetes controllers** create a new ReplicaSet, schedule pods, and (based on
   bounce strategy) drain old pods once new ones are healthy. The default `crossover`
   strategy maps to K8s `RollingUpdate` — new pods must be ready before old ones are removed
5. **`cleanup_kubernetes_jobs`** (runs periodically) compares running apps against config
   and `deep_delete`s any Deployment/StatefulSet that shouldn't exist
6. **`--wait-for-deployment`** (if set in `deploy.yaml` or CLI flag) polls the API's
   `bounce_status` endpoint checking: only the target version is running, deploy status
   is Running/Deploying/Waiting, and replicas meet the bounce margin

The playground lets you trigger each stage independently:

| Production | Playground |
|---|---|
| `mark-for-deployment` → `generate_deployments_for_service` | Edit `deployments.json` directly, or `make generate_deployments_for_service` |
| `setup_kubernetes_job` (periodic) creates/updates K8s Deployment | `make setup-kubernetes-job` (manual) |
| `cleanup_kubernetes_jobs` (periodic) removes stale K8s Deployments | `make cleanup-kubernetes-jobs` (manual) |
| K8s controllers handle the bounce (RollingUpdate/Recreate) | Same — Kind cluster runs real controllers |
| PaaSTA API serves bounce_status, instance status | `make playground-api` |
| CLI reads system config from `PAASTA_SYSTEM_CONFIG_DIR` | `PAASTA_SYSTEM_CONFIG_DIR=./etc_paasta_playground/` |
| soa-configs from `DEFAULT_SOA_DIR` | `-d ./soa_config_playground/` |
| `wait_for_deployment: true` in `deploy.yaml` blocks pipeline | `--wait-for-deployment` CLI flag |

The key advantage: in production these run continuously and automatically. In the
playground you run them manually, which lets you observe intermediate states and test
specific code paths in isolation.

## Testing Scenarios

Examples below use `compute-infra-test-service` (the default playground service) but
the same patterns apply to any service you configure.

### Testing mark-for-deployment (same-version re-run)

This exercises the early-exit logic when a pipeline re-runs with the same commit:

```bash
# Check current deployed version
jq '.v2.deployments["prod.main"]' soa_config_playground/compute-infra-test-service/deployments.json

# Run m-f-d with the SAME sha — triggers version-match detection
.tox/py310-linux/bin/python -m paasta_tools.cli.cli mark-for-deployment \
  --service compute-infra-test-service \
  --deploy-group prod.main \
  --commit $(jq -r '.v2.deployments["prod.main"].git_sha' soa_config_playground/compute-infra-test-service/deployments.json) \
  -d ./soa_config_playground/

# Exit code 0 = instances healthy, safe to proceed
# Exit code 1 = instances unhealthy, blocks pipeline
echo "Exit code: $?"
```

### Testing mark-for-deployment with --wait-for-deployment

This exercises the polling loop that checks `bounce_status`:

```bash
# API must be running for this to work
.tox/py310-linux/bin/python -m paasta_tools.cli.cli mark-for-deployment \
  --service compute-infra-test-service \
  --deploy-group prod.main \
  --commit $(jq -r '.v2.deployments["prod.main"].git_sha' soa_config_playground/compute-infra-test-service/deployments.json) \
  --wait-for-deployment \
  -d ./soa_config_playground/
```

### Simulating a full deploy (new version)

**Important:** `make setup-kubernetes-job` depends on `generate_deployments_for_service`,
which regenerates `deployments.json` from git tags — overwriting any manual edits.
If you've manually edited `deployments.json`, run `setup_kubernetes_job` directly:

```bash
# 1. Edit deployments.json — change git_sha and docker_image tag
#    This simulates what generate_deployments_for_service produces after m-f-d
vim soa_config_playground/compute-infra-test-service/deployments.json

# 2. Run setup_kubernetes_job DIRECTLY (bypasses Make dependency that would overwrite your edit)
export KUBECONFIG=./k8s_itests/kubeconfig
export PAASTA_SYSTEM_CONFIG_DIR=./etc_paasta_playground/
.tox/py310-linux/bin/python -m paasta_tools.setup_kubernetes_job \
  -d ./soa_config_playground -c kind-$USER-k8s-test \
  compute-infra-test-service.autoscaling

# 3. Run cleanup directly
.tox/py310-linux/bin/python -m paasta_tools.cleanup_kubernetes_jobs \
  -d ./soa_config_playground -c kind-$USER-k8s-test --force

# 4. Check status (same endpoint wait-for-deployment polls)
.tox/py310-linux/bin/python -m paasta_tools.cli.cli status \
  -s compute-infra-test-service -c kind-$USER-k8s-test \
  -d ./soa_config_playground/
```

**When to use `make` vs direct invocation:**
- `make setup-kubernetes-job` — safe when you haven't manually edited `deployments.json`
  (it regenerates from git tags first, then deploys)
- Direct `python -m paasta_tools.setup_kubernetes_job` — use when you've manually
  edited `deployments.json` or `kubernetes-*.yaml` and don't want it overwritten

### Simulating a config-only change (yelpsoa-configs update)

When kubernetes-*.yaml changes without a new docker image (scaling, resources, env):

```bash
# 1. Edit the config
vim soa_config_playground/compute-infra-test-service/kubernetes-kind-$USER-k8s-test.yaml

# 2. Run setup + cleanup directly (avoids generate_deployments overwriting deployments.json)
export KUBECONFIG=./k8s_itests/kubeconfig
export PAASTA_SYSTEM_CONFIG_DIR=./etc_paasta_playground/
.tox/py310-linux/bin/python -m paasta_tools.setup_kubernetes_job \
  -d ./soa_config_playground -c kind-$USER-k8s-test \
  compute-infra-test-service.autoscaling
.tox/py310-linux/bin/python -m paasta_tools.cleanup_kubernetes_jobs \
  -d ./soa_config_playground -c kind-$USER-k8s-test --force

# 3. Watch the bounce happen (K8s RollingUpdate by default)
KUBECONFIG=./k8s_itests/kubeconfig kubectl get pods -n paastasvc-compute-infra-test-service -w
```

### Testing API endpoints directly

The API routes use `{service}/{instance}` — no cluster in the path (each cluster has
its own API instance):

```bash
# Get the API URL from config
API_URL=$(jq -r '.api_endpoints["kind-'$USER'-k8s-test"]' etc_paasta_playground/api_endpoints.json)

# bounce_status — what wait-for-deployment polls
curl -s "$API_URL/v1/services/<service>/<instance>/bounce_status" | python -m json.tool

# instance status
curl -s "$API_URL/v1/services/<service>/<instance>/status" | python -m json.tool

# API version
curl -s "$API_URL/v1/version"
```

## Known Gotchas

| Issue | Cause | Fix |
|-------|-------|-----|
| `ImportError: cannot import name 'client' from 'kubernetes'` | Local `paasta_tools/kubernetes/` shadows pip `kubernetes` package | Always use `python -m` invocation, never direct script paths |
| API returns 404 for bounce_status | API not reading playground soa_dir | Ensure `PAASTA_API_SOA_DIR=./soa_config_playground` is set (handled by `tox -e playground-api`) |
| `get_currently_deployed_version` returns None in playground | The call at line 489 of `mark_for_deployment.py` doesn't pass `soa_dir` — reads from `DEFAULT_SOA_DIR` | Known pre-existing issue; for testing the version-match path, patch the function to pass `soa_dir` |
| Pods stuck in Pending | Kind cluster lacks resources or not running | `make k8s_fake_cluster` to recreate; check `kubectl describe pod` for scheduling errors |
| `Back-off pulling image` / `ImagePullBackOff` | Registry credentials expired (they rotate) | Re-run `k8s_itests/scripts/set-paasta-registry-credentials.sh $USER-k8s-test` (interactive auth required). Script is idempotent. |
| API connection refused | API not started or crashed | Run `make playground-api` and wait for `[INFO] Booting worker` |
| `No such cluster` errors | Cluster name mismatch | Must be `kind-$USER-k8s-test` (check `etc_paasta_playground/clusters.json`) |
| Manual edits to `deployments.json` lost | `make setup-kubernetes-job` depends on `generate_deployments_for_service` which regenerates from git tags | Run `setup_kubernetes_job` directly via `python -m` instead of `make` |
| Old pods still running after removing an instance | `setup_kubernetes_job` only creates/updates, never deletes | Run `cleanup_kubernetes_jobs` directly to remove stale Deployments |
| `cleanup_kubernetes_jobs` refuses to kill | Kill threshold exceeded (>50% of apps would be deleted) | The Makefile passes `--force`; if running manually, add `--force` |
| Zookeeper container not running | `create_paasta_playground.py` starts it but it may stop | `docker ps \| grep zookeeper`; re-run `make generate_deployments_for_service` |
| Bounce strategy confusion | `crossover` = K8s `RollingUpdate` (new ready before old removed); `downthenup` = K8s `Recreate` (kill all old first); `brutal` = `RollingUpdate` with maxUnavailable=100% | Check `bounce_method` in kubernetes config; default is `crossover` |

## Adding Custom Services to the Playground

The playground isn't limited to `compute-infra-test-service`. To run any service:

```bash
# 1. Create the service config directory
mkdir -p soa_config_playground/my-service

# 2. Add required config files:
#    - kubernetes-kind-$USER-k8s-test.yaml (instance definitions)
#    - deploy.yaml (pipeline config, can be minimal)
#    - deployments.json (version mapping)

# Example minimal kubernetes config:
cat > soa_config_playground/my-service/kubernetes-kind-$USER-k8s-test.yaml << 'EOF'
main:
  cpus: 0.1
  mem: 128
  instances: 2
  deploy_group: prod.main
EOF

cat > soa_config_playground/my-service/deploy.yaml << 'EOF'
---
pipeline:
- step: prod.main
EOF

# 3. Create deployments.json pointing to a valid docker image:
cat > soa_config_playground/my-service/deployments.json << EOF
{"v2": {"deployments": {"prod.main": {"docker_image": "services-my-service:paasta-<sha>", "git_sha": "<sha>", "image_version": null}}, "controls": {"my-service:kind-$USER-k8s-test.main": {"desired_state": "start", "force_bounce": null}}}}
EOF

# 4. Deploy it
export KUBECONFIG=./k8s_itests/kubeconfig
export PAASTA_SYSTEM_CONFIG_DIR=./etc_paasta_playground/
.tox/py310-linux/bin/python -m paasta_tools.setup_kubernetes_job \
  -d ./soa_config_playground -c kind-$USER-k8s-test \
  my-service.main
```

The docker image must be pullable from the Kind cluster. For testing PaaSTA logic
(not the service itself), you can reuse the existing test image from
`compute-infra-test-service`'s `deployments.json`.

## Cleanup

```bash
make clean-playground    # removes etc_paasta_playground/ and soa_config_playground/
make k8s_clean           # deletes the Kind cluster entirely
```
