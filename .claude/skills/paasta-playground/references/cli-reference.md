# CLI Commands in the Playground

All commands require:
```bash
export PAASTA_SYSTEM_CONFIG_DIR=./etc_paasta_playground/
export KUBECONFIG=./k8s_itests/kubeconfig
```

Invoke pattern:
```bash
.tox/py310-linux/bin/python -m paasta_tools.cli.cli <command> [flags]
```

## Command Reference

### status

Shows deployment state, pod health, ReplicaSets, and autoscaling info.

```bash
.tox/py310-linux/bin/python -m paasta_tools.cli.cli status \
  -s <service> -c kind-$USER-k8s-test -d ./soa_config_playground/

# Verbose (per-pod detail):
  -v

# Extra verbose (container stdout/stderr tail):
  -vv
```

**Requires:** API running (`make playground-api`)

**Output includes:** Version (desired), State (Running/Bouncing), Kubernetes health
(Healthy with N/N instances), ReplicaSet details, pod-level errors, autoscaling status.

---

### mark-for-deployment

Marks a git SHA for deployment to a deploy group. In the playground, this writes a
git tag but the version-match logic and wait-for-deployment polling are the primary
test targets.

```bash
.tox/py310-linux/bin/python -m paasta_tools.cli.cli mark-for-deployment \
  -s <service> \
  -l <deploy-group> \
  -c <40-char-sha> \
  -d ./soa_config_playground/
```

**Important:** `-c` here means **commit**, not cluster. This differs from most other
PaaSTA commands where `-c` means cluster.

**Flags:**
| Flag | Purpose |
|------|---------|
| `-s, --service` | Service name (required) |
| `-l, --deploy-group` | Deploy group to mark (required) |
| `-c, -k, --commit` | Git SHA to deploy (required) |
| `--wait-for-deployment` | Poll bounce_status until deploy completes or times out |
| `-t, --timeout <seconds>` | How long to wait (default: 1800) |
| `--auto-rollback` | Rollback if timeout is reached |
| `--polling-interval` | Seconds between bounce_status checks |
| `--diagnosis-interval` | Seconds between deploy diagnosis runs |

**Exit codes:**
- `0` — deploy successful (or version already healthy)
- `1` — deploy failed or instances unhealthy

**Note:** The same-version re-run path checks if the target SHA matches what's already
deployed. If it does, it checks instance health and returns 0 (healthy) or 1 (unhealthy).

---

### wait-for-deployment

Standalone version of the `--wait-for-deployment` polling loop. Useful for testing
the polling/diagnosis logic independently.

```bash
.tox/py310-linux/bin/python -m paasta_tools.cli.cli wait-for-deployment \
  -s <service> \
  -l <deploy-group> \
  -c <commit> \
  -d ./soa_config_playground/
```

**Flags:**
| Flag | Purpose |
|------|---------|
| `-t <seconds>` | Timeout (default: 1800) |
| `--polling-interval <seconds>` | How often to check bounce_status |
| `--diagnosis-interval <seconds>` | How often to run deploy diagnosis |
| `--time-before-first-diagnosis <seconds>` | Wait before first diagnosis |

**Requires:** API running

---

### get-latest-deployment

Returns the currently deployed git SHA for a deploy group.

```bash
.tox/py310-linux/bin/python -m paasta_tools.cli.cli get-latest-deployment \
  -s <service> -l <deploy-group> -d ./soa_config_playground/
```

**Output:** 40-character git SHA (or empty if not deployed)

---

### info

Shows service metadata from soa-configs (no API call required).

```bash
.tox/py310-linux/bin/python -m paasta_tools.cli.cli info \
  -s <service> -d ./soa_config_playground/
```

**Output:** Service name, description, owner, runbook, git repo, clusters deployed to.

---

### get-docker-image

Returns the docker image URL for a deploy group.

```bash
.tox/py310-linux/bin/python -m paasta_tools.cli.cli get-docker-image \
  -s <service> -i <instance> -l <deploy-group> -d ./soa_config_playground/
```

**Note:** Uses `-l` for deploy group — does NOT take `-c` for cluster.

---

### list-clusters

Lists clusters configured in the playground.

```bash
.tox/py310-linux/bin/python -m paasta_tools.cli.cli list-clusters \
  -d ./soa_config_playground/
```

**Output:** `kind-$USER-k8s-test`

---

### validate

Validates service config files against JSON schemas. Catches config errors before deploying.

```bash
.tox/py310-linux/bin/python -m paasta_tools.cli.cli validate \
  -s <service> -y ./soa_config_playground/
```

**Important:** Uses `-y` for soa_dir, NOT `-d`.

---

### autoscale

View or override autoscaling settings.

```bash
# View current state:
.tox/py310-linux/bin/python -m paasta_tools.cli.cli autoscale \
  -s <service> -i <instance> -c kind-$USER-k8s-test -d ./soa_config_playground/

# Set instance count (bypass autoscaler):
  --set <N>

# Set temporary floor:
  --set-min <N> --for 3h
```

**Requires:** API running

---

## Direct Module Invocation (Operator Modules)

These bypass the CLI and run PaaSTA backend modules directly. In production, these run
periodically on each cluster. In the playground, you invoke them manually to trigger
specific reconciliation steps.

### setup_kubernetes_job

Creates/updates Kubernetes Deployment objects from config. This is the primary
"deploy" action — it reads `deployments.json` + kubernetes config and creates or
updates the K8s Deployment object with the target image and settings.

```bash
.tox/py310-linux/bin/python -m paasta_tools.setup_kubernetes_job \
  -d ./soa_config_playground -c kind-$USER-k8s-test \
  <service>.<instance>
```

Multiple service.instance pairs can be passed as positional args (at least one is
required).

**Flags:**
| Flag | Purpose |
|------|---------|
| `-d, --soa-dir` | soa_config directory |
| `-c, --cluster` | Cluster name |
| `-v, --verbose` | Debug logging |

---

### cleanup_kubernetes_jobs

Compares running Deployments/StatefulSets against what's configured in soa_dir and
`deep_delete`s anything that shouldn't exist. This is the counterpart to
`setup_kubernetes_job` — together they form the full reconciliation loop.

```bash
.tox/py310-linux/bin/python -m paasta_tools.cleanup_kubernetes_jobs \
  -d ./soa_config_playground -c kind-$USER-k8s-test --force
```

`--force` skips the kill-threshold safety check (needed in playground since we have
few services and the ratio math would otherwise block deletion of >50% of apps).

---

### setup_kubernetes_crd

Creates/updates Custom Resource Definitions (CRDs) for a service. CRDs are the
schema definitions that allow custom resources (Flink jobs, Cassandra clusters, etc.)
to exist in the cluster.

```bash
.tox/py310-linux/bin/python -m paasta_tools.setup_kubernetes_crd \
  -d ./soa_config_playground -c kind-$USER-k8s-test \
  <service>
```

**Flags:**
| Flag | Purpose |
|------|---------|
| `-d, --soa-dir` | soa_config directory |
| `-c, --cluster` | Cluster name |
| `-v, --verbose` | Debug logging |
| positional | Service name(s) to set up CRDs for |

---

### setup_kubernetes_cr

Creates/updates Custom Resources (CRs) — the actual instances of custom resource
types (Flink jobs, Cassandra clusters, etc.). Reads CRD definitions from
system_paasta_config and applies configured CRs to the cluster.

```bash
.tox/py310-linux/bin/python -m paasta_tools.setup_kubernetes_cr \
  -d ./soa_config_playground -c kind-$USER-k8s-test
```

**Flags:**
| Flag | Purpose |
|------|---------|
| `-d, --soa-dir` | soa_config directory |
| `-c, --cluster` | Cluster name |
| `-s, --service` | Limit to a specific service |
| `-i, --instance` | Limit to a specific instance |
| `-D, --dry-run` | Print K8s manifests to stdout without applying |
| `-v, --verbose` | Debug logging |

---

### setup_kubernetes_internal_crd

Creates/updates PaaSTA's own internal CRDs (not service-specific). These are the
CRD schemas that PaaSTA itself needs to operate (e.g., autoscaling state, deploy
locks).

```bash
.tox/py310-linux/bin/python -m paasta_tools.setup_kubernetes_internal_crd -v
```

No flags required beyond `-v` — reads from the PaaSTA package itself.

---

### generate_deployments_for_service

Reads git deploy tags and materializes `deployments.json`. This maps deploy groups
to docker image + git SHA — the source of truth for what version should be running.

```bash
.tox/py310-linux/bin/python -m paasta_tools.generate_deployments_for_service \
  -s <service> -d ./soa_config_playground/
```

**Warning:** This overwrites any manual edits to `deployments.json`.

---

## Secrets

### paasta secret

Manages encrypted service secrets. In the playground, the `run` subcommand is most
useful — it decrypts secrets and runs a command with them as environment variables.

```bash
# Run a command with decrypted secrets:
.tox/py310-linux/bin/python -m paasta_tools.cli.cli secret run \
  -s <service> -i <instance> -c kind-$USER-k8s-test \
  -d ./soa_config_playground/ \
  -- env

# Add a new secret:
.tox/py310-linux/bin/python -m paasta_tools.cli.cli secret add \
  -s <service> -c kind-$USER-k8s-test \
  -d ./soa_config_playground/

# Decrypt and view a secret:
.tox/py310-linux/bin/python -m paasta_tools.cli.cli secret decrypt \
  -s <service> -c kind-$USER-k8s-test \
  -d ./soa_config_playground/
```

**Requires interactive terminal:** The `add` and `update` subcommands prompt for the
secret value via stdin. Run these with `! <command>` in Claude Code, or in a separate
terminal.

### paasta-secrets-sync (Makefile target)

Syncs encrypted secrets from Vault into Kubernetes Secrets for all services in the
playground. This is needed if your service reads secrets from environment variables.

```bash
make paasta-secrets-sync
```

**Requires interactive terminal (first run):** Generates a `.vault-token` via
`vault login -method=ldap` which prompts for credentials. The token is cached in
`.vault-token` and reused on subsequent runs until it expires.

The target depends on `setup-kubernetes-job` (ensures the cluster has workloads to
sync secrets to).

---

## Kind Cluster Management

### Creating the cluster

```bash
make k8s_fake_cluster
```

**Requires interactive terminal:** The cluster creation process calls
`set-paasta-registry-credentials.sh` which authenticates against the docker registry.
This triggers a browser-based login flow that requires keyboard interaction.
Run this with `! make k8s_fake_cluster` in Claude Code, or in a separate terminal.

### Cluster sizing

The default cluster config (`k8s_itests/deployments/kind/cluster-devbox.yaml`)
creates 1 control-plane + 6 workers. Kind nodes are lightweight containers (podman),
so the resource cost is minimal.

If pods are stuck in `Pending` due to resource pressure, check scheduling:
```bash
KUBECONFIG=./k8s_itests/kubeconfig kubectl describe pod <pod> -n paastasvc-<service> | grep -A5 Events
```

### Scaling the cluster (adding/removing nodes)

Kind does not support adding nodes to a running cluster. To change node count:

1. Edit `k8s_itests/deployments/kind/cluster-devbox.yaml` — add or remove `role: worker`
   entries
2. Recreate the cluster:
   ```bash
   make k8s_clean && make k8s_fake_cluster
   ```
3. Re-deploy workloads:
   ```bash
   make generate_deployments_for_service && make setup-kubernetes-job
   ```

### Registry credentials on new clusters

Kind nodes need credentials to pull images from the internal docker registry.
The `make k8s_fake_cluster` target handles this automatically by running
`k8s_itests/scripts/set-paasta-registry-credentials.sh`.

If you recreate the cluster or images fail to pull with "Back-off pulling image",
re-run the credentials script:

```bash
k8s_itests/scripts/set-paasta-registry-credentials.sh $USER-k8s-test
```

**Requires interactive terminal:** Uses docker credential helpers which trigger
browser-based auth. Run with `! <command>` or in a separate terminal.

The script iterates over all Kind nodes and:
1. Generates a registry auth token via docker credential helpers
2. Writes containerd registry host config to each node
3. Restarts containerd and kubelet on each node

### Destroying the cluster

```bash
make k8s_clean
```

This deletes the Kind cluster entirely. You'll need `make k8s_fake_cluster` to
recreate it.

---

## Commands That Don't Work in the Playground

| Command | Why |
|---------|-----|
| `list-deploy-queue` | Calls API endpoint `/deploy_queue` whose handler was removed with deployd (defunct) |
| `logs` | Reads from production log infrastructure (scribereader/vector-logs) |
| `local-run` | Builds/pulls docker images from registry (not a playground use case) |
| `rollback` | Creates git tags on remote; playground has no git server |
| `start` / `stop` | Push git tags to remote; use config edits + setup_kubernetes_job instead |
| `restart` | Same as start/stop — relies on git tag infrastructure |
| `mesh-status` | Requires SmartStack/Envoy proxy (not in Kind cluster) |

---

## Flag Inconsistencies

Some commands use different flags for the same concept:

| Concept | Most commands | Exception |
|---------|--------------|-----------|
| soa_dir | `-d` | `validate` uses `-y` |
| cluster | `-c` means cluster | `mark-for-deployment` and `wait-for-deployment`: `-c` means **commit** |
| instance | `-i` | `mark-for-deployment` doesn't take instance (operates on deploy group) |
