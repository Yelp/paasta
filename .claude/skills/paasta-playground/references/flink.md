# Running Flink Jobs in the Playground

The playground supports running real Flink jobs using the flink-operator.
This is useful for validating Flink service configs before deploying to production.

## Quick Start

From a clean state (no cluster, no configs):

```bash
make setup-flink
```

This single command sets up the Flink infrastructure:

1. Generates playground configs from templates
2. Creates the Kind cluster (with correct volume mounts)
3. Deploys flink-operator via `setup_kubernetes_job`
4. Registers the Flink CRD via `setup_kubernetes_crd`
5. Creates RBAC for the operator ServiceAccount
6. Creates Flink CRs for any flink services in `fake_soa_config/`

After this, the operator is running and ready. To run actual Flink jobs,
add a real service (see "Adding Real Flink Services for Testing").

Verify the operator is running:

```bash
KUBECONFIG=./k8s_itests/kubeconfig kubectl get pods -n paasta
```

## How Flink Differs from Regular Services

| Aspect               | Regular service             | Flink service                                        |
|----------------------|-----------------------------|------------------------------------------------------|
| Config file          | `kubernetes-<cluster>.yaml` | `flink-<cluster>.yaml`                               |
| Deployer             | `setup_kubernetes_job`      | `setup_kubernetes_cr`                                |
| K8s resource created | Deployment                  | Custom Resource (kind: `Flink`)                      |
| Namespace            | `paastasvc-<service>`       | `paasta-flinks`                                      |
| Who creates pods     | K8s directly                | Flink operator watches CR → creates JM/TM/Supervisor |
| Node pool            | `default`                   | `flink-spot` (must label nodes)                      |

## Production Flow vs Playground

| Production                                      | Playground                                      |
|-------------------------------------------------|-------------------------------------------------|
| setup_kubernetes_crd (reads crd-<cluster>.yaml) | setup_kubernetes_crd (reads crd-<cluster>.yaml) |
| flux deploys operator RBAC                      | kubectl apply -f flink-operator-rbac.yaml       |
| setup_kubernetes_job deploys operator           | setup_kubernetes_job deploys operator           |
| setup_kubernetes_cr creates flink CRs           | setup_kubernetes_cr creates flink CRs           |
| Operator reconciles → JM/TM pods                | Same — real operator runs in Kind               |

## Makefile Targets

| Target | When to use |
|--------|-------------|
| `make setup-flink` | First-time setup (cluster + operator + CRD + RBAC + CRs) |
| `make setup-kubernetes-cr` | Reconcile CRs after adding/changing flink service configs |
| `make generate_deployments_for_service` | Refresh `deployments.json` from git tags |

`make setup-flink` dependency chain:

```
make setup-flink
  ├── setup-kubernetes-job
  │     ├── k8s_fake_cluster (creates Kind with mounts)
  │     │     └── | etc_paasta_playground soa_config_playground (configs first)
  │     └── generate_deployments_for_service
  └── setup_kubernetes_crd + RBAC + setup_kubernetes_cr
```

## Kind Cluster Volume Mounts

The Kind cluster config (`k8s_itests/deployments/kind/cluster-devbox.yaml`) mounts
these from the devbox onto each worker node:

| Host path                          | Container path       | Purpose                                                            |
|------------------------------------|----------------------|--------------------------------------------------------------------|
| `/nail/etc`                        | `/nail/etc`          | ecosystem file, srv-configs                                        |
| `<checkout>/etc_paasta_playground` | `/etc/paasta`        | PaaSTA system config (docker registry, clusters, custom_resources) |
| `<checkout>/soa_config_playground` | `/nail/etc/services` | yelpsoa-configs (flink configs, deployments.json)                  |

`<checkout>` is the paasta repo root, substituted by `sed` at cluster creation time
(uses `PAASTA_CHECKOUT_DIR` placeholder in the YAML).

**Important**: The playground configs must exist BEFORE the cluster is created.
The Makefile dependency `k8s_fake_cluster: | etc_paasta_playground soa_config_playground`
ensures this. If you see empty mounts, recreate the cluster.

The flink-operator pod mounts these from the node into its container via `extra_volumes`:

```yaml
extra_volumes:
  - { containerPath: /nail/etc, hostPath: /nail/etc, mode: RO }
  - { containerPath: /etc/paasta, hostPath: /etc/paasta, mode: RO }
  - { containerPath: /nail/etc/services, hostPath: /nail/etc/services, mode: RO }
```

## Node Pool Labeling

The flink-operator sets `nodeSelector: {"yelp.com/pool": "flink-spot"}` on Flink pods.
Kind nodes default to `yelp.com/pool: default`, so you need to label some nodes:

```bash
KUBECONFIG=./k8s_itests/kubeconfig kubectl label node <worker-node> yelp.com/pool=flink-spot --overwrite
```

Label at least 2 nodes to allow scheduling of both JM and TM pods.

## Template Files

All live in `k8s_itests/deployments/paasta/fake_soa_config/` and use `<%cluster%>`
placeholder (rendered to `kind-$USER-k8s-test`):

### flink-operator/

- `kubernetes-<%cluster%>.yaml` — operator pod config (namespace: paasta, volumes, env)
- `crd-<%cluster%>.yaml` — Flink CRD definition (registered by `setup_kubernetes_crd`)
- `service.yaml` — service description

The operator's `deployments.json` is generated by `generate_deployments_for_service`
which reads deploy tags from `git@github.yelpcorp.com:services/flink-operator`.

### System config (fake_etc_paasta/)

- `custom_resources.json` — tells `setup_kubernetes_cr` about the flink CRD
- `stubs.json` — stub values for `dashboard_links` and `cr_owners` (required by `setup_kubernetes_cr`)

## Adding Real Flink Services for Testing

To test a real Flink service:

1. Create the service directory in `k8s_itests/deployments/paasta/fake_soa_config/<service>/`:
   ```
   flink-<%cluster%>.yaml   — copy/adapt from /nail/etc/services/<service>/flink-*.yaml
   service.yaml             — description (git_url is auto-resolved from service name)
   ```

2. `generate_deployments_for_service` will fetch deploy tags from
   `git@github.yelpcorp.com:services/<service>` and produce `deployments.json`
   automatically. No need to create it manually.

3. Regenerate and deploy:
   ```bash
   .tox/py310-linux/bin/python paasta_tools/contrib/create_paasta_playground.py
   make generate_deployments_for_service
   make setup-kubernetes-cr
   ```

4. The operator will create JM/TM/Supervisor pods using the real Flink image.

Use `make setup-kubernetes-cr` to reconcile CRs without re-running the full
`setup-flink` (which redeploys the operator, CRD, and RBAC). This is the
fast path when iterating on flink service configs.

**Note**: The operator reads `authenticating.yaml` from the yelpsoa-configs root.
`create_paasta_playground.py` runs `generate_authenticating_services` to produce it.

## RBAC

The flink-operator needs a ServiceAccount + ClusterRole to manage resources. In
production this is managed by Puppet (compute-infra-k8s manifests). PaaSTA has no
cronjob for this — per the code: "we're expecting that any Role dynamically associated
with a Service Account already exists."

For the playground, `k8s_itests/flink/` provides:

- ServiceAccount `flink-operator` in namespace `paasta`
- ClusterRoleBinding to `cluster-admin` (local Kind cluster — no security boundary)

Applied by `make setup-flink` automatically.

## Gotchas

| Issue                                                       | Cause                                                       | Fix                                                                                              |
|-------------------------------------------------------------|-------------------------------------------------------------|--------------------------------------------------------------------------------------------------|
| `CRD Flink not found`                                       | CRD not registered, or `singular` casing wrong              | Ensure `"singular": "Flink"` (capital F) in `custom_resources.json`; run `make setup-flink`      |
| `serviceaccount "flink-operator" not found`                 | RBAC not applied                                            | `make setup-flink` handles this; or `kubectl apply -f k8s_itests/flink/` |
| Operator pod `ErrImagePull`                                 | Registry creds expired                                      | Re-run `! k8s_itests/scripts/set-paasta-registry-credentials.sh $USER-k8s-test`                  |
| Operator can't find `/etc/paasta`                           | Configs generated after cluster creation (mount is empty)   | `make k8s_clean && make k8s_fake_cluster` (configs must exist first)                             |
| Operator can't find `deployments.json` for a service        | Service not in `soa_config_playground/`                     | Add service to `fake_soa_config/` templates and regenerate                                       |
| `open /nail/etc/services/authenticating.yaml: no such file` | Missing global yelpsoa-configs file                         | Regenerate with `create_paasta_playground.py` (runs `generate_authenticating_services`)           |
| Flink pods stuck `Pending` — node affinity                  | Nodes don't have `yelp.com/pool: flink-spot`                | Label nodes: `kubectl label node <node> yelp.com/pool=flink-spot`                                |
| `setup_kubernetes_cr` uses wrong soa_dir                    | Was hardcoded to `DEFAULT_SOA_DIR`                          | Fixed; pass `-d` flag to use playground soa_dir                                                  |
| `'dashboard_links'` KeyError                                | Missing system config file                                  | Already in `fake_etc_paasta/stubs.json`                                                          |

## Architecture

```
┌──────────────────────────────────────────────────────────────────────┐
│  Kind Cluster                                                        │
│                                                                      │
│  namespace: paasta                                                   │
│    └── flink-operator-main pod                                       │
│         - mounts /nail/etc, /etc/paasta, /nail/etc/services          │
│         - watches flinks.yelp.com CRs in paasta-flinks               │
│                                                                      │
│  namespace: paasta-flinks                                            │
│    └── per flink service:                                            │
│         ├── <service>-<sha>-jobmanager (1 pod)                       │
│         ├── <service>-<sha>-taskmanager (N pods)                     │
│         └── <service>-<sha>-supervisor (1 job)                       │
│                                                                      │
│  Node labels:                                                        │
│    worker1-4: yelp.com/pool=default (regular services)               │
│    worker5-6: yelp.com/pool=flink-spot (flink pods)                  │
└──────────────────────────────────────────────────────────────────────┘
```
