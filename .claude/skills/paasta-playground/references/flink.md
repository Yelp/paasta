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
6. Creates Flink CRs for any flink services in `operators_soa_config/`

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
| Node pool            | `default`                   | `flink-spot` (workers 5-6 pre-labeled)               |

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
The Kind cluster config (`cluster-devbox.yaml`) pre-labels workers 5-6 with
`yelp.com/pool: flink-spot` at creation time, so no manual labeling is needed.

If you need more flink-spot nodes (e.g., for higher TM parallelism), label additional
workers manually:

```bash
KUBECONFIG=./k8s_itests/kubeconfig kubectl label node <worker-node> yelp.com/pool=flink-spot --overwrite
```

## Template Files

All live in `k8s_itests/deployments/paasta/operators_soa_config/` and use `<%cluster%>`
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

1. Create the service directory in `k8s_itests/deployments/paasta/operators_soa_config/<service>/`:
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
| Operator can't find `deployments.json` for a service        | Service not in `soa_config_playground/`                     | Add service to `operators_soa_config/` templates and regenerate                                       |
| `open /nail/etc/services/authenticating.yaml: no such file` | Missing global yelpsoa-configs file                         | Regenerate with `create_paasta_playground.py` (runs `generate_authenticating_services`)           |
| Flink pods stuck `Pending` — node affinity                  | Nodes don't have `yelp.com/pool: flink-spot`                | Workers 5-6 are pre-labeled; if more needed: `kubectl label node <node> yelp.com/pool=flink-spot` |
| Supervisor tries JAR instead of sql-client.sh               | Missing `app_type: streamhouse` in flink config             | Add `app_type: streamhouse` at instance level (see Streamhouse section below)                    |
| Supervisor pod `Evicted` — ephemeral storage                | SQL client writes logs/temp exceeding 512Mi disk limit      | Set `flink_supervisor.disk: 4096` in flink config                                                |
| Supervisor pod OOMKill (exit 137) quickly                   | SQL client + supervisor exceed default memory               | Set `flink_supervisor.mem: 4096` (sql-client JVM needs ~2Gi)                                     |
| `ExpiredTokenException` in supervisor logs                  | AWS session token in config expired                         | Re-run `aws-okta` and re-inject credentials (see AWS section below)                              |
| Acorn CrashLoopBackOff consuming resources                  | Default acorn service in playground has config issues       | Remove/rename `soa_config_playground/acorn` if not needed                                        |
| Cluster lost after devbox restart                           | Kind containers don't persist across devbox reboots         | Remove sentinels (`rm k8s_itests/.create_cluster .fake_cluster`) then `make setup-flink`         |
| `set-paasta-registry-credentials.sh` says "No kind nodes"  | Sentinel `.fake_cluster` exists but cluster doesn't         | `rm k8s_itests/.fake_cluster k8s_itests/.create_cluster` then recreate cluster                   |

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

## Running Streamhouse/SQLClient Jobs

Streamhouse (sqlclient) jobs require additional config compared to regular JAR-based
Flink jobs. The supervisor determines the submission method based on `app_type`.

### Required Config Fields

```yaml
# soa_config_playground/<service>/flink-kind-$USER-k8s-test.yaml
canary:
  app_type: streamhouse         # REQUIRED - tells supervisor to use sql-client.sh
  service_type: application     # How jobs are discovered (application vs connector)
  job_type: testing             # Uses TestingDB() instead of DynamoDB (data-streams role lacks PutItem)
  checkpoint_path: s3://yelp-streamhouse-playground-dev-us-west-2/paimon/$USER/flink-checkpoints/
  savepoint_path: s3://yelp-streamhouse-playground-dev-us-west-2/paimon/$USER/flink-savepoints/
  always_allow_non_restored_state: true
  flink_supervisor:
    cpus: 1
    disk: 4096                  # SQL client writes temp files/logs; 512Mi default is too small
    mem: 4096                   # SQL client JVM needs ~2Gi; total with supervisor needs ~4Gi
  flink_conf:
    execution.checkpointing.interval: "60000"           # 1 min for fast validation
    execution.checkpointing.timeout: "600000"
    execution.checkpointing.externalized-checkpoint-retention: "RETAIN_ON_CANCELLATION"
    state.backend.type: rocksdb
    s3.aws.credentials.provider: com.amazonaws.auth.EnvironmentVariableCredentialsProvider
    fs.s3a.aws.credentials.provider: com.amazonaws.auth.EnvironmentVariableCredentialsProvider
    fs.s3a.impl: org.apache.hadoop.fs.s3a.S3AFileSystem
  jobs:
    my_job:
      sql: "SELECT ..."         # SQL is mounted as ConfigMap at /etc/flink-jobs/<instance>/
```

### How `app_type` Works

The supervisor reads its config from `/nail/etc/services/<service>/flink-<cluster>.yaml`
(volume-mounted from `soa_config_playground/`). It checks `<instance>.app_type`:

| `app_type` value | Job class | Submission method |
|------------------|-----------|-------------------|
| (not set/`flink`) | `FlinkJob` | `flink run ... /flink/apps/<service>.jar` |
| `streamhouse` | `StreamhouseJob` | `sql-client.sh -f <sql-file>` |
| `beam` | `BeamJob` | Beam job server |
| `pyflink` | `PyFlinkJob` | Python Flink |

Without `app_type: streamhouse`, the supervisor tries to find a JAR that doesn't exist
in sqlclient images. This is the most common config mistake for streamhouse services.

### job_type for Playground

- **`testing`** (recommended): Uses `TestingDB()` instead of `DynamoSupervisorDB()`,
  bypassing all DynamoDB access. The `data-streams` role lacks `dynamodb:PutItem` on
  the `flink_job` table, so `testing` is required for the playground. Checkpoints
  still work (managed by Flink itself, not the supervisor's DynamoDB tracking).
- **`stateless`**: Supervisor still attempts DynamoDB writes for job tracking; will
  crash with `AccessDeniedException` if using `data-streams` role.
- **`stateful`**: Same DynamoDB issue as `stateless`, plus attempts savepoint restore.

### Supervisor Resource Requirements

The supervisor pod runs both the Python supervisor process AND the Java SQL client
process (which loads all Flink/Paimon/Kafka JARs). Minimum resources:

| Resource | Minimum for SQLClient | Default (too low) |
|----------|----------------------|-------------------|
| mem | 4096 Mi | 512 Mi |
| disk | 4096 Mi | 512 Mi |
| cpus | 0.5 | 0.1 |

If the supervisor pod gets `Evicted` (exit 137), check `kubectl describe pod` for
"ephemeral local storage usage exceeds" — this means disk limit is too low.

## AWS Credentials for Playground

The Flink supervisor needs AWS credentials for DynamoDB (savepoint state) and S3
(checkpoint/savepoint storage). Credentials are injected as env vars in the flink config.

### Refresh Credentials

```bash
# Run in a real terminal (needs Okta)
aws-okta -a dev -p dev -r data-streams

# Then inject into playground config
python3 -c "
import yaml, json, subprocess
result = subprocess.run(
    ['/usr/bin/aws-okta', '--drop-env', '-a', 'dev', '-r', 'data-streams', '-o', 'json', '--session-duration', '7200'],
    capture_output=True, text=True)
cred_json = json.loads(result.stdout)
creds = {
    'AWS_ACCESS_KEY_ID': cred_json['AccessKeyId'],
    'AWS_SECRET_ACCESS_KEY': cred_json['SecretAccessKey'],
    'AWS_SESSION_TOKEN': cred_json.get('SessionToken', ''),
    'AWS_DEFAULT_REGION': 'us-west-2',
    'IS_FLINKV2_ENABLED': 'true',
}
config_path = 'soa_config_playground/streamhouse_queries/flink-kind-\$USER-k8s-test.yaml'
with open(config_path) as f:
    config = yaml.safe_load(f)
instance = list(config.keys())[0]
config[instance]['env'] = creds
with open(config_path, 'w') as f:
    yaml.dump(config, f, default_flow_style=False)
print(f'Updated. Expires: {cred_json.get(\"Expiration\", \"unknown\")}')
"
```

Then reconcile: `make setup-kubernetes-cr`

**Note**: `aws-okta` tokens expire after ~2 hours.

### S3 Access -- Use the Playground Bucket

The `data-streams` role has **read+write** access to:
- `s3://yelp-streamhouse-playground-dev-us-west-2/` -- **USE THIS** for checkpoints, savepoints, and Paimon warehouse

The `data-streams` role has **read-only** access to:
- `s3://yelp-flink-dev-us-west-2/` (checkpoints/savepoints -- cannot write!)

The `data-streams` role **CANNOT** access:
- Service-specific Paimon warehouse buckets (e.g., `s3://streamhouse-mysql-*`)
- Production checkpoint buckets
- DynamoDB `flink_job` table (PutItem denied -- use `job_type: testing` to bypass)

The `yelp-streamhouse-playground-dev-us-west-2` bucket has:
- 14-day retention (auto-cleanup)
- Open to all Yelp engineers
- Use path: `s3://yelp-streamhouse-playground-dev-us-west-2/paimon/$USER/...`

If the SQL job references a Paimon source on an inaccessible bucket, the job will
submit successfully but fail at runtime with `AmazonS3Exception: Forbidden`.

## Replicating Real k2p -> Streamhouse Queries Pipelines

The playground can run real migration patterns (k2p -> sq) locally using the
`streamhouse-data-hydration` IAM role, which has access to both source Paimon
warehouses and a writable S3 bucket for checkpoints/sink.

### IAM Role Setup

The `data-streams` role (from `aws-okta`) cannot access service-specific Paimon
source buckets. To read from real Paimon tables, assume a service-specific role:

```bash
# Store creds in a file (avoid printing secrets in terminal)
aws --profile dev sts assume-role \
  --role-arn arn:aws:iam::528741615426:role/streamhouse-data-hydration \
  --role-session-name playground-test \
  --duration-seconds 3600 \
  --output json > /tmp/playground-creds.json

# Inject into playground config
python3 -c "
import yaml, json
with open('/tmp/playground-creds.json') as f:
    cred_json = json.load(f)
creds = {
    'AWS_ACCESS_KEY_ID': cred_json['Credentials']['AccessKeyId'],
    'AWS_SECRET_ACCESS_KEY': cred_json['Credentials']['SecretAccessKey'],
    'AWS_SESSION_TOKEN': cred_json['Credentials']['SessionToken'],
    'AWS_DEFAULT_REGION': 'us-west-2',
    'IS_FLINKV2_ENABLED': 'true',
}
config_path = 'soa_config_playground/streamhouse_queries/flink-kind-\$USER-k8s-test.yaml'
with open(config_path) as f:
    config = yaml.safe_load(f)
config['canary']['env'] = creds
with open(config_path, 'w') as f:
    yaml.dump(config, f, default_flow_style=False)
print(f'Updated. Expires: {cred_json[\"Credentials\"][\"Expiration\"]}')
"
make setup-kubernetes-cr
```

### What `streamhouse-data-hydration` Role Can Access

| Bucket | Access | Use for |
|--------|--------|---------|
| `streamhouse-mysql-yelp-video-contribution-dev` | Read | Paimon source tables |
| `yelp-streamhouse-data-hydration-dev-us-west-2` | Read/Write | Checkpoints, savepoints, Paimon sink |
| `yelp-streamhouse-plugins-dev-us-west-2` | Read | UDF plugin downloads |

### Example: data_hydration Canary (Verified Working)

This replicates the real `data_hydration_biz_video_like_v1_clone_for_canary` job
reading from Paimon source tables with a stateful JOIN and Python UDF:

```yaml
canary:
  app_type: streamhouse
  job_type: testing
  service_type: application
  checkpoint_path: s3://yelp-streamhouse-data-hydration-dev-us-west-2/playground-checkpoints/$USER/checkpoints/
  savepoint_path: s3://yelp-streamhouse-data-hydration-dev-us-west-2/playground-checkpoints/$USER/savepoints/
  always_allow_non_restored_state: true
  udf_plugin_name: canary_noop
  udf_plugin_version: 1.0.0
  flink_supervisor:
    cpus: 1
    disk: 4096
    mem: 4096
  jobs:
    playground_data_hydration_canary:
      sql: |
        SET 'execution.runtime-mode' = 'streaming';
        CREATE TEMPORARY SYSTEM FUNCTION canary_noop AS 'canary_noop_pyudf.canary_noop' LANGUAGE PYTHON;
        CREATE CATALOG paimon_source WITH (
            'type' = 'paimon',
            'warehouse' = 's3://streamhouse-mysql-yelp-video-contribution-dev/warehouse_v1'
        );
        CREATE CATALOG paimon_sink WITH (
            'type' = 'paimon',
            'warehouse' = 's3://yelp-streamhouse-data-hydration-dev-us-west-2/playground-checkpoints/$USER/warehouse'
        );
        CREATE DATABASE IF NOT EXISTS paimon_sink.playground;
        CREATE TABLE IF NOT EXISTS paimon_sink.playground.biz_video_like_join (
            id INT, video_id INT, business_id INT,
            __internal__op_ts BIGINT, dt VARCHAR,
            PRIMARY KEY (`business_id`, `dt`) NOT ENFORCED
        ) WITH ('sequence.field' = '__internal__op_ts');
        INSERT INTO paimon_sink.playground.biz_video_like_join
        SELECT
            COALESCE(bvl.id, 0) AS id,
            COALESCE(bvl.video_id, 0) AS video_id,
            COALESCE(bv.business_id, 0) AS business_id,
            GREATEST(bvl.__internal__op_ts, bv.__internal__op_ts) AS __internal__op_ts,
            canary_noop(date_format(TO_TIMESTAMP_LTZ(bvl.time_created, 0), 'yyyy-MM-dd')) AS dt
        FROM paimon_source.yelp_video_contribution.biz_video_like /*+ OPTIONS('consumer-id' = 'flink.streamhouse_queries.canary.playground_data_hydration_canary.local') */ bvl
        INNER JOIN paimon_source.yelp_video_contribution.biz_video /*+ OPTIONS('consumer-id' = 'flink.streamhouse_queries.canary.playground_data_hydration_canary.local') */ bv
        ON bvl.video_id = bv.id;
```

### Isolation Measures

| Concern | Mitigation |
|---------|-----------|
| Consumer offset interference | `.local` suffix on consumer-id (separate Paimon consumer progress) |
| Sink writes to prod tables | Sink uses isolated path (`/playground-checkpoints/$USER/warehouse`) |
| Job name clash | Unique name `playground_data_hydration_canary` |
| DynamoDB writes | `job_type: testing` bypasses DynamoDB entirely |
| Checkpoints on shared bucket | Path-isolated under `$USER` prefix |

### Adapting for Other Migrations

To test a different migration locally, transform the generated config:

| Real config | Playground substitute |
|---|---|
| `'metastore' = 'jdbc'` | Remove (use filesystem catalog) |
| `'warehouse' = 's3://streamhouse-mysql-...'` | Keep if role has read access, or swap to playground path |
| `consumer-id = 'flink...job'` | Append `.local` suffix |
| Checkpoint/savepoint paths | Use bucket the assumed role can write to |
| `job_type: stateful` | `job_type: testing` |
| Job name | Prefix with `playground_` |

### Testing with a Locally-Built UDF (No S3 Upload)

When the UDF hasn't been published to S3 yet, you can inject it into the supervisor
pod via `kubectl cp`. The supervisor's `download_from_s3()` skips the download if the
file already exists at `/tmp/{expected_filename}`.

**How it works:**

1. The supervisor reads `udf_plugin_name` + `udf_plugin_version` from the flink config
2. It constructs the expected local path: `/tmp/streamhouse_plugins-{name}-{version}-{ubuntu_version}.zip`
3. If the file exists, it skips S3 and uses the local copy
4. If the file doesn't exist, it tries S3 (and fails with 404 for unpublished UDFs)
5. On failure, the supervisor retries with exponential backoff

**Steps:**

```bash
# 1. Use a unique plugin name (append _local) to avoid conflicts
#    In soa_config_playground/<service>/flink-kind-$USER-k8s-test.yaml:
#      udf_plugin_name: my_udf_local
#      udf_plugin_version: 0.0.1

# 2. Deploy the cluster (supervisor will fail initial S3 download -- expected)
make setup-kubernetes-cr

# 3. Wait for supervisor pod to start
KUBECONFIG=./k8s_itests/kubeconfig kubectl get pods -n paasta-flinks -w

# 4. Download an existing compatible UDF from S3 (or package your own)
#    The ZIP must be built for the container's Ubuntu version (check Dockerfile base image)
aws s3 cp s3://yelp-streamhouse-plugins-dev-us-west-2/streamhouse_plugins-canary_noop-1.0.0-ubuntu_24_04.zip /tmp/my_udf.zip

# 5. Copy into supervisor pod with the exact filename the supervisor expects
SUPERVISOR_POD=$(KUBECONFIG=./k8s_itests/kubeconfig kubectl get pods -n paasta-flinks -l flink.yelp.com/container-type=supervisor -o jsonpath='{.items[0].metadata.name}')
KUBECONFIG=./k8s_itests/kubeconfig kubectl cp /tmp/my_udf.zip paasta-flinks/$SUPERVISOR_POD:/tmp/streamhouse_plugins-my_udf_local-0.0.1-ubuntu_24_04.zip

# 6. Supervisor retries automatically -- check logs for success
KUBECONFIG=./k8s_itests/kubeconfig kubectl logs -n paasta-flinks $SUPERVISOR_POD --tail=10
# Look for: "Job ... submitted successfully."

# 7. Verify job is running
KUBECONFIG=./k8s_itests/kubeconfig kubectl exec -n paasta-flinks <jm-pod> -- curl -s http://localhost:8081/jobs
```

**Expected filename format:** `streamhouse_plugins-{udf_plugin_name}-{udf_plugin_version}-{ubuntu_version}.zip`

Where `ubuntu_version` is determined by the container's OS (check with
`lsb_release -irs` inside the pod). For `noble_yelp` base images this is `ubuntu_24_04`.

**Packaging your own UDF:** Use `streamhouse_queries/acceptance/test_utils/package_udf.sh`:
```bash
cd ~/source/streamhouse_queries
bash acceptance/test_utils/package_udf.sh path/to/my_udf.py 0.0.1 /tmp/udf_output
```
Note: this builds for the local devbox platform (ubuntu_22_04). If the container uses a
different Ubuntu version, either build inside the container or copy an existing ZIP from
S3 and replace only the Python source file inside.

**Timing:** The supervisor retries every few seconds with exponential backoff (starting
at ~5s, max ~60s). The file just needs to be in place before the next retry.

### Choosing the Right IAM Role

| Migration source | Role to assume | ARN |
|---|---|---|
| yelp_video_contribution | `streamhouse-data-hydration` | `arn:aws:iam::528741615426:role/streamhouse-data-hydration` |
| Other services | Check `yelpsoa-configs/<service>/flinkeks-pnw-devc.yaml` for `iam_role` field |

## Accessing the Flink UI

The Flink UI runs inside the JobManager pod. To access it from a browser:

**Step 1: Port-forward on the devbox** (run in a separate terminal):

```bash
# Find the JM service name
KUBECONFIG=./k8s_itests/kubeconfig kubectl get svc -n paasta-flinks
# Port-forward (service name includes config hash)
KUBECONFIG=./k8s_itests/kubeconfig kubectl port-forward -n paasta-flinks svc/<jm-service-name> 8081:8081
```

**Step 2: SSH tunnel from your local machine:**

```bash
ssh -L 8081:localhost:8081 devbox-$USER-main
```

Then open http://localhost:8081 in your browser.

**Troubleshooting "Connection refused":** The port-forward must be running first. If
the service name changed (new config hash after CR reconcile), re-run `kubectl get svc`
to find the new name.

**CLI alternative (no browser needed):**

```bash
# From the supervisor pod, curl the JM REST API directly
KUBECONFIG=./k8s_itests/kubeconfig kubectl exec -n paasta-flinks <supervisor-pod> -- \
  curl -s http://<jm-service>.paasta-flinks.svc.cluster.local:8081/jobs
```

## Devbox Restart Recovery

Kind clusters don't survive devbox reboots. Recovery steps:

```bash
cd ~/source/paasta
make k8s_recreate_cluster   # deletes stale cluster + markers, creates fresh
make setup-flink            # deploys operator, CRD, RBAC, CRs
# Re-inject AWS creds and reconcile
make setup-kubernetes-cr
```

### Disabling Unused Services

To save resources, disable services you're not testing:

```bash
mv soa_config_playground/acorn soa_config_playground/acorn.disabled
```

Then `make setup-kubernetes-cr` won't recreate them. To fully clean up already-running
pods: `KUBECONFIG=./k8s_itests/kubeconfig kubectl delete flink -n paasta-flinks <name>`
