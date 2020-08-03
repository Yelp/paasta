PROJECT_NAME ?= clusterman
WHOAMI := $(shell whoami)
KIND_CLUSTER ?= $(WHOAMI)-$(PROJECT_NAME)
KUBECONFIG ?= .local-$(KIND_CLUSTER).conf
DOCKER_TAG ?= $(PROJECT_NAME)-dev-$(WHOAMI)
DOCKER_IMAGE := $(shell echo -n $(DOCKER_TAG) | sed 's|docker-paasta.yelpcorp.com:443/\(.*\):.*|\1|')
DOCKER_PORT ?= 31234
DOCKER_REGISTRY ?= $(KIND_CLUSTER)-reg
DOCKER_COMPOSE := COMPOSE_PROJECT_NAME=$(PROJECT_NAME)_k8s ../.tox/acceptance/bin/docker-compose -f docker-compose-k8s.yaml

.PHONY: local-cluster-internal
local-cluster-internal: .local-cluster.yaml .local-clusterman-internal.yaml .local-cluster-up
	@echo Done.

.PHONY: local-cluster-external
local-cluster-external: .local-cluster.yaml .local-clusterman-external.yaml .local-cluster-up
	@echo Done.

.local-cluster-up:
	kind create cluster --name $(KIND_CLUSTER) --config .local-cluster.yaml --verbosity 2
	kubectl -n kube-system get configmap/coredns -o yaml | \
		sed "s|/etc/resolv.conf|$$(awk '/nameserver/ { print $$2; exit }' /etc/resolv.conf)|" | \
		kubectl replace -f -
	./k8s-local-docker-registry.sh $(DOCKER_REGISTRY) $(DOCKER_PORT) $(KIND_CLUSTER)
	docker tag $(DOCKER_TAG) localhost:$(DOCKER_PORT)/$(DOCKER_IMAGE)
	docker push localhost:$(DOCKER_PORT)/$(DOCKER_IMAGE)
	docker exec $(KIND_CLUSTER)-control-plane mkdir -p /var/lib/clusterman
	docker cp $(KUBECONFIG) $(KIND_CLUSTER)-control-plane:/var/lib/clusterman/clusterman.conf
	docker exec $(KIND_CLUSTER)-control-plane chmod 644 /var/lib/clusterman/clusterman.conf
	docker exec $(KIND_CLUSTER)-control-plane sed -i "s/\(.*server: \).*/\1https:\\/\\/127.0.0.1:6443/g" /var/lib/clusterman/clusterman.conf
	$(DOCKER_COMPOSE) up -d moto-ec2 moto-s3 moto-dynamodb
	sleep 10  # Give some time for the moto containers to come up
	ACCEPTANCE_ROOT=. DISTRIB_CODENAME=bionic ../.tox/acceptance/bin/python run_instance.py \
		$$($(DOCKER_COMPOSE) ps moto-ec2 | tail -n 1 | sed -e 's|^.*:\(.*\)->5000.*$$|http://127.0.0.1:\1|') \
		$$($(DOCKER_COMPOSE) ps moto-s3 | tail -n 1 | sed -e 's|^.*:\(.*\)->5000.*$$|http://127.0.0.1:\1|') \
		$$($(DOCKER_COMPOSE) ps moto-dynamodb | tail -n 1 | sed -e 's|^.*:\(.*\)->5000.*$$|http://127.0.0.1:\1|') \
		172.27.0.0/24
	touch .local-cluster-up

.PHONY: acceptance-%
export KIND_CLUSTER
export KUBECONFIG
acceptance-%: local-cluster-%
	kubectl apply -f .local-clusterman-$*.yaml
	echo "Checking to see if Clusterman is running..."
	running=1; \
	for count in 1 2 3 4 5 6 7 8 9 10; do \
		echo "Not running yet, waiting 10 seconds and trying again (check $$count of 10)..."; \
		sleep 10; \
		kubectl describe pods; \
		kubectl logs $$(kubectl get pods | grep clusterman | cut -f1 -d' '); \
		if kubectl get pods | grep -q clusterman.*Running; then echo "Clusterman running successfully!"; running=0; break; fi; \
	done; \
	exit $$running

define LOCAL_CLUSTER_YAML
---
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
  - role: control-plane
    extraMounts:
      - containerPath: /nail/srv/configs
        hostPath: ./srv-configs
        readOnly: true
      - containerPath: /etc/boto_cfg/clusterman.json
        hostPath: ./clusterman.json
        readOnly: true
      - containerPath: /etc/boto_cfg/clusterman.sh
        hostPath: ./clusterman.json
        readOnly: true
      - containerPath: /etc/boto_cfg/clusterman_metrics.json
        hostPath: ./clusterman.json
        readOnly: true
      - containerPath: /nail/etc/services/services.yaml
        hostPath: /nail/etc/services/services.yaml
        readOnly: true
  - role: worker
  - role: worker
  - role: worker
containerdConfigPatches:
- |-
  [plugins."io.containerd.grpc.v1.cri".registry.mirrors."localhost:$(DOCKER_PORT)"]
    endpoint = ["http://$(DOCKER_REGISTRY):$(DOCKER_PORT)"]
endef

define LOCAL_CMAN_DEPLOYMENT
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: clusterman
  labels:
    app: clusterman
spec:
  replicas: 1
  selector:
    matchLabels:
      app: clusterman
  template:
    metadata:
      labels:
        app: clusterman
    spec:
      hostNetwork: true
      nodeSelector:
        kubernetes.io/hostname: $(KIND_CLUSTER)-control-plane
      tolerations:
        - operator: Exists
      containers:
        - name: clusterman
          image: localhost:$(DOCKER_PORT)/$(DOCKER_IMAGE)
          command: ['python']
          args: ['-m', $(CMD), $(EXTRA_ARGS)]
          env:
            - name: AWS_ENDPOINT_URL_ARGS
              value: --endpoint-url http://moto-s3:5000
            - name: CMAN_CLUSTER
              value: local-dev
            - name: CMAN_POOL
              value: default
            - name: CMAN_SCHEDULER
              value: kubernetes
          volumeMounts:
            - name: services-yaml
              mountPath: /nail/etc/services/services.yaml
            - name: srvconfigs
              mountPath: /nail/srv/configs
            - name: botocfg
              mountPath: /etc/boto_cfg
            - name: kubeconfig
              mountPath: /var/lib/clusterman/clusterman.conf
      volumes:
        - name: services-yaml
          hostPath:
            path: /nail/etc/services/services.yaml
        - name: srvconfigs
          hostPath:
            path: /nail/srv/configs
            type: Directory
        - name: botocfg
          hostPath:
            path: /etc/boto_cfg
            type: Directory
        - name: kubeconfig
          hostPath:
            path: /var/lib/clusterman/clusterman.conf
            type: File
endef

export LOCAL_CLUSTER_YAML
.local-cluster.yaml:  ## Create config file for `kind` cluster manager
	$(MAKE) local-cluster-clean || true
	@echo "$$LOCAL_CLUSTER_YAML" > .local-cluster.yaml

export LOCAL_CMAN_DEPLOYMENT
export CMD="clusterman.batch.autoscaler_bootstrap"
.local-clusterman-internal.yaml:  ## Create config file for `kind` cluster manager
	@echo "$$LOCAL_CMAN_DEPLOYMENT" > .local-clusterman-internal.yaml

export LOCAL_CMAN_DEPLOYMENT
export EXTRA_ARGS="--env-config-path=/nail/srv/configs/clusterman-external.yaml"
export CMD="examples.batch.autoscaler_bootstrap"
.local-clusterman-external.yaml:  ## Create config file for `kind` cluster manager
	@echo "$$LOCAL_CMAN_DEPLOYMENT" > .local-clusterman-external.yaml

.PHONY: local-cluster-clean
local-cluster-clean:  ## Run if your local cluster is having issues starting
	-kind delete cluster --name $(KIND_CLUSTER) --verbosity 2
	-docker stop $(DOCKER_REGISTRY)
	-docker rm $(DOCKER_REGISTRY)
	-../.tox/acceptance/bin/docker-compose -f docker-compose-k8s.yaml down
	rm -f .local*
