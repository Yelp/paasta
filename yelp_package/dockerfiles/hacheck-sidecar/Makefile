all: build push

build:
	docker build -t docker-paasta.yelpcorp.com:443/hacheck-k8s-sidecar .

push:
	sudo -i bash -c 'docker push docker-paasta.yelpcorp.com:443/hacheck-k8s-sidecar'
