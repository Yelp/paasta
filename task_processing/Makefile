.PHONY: all docs test itest

docs: .taskproc/bin/activate
	.taskproc/bin/tox -e docs

test: .taskproc/bin/activate
	.taskproc/bin/tox

itest:
	cd tests/integration && \
	docker-compose -f cluster/docker-compose.yaml down && \
	docker-compose -f cluster/docker-compose.yaml pull && \
	docker-compose -f cluster/docker-compose.yaml build && \
	docker-compose -f cluster/docker-compose.yaml up -d zookeeper mesosmaster mesosslave && \
	docker-compose -f cluster/docker-compose.yaml scale mesosslave=2 && \
	docker-compose -f cluster/docker-compose.yaml run playground /src/itest.sh

.taskproc/bin/activate:
	test -d .taskproc/bin/activate || virtualenv -p python3.6 .taskproc
	.taskproc/bin/pip install -U tox
	touch .taskproc/bin/activate

clean:
	rm -rf docs/build
	find . -name '*.pyc' -delete
	find . -name '__pycache__' -delete
	rm -rf .tox .taskproc
