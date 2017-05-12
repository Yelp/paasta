.PHONY: all docs test itest

docs: .taskproc/bin/activate
	.taskproc/bin/tox -e docs

test: .taskproc/bin/activate
	.taskproc/bin/tox

itest:
	.taskproc/bin/tox -e integration

.taskproc/bin/activate:
	test -d .taskproc/bin/activate || virtualenv -p python3.6 .taskproc
	.taskproc/bin/pip install -U tox
	touch .taskproc/bin/activate

clean:
	rm -rf docs/build
	find . -name '*.pyc' -delete
	find . -name '__pycache__' -delete
	rm -rf .tox .taskproc
