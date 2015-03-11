.PHONY: all docs test itest

docs:
	tox -e docs

test:
	rm -rf .tox
	tox

itest: test
	tox -e general_integration
	tox -e marathon_integration

# See the makefile in yelp_package/Makefile for packaging stuff
itest_%: itest
	make -C yelp_package $@

release:
	make -C yelp_package release


clean:
	rm -f ./dist
	make -C yelp_package clean
	rm -rf docs/build
	find . -name '*.pyc' -delete
	find . -name '__pycache__' -delete
	rm -rf .tox

