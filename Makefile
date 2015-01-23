.PHONY: all docs test itest

docs:
	tox -e docs

test:
	tox -r

itest: test
	tox -e marathon_integration

# See the makefile in yelp_package/Makefile for packaging stuff
itest_%: itest
	make -C yelp_package $@

release:
	make -C yelp_package release
