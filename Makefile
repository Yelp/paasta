.PHONY: all docs test itest

docs:
	tox -e docs

test:
	rm -rf .tox
	tox

itest: test
#	tox -e general_itests
#	tox -e paasta_itests

# See the makefile in yelp_package/Makefile for packaging stuff
itest_%:
	make -C yelp_package $@

# Steps to release
# 1. Bump version in yelp_package/Makefile
# 2. `make release`
release:
	make -C yelp_package release


clean:
	rm -rf ./dist
	make -C yelp_package clean
	rm -rf docs/build
	find . -name '*.pyc' -delete
	find . -name '__pycache__' -delete
	rm -rf .tox

