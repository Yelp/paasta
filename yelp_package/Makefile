# Copyright 2015-2016 Yelp Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Edit this release and run "make release"
RELEASE=0.48.1

SHELL=/bin/bash

UID:=`id -u`
GID:=`id -g`
DOCKER_RUN_LUCID:=docker run -t -v  $(CURDIR)/../:/work:rw paastatools_lucid_container
DOCKER_RUN_TRUSTY:=docker run -t -v  $(CURDIR)/../:/work:rw paastatools_trusty_container

NOOP = true
ifeq ($(PAASTA_ENV),YELP)
	ADD_MISSING_DEPS_MAYBE:=-diff --unchanged-line-format= --old-line-format= --new-line-format='%L' ../requirements.txt ./extra_requirements_yelp.txt >> ../requirements.txt
	ACTUAL_PACKAGE_VERSION:=$(RELEASE)-yelp1
	ADD_YELP_PREFIX_MAYBE:=dch -v $(ACTUAL_PACKAGE_VERSION) --changelog ../debian/changelog $$'Build for yelp: add scribereader to virtualenv'
else
	ADD_MISSING_DEPS_MAYBE:=$(NOOP)
	ADD_YELP_PREFIX_MAYBE:=$(NOOP)
	ACTUAL_PACKAGE_VERSION:=$(RELEASE)
endif


build_lucid_docker:
	[ -d ../dist ] || mkdir ../dist
	cd dockerfiles/lucid/ && docker build -t "paastatools_lucid_container" .
package_lucid: build_lucid_docker
	# Copy these files to .old before maybe clobbering them
	cp ../requirements.txt ../requirements.txt.old
	cp ../debian/changelog ../debian/changelog.old
	$(ADD_MISSING_DEPS_MAYBE)
	$(ADD_YELP_PREFIX_MAYBE)
	$(DOCKER_RUN_LUCID) /bin/bash -c "dpkg-buildpackage -d && mv ../*.deb dist/"
	# then move them back
	mv ../requirements.txt.old ../requirements.txt
	mv ../debian/changelog.old ../debian/changelog
	$(DOCKER_RUN_LUCID) chown -R $(UID):$(GID) /work
itest_lucid: package_lucid
	$(DOCKER_RUN_LUCID) /work/yelp_package/itest/ubuntu.sh paasta-tools_$(ACTUAL_PACKAGE_VERSION)_amd64.deb


build_trusty_docker:
	[ -d ../dist ] || mkdir ../dist
	cd dockerfiles/trusty/ && docker build -t "paastatools_trusty_container" .
package_trusty: build_trusty_docker
	# Copy these files to .old before maybe clobbering them
	cp ../requirements.txt ../requirements.txt.old
	cp ../debian/changelog ../debian/changelog.old
	$(ADD_MISSING_DEPS_MAYBE)
	$(ADD_YELP_PREFIX_MAYBE)
	$(DOCKER_RUN_TRUSTY) /bin/bash -c "dpkg-buildpackage -d && mv ../*.deb dist/"
	# then move them back
	mv ../requirements.txt.old ../requirements.txt
	mv ../debian/changelog.old ../debian/changelog
	$(DOCKER_RUN_TRUSTY) chown -R $(UID):$(GID) /work
itest_trusty: package_trusty bintray.json
	$(DOCKER_RUN_TRUSTY) /work/yelp_package/itest/ubuntu.sh paasta-tools_$(ACTUAL_PACKAGE_VERSION)_amd64.deb

DATE := $(shell date +'%Y-%m-%d' -d "`dpkg-parsechangelog -l../debian/changelog | sed -n 's/^Date: //p'`")
PAASTAVERSION := $(shell dpkg-parsechangelog -l../debian/changelog | sed -n 's/^Version: //p')
bintray.json: bintray.json.in ../debian/changelog
	sed -e 's/@DATE@/$(DATE)/g' -e 's/@PAASTAVERSION@/$(PAASTAVERSION)/g' $< > $@

clean:
	rm -rf dist/
	find . -name '*.pyc' -delete
	find . -name '__pycache__' -delete


VERSION = $(firstword $(subst -, ,$(RELEASE) ))
# This sed isn't really right but at least it doesn't crash (and I couldn't get
# \' or ''' or \\' to work :( )
LAST_COMMIT_MSG = $(shell git log -1 --pretty=%B | sed -e "s/'/"/g")
# github_changelog_generator comes from
# https://github.com/skywinder/github-changelog-generator
# Follow the instructions to generate a github api token
release:
	dch -v $(RELEASE) --changelog ../debian/changelog $$'$(VERSION) tagged with \'make release\'\rCommit: $(LAST_COMMIT_MSG)'
	sed -i -e "s/__version__ = .*/__version__ = '$(VERSION)'/" ../paasta_tools/__init__.py
	@echo "$(RELEASE) has the changelog set."
	github_changelog_generator --future-release $(VERSION) || echo github_changelog_generator not installed, not generating changelog!!
	cd .. && make docs
	@git diff
	@echo "Now Run:"
	@echo 'git commit -a -m "Released $(RELEASE) via make release"'
	@echo 'git tag --force v$(VERSION)'
	@echo 'git push --tags origin master'
