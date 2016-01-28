# Copyright 2015 Yelp Inc.
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
RELEASE=0.16.21-yelp1

SHELL=/bin/bash

UID:=`id -u`
GID:=`id -g`
DOCKER_RUN_LUCID:=docker run -t -v  $(CURDIR)/../:/work:rw paastatools_lucid_container
DOCKER_RUN_TRUSTY:=docker run -t -v  $(CURDIR)/../:/work:rw paastatools_trusty_container
ADD_MISSING_DEPS:=-diff --unchanged-line-format= --old-line-format= --new-line-format='%L' ../requirements.txt ./extra_requirements.txt >> ../requirements.txt

build_lucid_docker:
	[ -d ../dist ] || mkdir ../dist
	cd dockerfiles/lucid/ && docker build -t "paastatools_lucid_container" .
package_lucid: build_lucid_docker
	cp ../requirements.txt ../requirements.txt.old
	$(ADD_MISSING_DEPS)
	$(DOCKER_RUN_LUCID) /bin/bash -c "dpkg-buildpackage -d && mv ../*.deb dist/"
	mv ../requirements.txt.old ../requirements.txt
	$(DOCKER_RUN_LUCID) chown -R $(UID):$(GID) /work
itest_lucid: package_lucid
	$(DOCKER_RUN_LUCID) /work/yelp_package/itest/ubuntu.sh


build_trusty_docker:
	[ -d ../dist ] || mkdir ../dist
	cd dockerfiles/trusty/ && docker build -t "paastatools_trusty_container" .
package_trusty: build_trusty_docker
	cp ../requirements.txt ../requirements.txt.old
	$(ADD_MISSING_DEPS)
	$(DOCKER_RUN_TRUSTY) /bin/bash -c "dpkg-buildpackage -d && mv ../*.deb dist/"
	mv ../requirements.txt.old ../requirements.txt
	$(DOCKER_RUN_TRUSTY) chown -R $(UID):$(GID) /work
itest_trusty: package_trusty
	$(DOCKER_RUN_TRUSTY) /work/yelp_package/itest/ubuntu.sh


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
	sed -i -e "s/version.*=.*/version        = '$(VERSION)',/" ../setup.py
	@echo "$(RELEASE) has the changelog set."
	github_changelog_generator --future-release $(VERSION) || echo github_changelog_generator not installed, not generating changelog!!
	cd .. && make docs
	@git diff
	@echo "Now Run:"
	@echo 'git commit -a -m "Released $(RELEASE) via make release"'
	@echo 'git tag --force v$(VERSION)'
	@echo 'git push --tags origin master'
