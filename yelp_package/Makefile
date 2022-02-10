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
RELEASE=0.120.1

SHELL=/bin/bash

UID:=`id -u`
GID:=`id -g`
DOCKER_RUN=docker run -t -v $(CURDIR)/../:/work:rw -e PIP_INDEX_URL=$(PIP_INDEX_URL) yelp/paastatools_$*_container

NOOP = true
ifeq ($(PAASTA_ENV),YELP)
	ADD_MISSING_DEPS_MAYBE:=-diff --unchanged-line-format= --old-line-format= --new-line-format='%L' ../requirements.txt ./extra_requirements_yelp.txt >> ../requirements.txt
	ACTUAL_PACKAGE_VERSION=$(RELEASE)-yelp1
	ADD_VERSION_SUFFIX=dch -v $(ACTUAL_PACKAGE_VERSION) --force-distribution --distribution $* --changelog ../debian/changelog 'Build for yelp: add scribereader to virtualenv'
else
	ADD_MISSING_DEPS_MAYBE:=$(NOOP)
	ACTUAL_PACKAGE_VERSION=$(RELEASE)~$*1
	ADD_VERSION_SUFFIX=dch -b -v $(ACTUAL_PACKAGE_VERSION) --force-distribution --distribution $* --changelog ../debian/changelog 'Build for $*'
endif

build_%_docker:
	[ -d ../dist ] || mkdir ../dist
	docker pull "yelp/paastatools_$*_container" || true
	cd dockerfiles/$*/ && docker build --build-arg DOCKER_REGISTRY=$(DOCKER_REGISTRY) \
        --build-arg PIP_INDEX_URL=$(PIP_INDEX_URL) -t "yelp/paastatools_$*_container" .
	if [ "$$TRAVIS_BRANCH" = "master" -a \
	     "$$TRAVIS_PULL_REQUEST" = "false" -a \
	     "$$DOCKER_USERNAME" != "" ]; then \
	  docker push "yelp/paastatools_$*_container"; \
	fi

.SECONDEXPANSION:
itest_%: package_$$*
	$(DOCKER_RUN) /work/yelp_package/itest/ubuntu.sh paasta-tools_$(ACTUAL_PACKAGE_VERSION)_amd64.deb

PTG_VERSION=0.0.12
PTG_SUM=1fa3f94fe5dd00059186404c2e2717625808c1cdefd90bd551f16b6e72018640
ifeq ($(PAASTA_ENV),YELP)
	PTG_SANITY=monk
else
	PTG_SANITY=http
endif

go_entrypoint:
	rm -rf gopath && \
	mkdir -p gopath && \
	cd gopath && \
	wget https://github.com/Yelp/paasta-tools-go/archive/v$(PTG_VERSION).zip && \
	sha256sum v$(PTG_VERSION).zip | grep -q ^$(PTG_SUM) && \
	unzip v$(PTG_VERSION).zip && \
	cd paasta-tools-go-$(PTG_VERSION) && \
	PAASTA_VERSION=$(RELEASE) make paasta_go && \
	./paasta_go -version | grep "paasta-tools-go version: $(PTG_VERSION)" && \
	./paasta_go -version | grep "initializers.*$(PTG_SANITY)" && \
	mv ./paasta_go ../

package_%: build_$$*_docker go_entrypoint
	# Copy these files to .old before maybe clobbering them
	cp ../requirements.txt ../requirements.txt.old
	cp ../debian/changelog ../debian/changelog.old
	$(ADD_MISSING_DEPS_MAYBE)
	$(ADD_VERSION_SUFFIX)
	cat ../extra-linux-requirements.txt >> ../requirements.txt

	# noddebs to work around https://bugs.debian.org/cgi-bin/bugreport.cgi?bug=897569
	$(DOCKER_RUN) /bin/bash -c "DEB_BUILD_OPTIONS=noddebs dpkg-buildpackage -d && mv ../*.deb dist/"
	$(DOCKER_RUN) chown -R $(UID):$(GID) /work
	# then move them back
	mv ../requirements.txt.old ../requirements.txt
	mv ../debian/changelog.old ../debian/changelog

clean:
	rm -rf dist/
	find . -name '*.pyc' -delete
	find . -name '__pycache__' -delete


VERSION = $(firstword $(subst -, ,$(RELEASE) ))
LAST_COMMIT_MSG = $(shell git log -1 --pretty=%B | sed -e 's/\x27/"/g')
release:
	@if [[ "$$(git status --porcelain --untracked-files=no :^/yelp_package/Makefile)" != '' ]]; then echo "Error: Working directory is not clean; only changes to yelp_package/Makefile are allowed when cutting a release."; exit 1; fi
	$(eval untracked_files_tmpfile=$(shell mktemp))
	git status --porcelain --untracked-files=all :^/yelp_package/Makefile > $(untracked_files_tmpfile)
	@if [[ "$$(git status --porcelain --untracked-files=normal :/docs/source/generated)" != '' ]]; then echo "Error: Untracked files found in docs/source/generated."; exit 1; fi
	@if existing_sha=$$(git rev-parse --verify --quiet v$(VERSION)); then echo "Error: tag v$(VERSION) exists and points at $$existing_sha"; exit 1; fi
	@read upstream_master junk <<<"$$(git ls-remote -h origin master)" && if ! git merge-base --is-ancestor $$upstream_master HEAD; then echo "Error: HEAD is missing commits from origin/master ($$upstream_master)."; exit 1; fi
	dch -v $(RELEASE) --distribution xenial --changelog ../debian/changelog $$'$(VERSION) tagged with \'make release\'\rCommit: $(LAST_COMMIT_MSG)'
	sed -i -e "s/__version__ = .*/__version__ = \"$(VERSION)\"/" ../paasta_tools/__init__.py
	cd .. && make docs || true
	git add ./Makefile ../debian/changelog ../paasta_tools/__init__.py ../docs/source/generated/
	git commit -m "Released $(RELEASE) via make release"
	if [[ "$$(git status --porcelain --untracked-files=all)" != "$$(<$(untracked_files_tmpfile))" ]]; then echo "Error: automatic git commit left some files uncommitted. Fix the git commit command in yelp_package/Makefile to include any automatically generated files that it is currently missing."; exit 1; fi
	git tag v$(VERSION)
	git push --atomic origin master v$(VERSION)
