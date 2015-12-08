# Change Log

## [0.16.5](https://github.com/Yelp/paasta/tree/0.16.5) (2015-12-08)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.16.4...0.16.5)

**Merged pull requests:**

- Make backoff\_seconds work as a function of the instance count. [\#102](https://github.com/Yelp/paasta/pull/102) ([solarkennedy](https://github.com/solarkennedy))

## [v0.16.4](https://github.com/Yelp/paasta/tree/v0.16.4) (2015-12-08)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.16.3...v0.16.4)

**Closed issues:**

- paasta local-run --interactive default cmd is "b a s h" [\#115](https://github.com/Yelp/paasta/issues/115)
- paasta local-run --help crashes [\#114](https://github.com/Yelp/paasta/issues/114)
- automate builds with pypi [\#35](https://github.com/Yelp/paasta/issues/35)

**Merged pull requests:**

- Added docs explaining the difference between difference service modes [\#113](https://github.com/Yelp/paasta/pull/113) ([solarkennedy](https://github.com/solarkennedy))
- Documented the relationship between instance and nerve\_ns better [\#112](https://github.com/Yelp/paasta/pull/112) ([solarkennedy](https://github.com/solarkennedy))
- Support --version instead of version [\#109](https://github.com/Yelp/paasta/pull/109) ([asottile](https://github.com/asottile))
- Fix --help for local-run and add a test [\#108](https://github.com/Yelp/paasta/pull/108) ([asottile](https://github.com/asottile))

## [v0.16.3](https://github.com/Yelp/paasta/tree/v0.16.3) (2015-12-02)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.16.2...v0.16.3)

**Merged pull requests:**

- Setup Travis autodeploy configs [\#103](https://github.com/Yelp/paasta/pull/103) ([tomelm](https://github.com/tomelm))
- Paasta rollback now accepts none, one or a list of instances. [\#99](https://github.com/Yelp/paasta/pull/99) ([zeldinha](https://github.com/zeldinha))
- Enabled local-run to work on docker images on registries instead of building locally [\#88](https://github.com/Yelp/paasta/pull/88) ([solarkennedy](https://github.com/solarkennedy))

## [v0.16.2](https://github.com/Yelp/paasta/tree/v0.16.2) (2015-11-30)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.16.1...v0.16.2)

**Closed issues:**

- 0.16.1 Release Broke 'paasta status' [\#91](https://github.com/Yelp/paasta/issues/91)
- http://paasta.readthedocs.org/en/latest/ references y/paasta [\#57](https://github.com/Yelp/paasta/issues/57)

**Merged pull requests:**

- Fix scale condition poistion [\#100](https://github.com/Yelp/paasta/pull/100) ([dichiarafrancesco](https://github.com/dichiarafrancesco))
- remove hardcoded docker registry location [\#93](https://github.com/Yelp/paasta/pull/93) ([Rob-Johnson](https://github.com/Rob-Johnson))
- bump scribereader version in line with aed1812b917daba17a5fd8f1a0fe9b… [\#92](https://github.com/Yelp/paasta/pull/92) ([Rob-Johnson](https://github.com/Rob-Johnson))
- Only allow the itest\_% Makefile target to run at Yelp [\#90](https://github.com/Yelp/paasta/pull/90) ([nhandler](https://github.com/nhandler))
- More smartstack.yaml docs: explanation of the top level, and moving a… [\#89](https://github.com/Yelp/paasta/pull/89) ([EvanKrall](https://github.com/EvanKrall))
- Bump scribereader requirement [\#86](https://github.com/Yelp/paasta/pull/86) ([asottile](https://github.com/asottile))

## [v0.16.1](https://github.com/Yelp/paasta/tree/v0.16.1) (2015-11-25)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.16.0...v0.16.1)

**Merged pull requests:**

- Minor fixups to setup.py [\#87](https://github.com/Yelp/paasta/pull/87) ([asottile](https://github.com/asottile))
- Added paasta\_rollback command [\#79](https://github.com/Yelp/paasta/pull/79) ([zeldinha](https://github.com/zeldinha))
- Add emergency scale feature [\#74](https://github.com/Yelp/paasta/pull/74) ([dichiarafrancesco](https://github.com/dichiarafrancesco))
- Make local-run default to use bash if set to be interactive [\#73](https://github.com/Yelp/paasta/pull/73) ([solarkennedy](https://github.com/solarkennedy))
- drain\_method\_params affect drain\_method, not bounce\_method. Also fix uwsgi typo. [\#58](https://github.com/Yelp/paasta/pull/58) ([EvanKrall](https://github.com/EvanKrall))
- Flesh out docs for smartstack.yaml [\#47](https://github.com/Yelp/paasta/pull/47) ([EvanKrall](https://github.com/EvanKrall))

## [v0.16.0](https://github.com/Yelp/paasta/tree/v0.16.0) (2015-11-23)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.15.9...v0.16.0)

**Merged pull requests:**

- don't use vars\(\) to check for scribereader [\#80](https://github.com/Yelp/paasta/pull/80) ([Rob-Johnson](https://github.com/Rob-Johnson))
- update dh-virtualenv location [\#78](https://github.com/Yelp/paasta/pull/78) ([Rob-Johnson](https://github.com/Rob-Johnson))
- Update Flynn comparison [\#76](https://github.com/Yelp/paasta/pull/76) ([titanous](https://github.com/titanous))
- Added first pass at paasta-contract transplant [\#75](https://github.com/Yelp/paasta/pull/75) ([solarkennedy](https://github.com/solarkennedy))

## [v0.15.9](https://github.com/Yelp/paasta/tree/v0.15.9) (2015-11-20)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.15.8...v0.15.9)

**Merged pull requests:**

- Added automatic changelog [\#71](https://github.com/Yelp/paasta/pull/71) ([solarkennedy](https://github.com/solarkennedy))
- Use dulwich for mark for deployment [\#68](https://github.com/Yelp/paasta/pull/68) ([solarkennedy](https://github.com/solarkennedy))

## [v0.15.8](https://github.com/Yelp/paasta/tree/v0.15.8) (2015-11-20)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.15.7...v0.15.8)

## [v0.15.7](https://github.com/Yelp/paasta/tree/v0.15.7) (2015-11-20)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.15.6...v0.15.7)

**Closed issues:**

- ``paasta logs`` should fail gracefully when scribereader depedency is missing [\#63](https://github.com/Yelp/paasta/issues/63)

**Merged pull requests:**

- active\_only=True doesn't work with chronos. Let's try it with False. [\#70](https://github.com/Yelp/paasta/pull/70) ([mrtyler](https://github.com/mrtyler))
- Mrtyler rejigger status [\#69](https://github.com/Yelp/paasta/pull/69) ([mrtyler](https://github.com/mrtyler))
- execute docker pull before running container [\#67](https://github.com/Yelp/paasta/pull/67) ([Rob-Johnson](https://github.com/Rob-Johnson))

## [v0.15.6](https://github.com/Yelp/paasta/tree/v0.15.6) (2015-11-20)
[Full Changelog](https://github.com/Yelp/paasta/compare/help...v0.15.6)

**Merged pull requests:**

- fail gracefully if you try and run paasta logs without scribe existing [\#66](https://github.com/Yelp/paasta/pull/66) ([Rob-Johnson](https://github.com/Rob-Johnson))
- Some refactoring related to the argument 'clusterinstance' [\#65](https://github.com/Yelp/paasta/pull/65) ([zeldinha](https://github.com/zeldinha))

## [help](https://github.com/Yelp/paasta/tree/help) (2015-11-19)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.15.5...help)

## [v0.15.5](https://github.com/Yelp/paasta/tree/v0.15.5) (2015-11-19)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.15.4...v0.15.5)

## [v0.15.4](https://github.com/Yelp/paasta/tree/v0.15.4) (2015-11-19)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.15.3...v0.15.4)

**Closed issues:**

- Opensource some sort of Mesos -\>SmartStack \(nerve\) bridge [\#13](https://github.com/Yelp/paasta/issues/13)

**Merged pull requests:**

- Mrtyler instance filter [\#53](https://github.com/Yelp/paasta/pull/53) ([mrtyler](https://github.com/mrtyler))

## [v0.15.3](https://github.com/Yelp/paasta/tree/v0.15.3) (2015-11-18)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.15.2...v0.15.3)

**Merged pull requests:**

- add missing scribereader dependency when building internally [\#62](https://github.com/Yelp/paasta/pull/62) ([Rob-Johnson](https://github.com/Rob-Johnson))
- reject jobs with an interval more frequent than 60s [\#59](https://github.com/Yelp/paasta/pull/59) ([Rob-Johnson](https://github.com/Rob-Johnson))
- Added reproducible principle [\#56](https://github.com/Yelp/paasta/pull/56) ([solarkennedy](https://github.com/solarkennedy))
- Add coveralls support [\#43](https://github.com/Yelp/paasta/pull/43) ([nhandler](https://github.com/nhandler))

## [v0.15.2](https://github.com/Yelp/paasta/tree/v0.15.2) (2015-11-16)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.15.1...v0.15.2)

**Merged pull requests:**

- upgrade marathon and chronos-python [\#55](https://github.com/Yelp/paasta/pull/55) ([Rob-Johnson](https://github.com/Rob-Johnson))
- Fix typo in README [\#52](https://github.com/Yelp/paasta/pull/52) ([chriskuehl](https://github.com/chriskuehl))
- Use the right srvname [\#51](https://github.com/Yelp/paasta/pull/51) ([mrtyler](https://github.com/mrtyler))
- Added paasta principles [\#50](https://github.com/Yelp/paasta/pull/50) ([solarkennedy](https://github.com/solarkennedy))

## [v0.15.1](https://github.com/Yelp/paasta/tree/v0.15.1) (2015-11-12)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.15.0...v0.15.1)

**Closed issues:**

- comparison table updated against autodesk/ochopod [\#49](https://github.com/Yelp/paasta/issues/49)

**Merged pull requests:**

- fix marathon package naming in requirements.txt [\#48](https://github.com/Yelp/paasta/pull/48) ([Rob-Johnson](https://github.com/Rob-Johnson))

## [v0.15.0](https://github.com/Yelp/paasta/tree/v0.15.0) (2015-11-11)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.14.1...v0.15.0)

**Merged pull requests:**

- Fixing grammar error [\#46](https://github.com/Yelp/paasta/pull/46) ([nickrobinson](https://github.com/nickrobinson))
- Make setup\_chronos\_job handle the situation where the docker image isn't available [\#44](https://github.com/Yelp/paasta/pull/44) ([solarkennedy](https://github.com/solarkennedy))
- Run itests in travis [\#42](https://github.com/Yelp/paasta/pull/42) ([nhandler](https://github.com/nhandler))
- Bump yelp-clog to 2.2.10 [\#41](https://github.com/Yelp/paasta/pull/41) ([EvanKrall](https://github.com/EvanKrall))
- Fixup make targets [\#40](https://github.com/Yelp/paasta/pull/40) ([Rob-Johnson](https://github.com/Rob-Johnson))
- Install tox in travis [\#39](https://github.com/Yelp/paasta/pull/39) ([nhandler](https://github.com/nhandler))
- Fix some typos in comparison.md [\#38](https://github.com/Yelp/paasta/pull/38) ([nhandler](https://github.com/nhandler))
- Add a note about our IRC channel to the README [\#37](https://github.com/Yelp/paasta/pull/37) ([nhandler](https://github.com/nhandler))
- Add initial .travis.yml [\#36](https://github.com/Yelp/paasta/pull/36) ([nhandler](https://github.com/nhandler))
- Added more words and docker swarm comparison [\#34](https://github.com/Yelp/paasta/pull/34) ([solarkennedy](https://github.com/solarkennedy))
- delete unused wizard-y code [\#33](https://github.com/Yelp/paasta/pull/33) ([mrtyler](https://github.com/mrtyler))
- Remove deps [\#32](https://github.com/Yelp/paasta/pull/32) ([Rob-Johnson](https://github.com/Rob-Johnson))
- Sort jobs so that we really clean up old ones [\#31](https://github.com/Yelp/paasta/pull/31) ([mrtyler](https://github.com/mrtyler))
-  More comparison docs, added flynn [\#29](https://github.com/Yelp/paasta/pull/29) ([solarkennedy](https://github.com/solarkennedy))

## [v0.14.1](https://github.com/Yelp/paasta/tree/v0.14.1) (2015-11-10)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.13.11...v0.14.1)

**Closed issues:**

- paasta metastatus should only report "enabled" chronos jobs [\#25](https://github.com/Yelp/paasta/issues/25)

**Merged pull requests:**

- remove pgrp management; it doesn't work properly [\#28](https://github.com/Yelp/paasta/pull/28) ([Rob-Johnson](https://github.com/Rob-Johnson))
- only show enabled jobs in paasta metastatus + cleanup test mocks [\#27](https://github.com/Yelp/paasta/pull/27) ([Rob-Johnson](https://github.com/Rob-Johnson))
- Make paasta code / itests pass on servers with no paasta configuration [\#26](https://github.com/Yelp/paasta/pull/26) ([solarkennedy](https://github.com/solarkennedy))
- refactor lookup\_chronos\_jobs [\#24](https://github.com/Yelp/paasta/pull/24) ([mrtyler](https://github.com/mrtyler))
- remove remove\_tag\_from\_job\_id [\#22](https://github.com/Yelp/paasta/pull/22) ([mrtyler](https://github.com/mrtyler))
- Metastatus verbose mode [\#21](https://github.com/Yelp/paasta/pull/21) ([Rob-Johnson](https://github.com/Rob-Johnson))

## [v0.13.11](https://github.com/Yelp/paasta/tree/v0.13.11) (2015-11-05)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.13.10...v0.13.11)

**Merged pull requests:**

- Make tab completions work in zsh [\#19](https://github.com/Yelp/paasta/pull/19) ([nhandler](https://github.com/nhandler))
- Fix more tests to work on a non yelp dev box [\#18](https://github.com/Yelp/paasta/pull/18) ([solarkennedy](https://github.com/solarkennedy))
- Remove references to internal CEP/SCF stuff [\#17](https://github.com/Yelp/paasta/pull/17) ([solarkennedy](https://github.com/solarkennedy))
- Fix chronos bouncing to properly delete chronos jobs [\#16](https://github.com/Yelp/paasta/pull/16) ([solarkennedy](https://github.com/solarkennedy))
- sorting hat [\#15](https://github.com/Yelp/paasta/pull/15) ([mrtyler](https://github.com/mrtyler))
- Consolidate dockerfiles [\#14](https://github.com/Yelp/paasta/pull/14) ([solarkennedy](https://github.com/solarkennedy))

## [v0.13.10](https://github.com/Yelp/paasta/tree/v0.13.10) (2015-11-02)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.13.9...v0.13.10)

**Merged pull requests:**

- Added first pass at getting started doc [\#12](https://github.com/Yelp/paasta/pull/12) ([solarkennedy](https://github.com/solarkennedy))
- Use a glob to only read json files out of /etc/paasta/ [\#11](https://github.com/Yelp/paasta/pull/11) ([solarkennedy](https://github.com/solarkennedy))

## [v0.13.9](https://github.com/Yelp/paasta/tree/v0.13.9) (2015-11-02)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.13.8...v0.13.9)

**Merged pull requests:**

- Added changes to remove blacklisted locations from the GROUP\_BY value [\#8](https://github.com/Yelp/paasta/pull/8) ([davenonne](https://github.com/davenonne))

## [v0.13.8](https://github.com/Yelp/paasta/tree/v0.13.8) (2015-10-30)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.13.7...v0.13.8)

**Merged pull requests:**

- distribute the check\_chronos\_jobs script properly [\#10](https://github.com/Yelp/paasta/pull/10) ([Rob-Johnson](https://github.com/Rob-Johnson))
- Redo the chronos bounce code to be easier to read and actually clean up old jobs [\#7](https://github.com/Yelp/paasta/pull/7) ([solarkennedy](https://github.com/solarkennedy))

## [v0.13.7](https://github.com/Yelp/paasta/tree/v0.13.7) (2015-10-29)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.13.6...v0.13.7)

**Merged pull requests:**

- Update chronos\_serviceinit to use LastRunState [\#5](https://github.com/Yelp/paasta/pull/5) ([Rob-Johnson](https://github.com/Rob-Johnson))

## [v0.13.6](https://github.com/Yelp/paasta/tree/v0.13.6) (2015-10-28)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.13.5...v0.13.6)

**Merged pull requests:**

- Ensure chronos jobs have docker credentials to pull from the private registry [\#4](https://github.com/Yelp/paasta/pull/4) ([solarkennedy](https://github.com/solarkennedy))

## [v0.13.5](https://github.com/Yelp/paasta/tree/v0.13.5) (2015-10-27)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.13.4...v0.13.5)

**Merged pull requests:**

- Catching keyboard interrupt during cook-image build [\#2](https://github.com/Yelp/paasta/pull/2) ([zeldinha](https://github.com/zeldinha))

## [v0.13.4](https://github.com/Yelp/paasta/tree/v0.13.4) (2015-10-26)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.13.3...v0.13.4)

## [v0.13.3](https://github.com/Yelp/paasta/tree/v0.13.3) (2015-10-26)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.13.2...v0.13.3)

**Merged pull requests:**

- add check\_chronos\_jobs script [\#1](https://github.com/Yelp/paasta/pull/1) ([Rob-Johnson](https://github.com/Rob-Johnson))

## [v0.13.2](https://github.com/Yelp/paasta/tree/v0.13.2) (2015-10-23)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.13.1...v0.13.2)

## [v0.13.1](https://github.com/Yelp/paasta/tree/v0.13.1) (2015-10-23)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.12.99...v0.13.1)

## [v0.12.99](https://github.com/Yelp/paasta/tree/v0.12.99) (2015-10-16)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.12.98...v0.12.99)

## [v0.12.98](https://github.com/Yelp/paasta/tree/v0.12.98) (2015-10-16)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.12.97...v0.12.98)

## [v0.12.97](https://github.com/Yelp/paasta/tree/v0.12.97) (2015-10-15)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.12.96...v0.12.97)

## [v0.12.96](https://github.com/Yelp/paasta/tree/v0.12.96) (2015-10-14)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.12.95...v0.12.96)

## [v0.12.95](https://github.com/Yelp/paasta/tree/v0.12.95) (2015-10-13)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.12.94...v0.12.95)

## [v0.12.94](https://github.com/Yelp/paasta/tree/v0.12.94) (2015-10-13)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.12.93...v0.12.94)

## [v0.12.93](https://github.com/Yelp/paasta/tree/v0.12.93) (2015-10-10)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.12.92...v0.12.93)

## [v0.12.92](https://github.com/Yelp/paasta/tree/v0.12.92) (2015-10-09)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.12.91...v0.12.92)

## [v0.12.91](https://github.com/Yelp/paasta/tree/v0.12.91) (2015-10-09)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.12.90...v0.12.91)

## [v0.12.90](https://github.com/Yelp/paasta/tree/v0.12.90) (2015-10-09)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.12.89...v0.12.90)

## [v0.12.89](https://github.com/Yelp/paasta/tree/v0.12.89) (2015-10-08)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.12.88...v0.12.89)

## [v0.12.88](https://github.com/Yelp/paasta/tree/v0.12.88) (2015-10-07)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.12.87...v0.12.88)

## [v0.12.87](https://github.com/Yelp/paasta/tree/v0.12.87) (2015-09-29)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.12.86...v0.12.87)

## [v0.12.86](https://github.com/Yelp/paasta/tree/v0.12.86) (2015-09-29)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.12.85...v0.12.86)

## [v0.12.85](https://github.com/Yelp/paasta/tree/v0.12.85) (2015-09-29)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.12.84...v0.12.85)

## [v0.12.84](https://github.com/Yelp/paasta/tree/v0.12.84) (2015-09-24)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.12.82...v0.12.84)

## [v0.12.82](https://github.com/Yelp/paasta/tree/v0.12.82) (2015-09-24)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.12.81...v0.12.82)

## [v0.12.81](https://github.com/Yelp/paasta/tree/v0.12.81) (2015-09-23)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.12.80...v0.12.81)

## [v0.12.80](https://github.com/Yelp/paasta/tree/v0.12.80) (2015-09-23)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.12.79...v0.12.80)

## [v0.12.79](https://github.com/Yelp/paasta/tree/v0.12.79) (2015-09-21)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.12.78...v0.12.79)

## [v0.12.78](https://github.com/Yelp/paasta/tree/v0.12.78) (2015-09-21)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.12.77...v0.12.78)

## [v0.12.77](https://github.com/Yelp/paasta/tree/v0.12.77) (2015-09-18)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.12.76...v0.12.77)

## [v0.12.76](https://github.com/Yelp/paasta/tree/v0.12.76) (2015-09-15)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.12.75...v0.12.76)

## [v0.12.75](https://github.com/Yelp/paasta/tree/v0.12.75) (2015-09-14)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.12.73...v0.12.75)

## [v0.12.73](https://github.com/Yelp/paasta/tree/v0.12.73) (2015-09-14)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.12.72...v0.12.73)

## [v0.12.72](https://github.com/Yelp/paasta/tree/v0.12.72) (2015-09-12)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.12.71...v0.12.72)

## [v0.12.71](https://github.com/Yelp/paasta/tree/v0.12.71) (2015-09-10)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.12.70...v0.12.71)

## [v0.12.70](https://github.com/Yelp/paasta/tree/v0.12.70) (2015-09-10)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.12.69...v0.12.70)

## [v0.12.69](https://github.com/Yelp/paasta/tree/v0.12.69) (2015-09-09)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.12.68...v0.12.69)

## [v0.12.68](https://github.com/Yelp/paasta/tree/v0.12.68) (2015-09-08)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.12.67...v0.12.68)

## [v0.12.67](https://github.com/Yelp/paasta/tree/v0.12.67) (2015-09-04)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.12.66...v0.12.67)

## [v0.12.66](https://github.com/Yelp/paasta/tree/v0.12.66) (2015-09-04)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.12.65...v0.12.66)

## [v0.12.65](https://github.com/Yelp/paasta/tree/v0.12.65) (2015-09-04)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.12.64...v0.12.65)

## [v0.12.64](https://github.com/Yelp/paasta/tree/v0.12.64) (2015-09-02)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.12.63...v0.12.64)

## [v0.12.63](https://github.com/Yelp/paasta/tree/v0.12.63) (2015-09-02)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.12.62...v0.12.63)

## [v0.12.62](https://github.com/Yelp/paasta/tree/v0.12.62) (2015-08-31)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.12.61...v0.12.62)

## [v0.12.61](https://github.com/Yelp/paasta/tree/v0.12.61) (2015-08-28)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.12.60...v0.12.61)

## [v0.12.60](https://github.com/Yelp/paasta/tree/v0.12.60) (2015-08-28)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.12.59...v0.12.60)

## [v0.12.59](https://github.com/Yelp/paasta/tree/v0.12.59) (2015-08-28)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.12.58...v0.12.59)

## [v0.12.58](https://github.com/Yelp/paasta/tree/v0.12.58) (2015-08-26)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.12.57...v0.12.58)

## [v0.12.57](https://github.com/Yelp/paasta/tree/v0.12.57) (2015-08-20)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.12.56...v0.12.57)

## [v0.12.56](https://github.com/Yelp/paasta/tree/v0.12.56) (2015-08-19)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.12.55...v0.12.56)

## [v0.12.55](https://github.com/Yelp/paasta/tree/v0.12.55) (2015-08-18)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.12.54...v0.12.55)

## [v0.12.54](https://github.com/Yelp/paasta/tree/v0.12.54) (2015-08-17)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.12.53...v0.12.54)

## [v0.12.53](https://github.com/Yelp/paasta/tree/v0.12.53) (2015-08-17)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.12.52...v0.12.53)

## [v0.12.52](https://github.com/Yelp/paasta/tree/v0.12.52) (2015-08-14)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.12.51...v0.12.52)

## [v0.12.51](https://github.com/Yelp/paasta/tree/v0.12.51) (2015-08-14)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.12.50...v0.12.51)

## [v0.12.50](https://github.com/Yelp/paasta/tree/v0.12.50) (2015-08-14)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.12.49...v0.12.50)

## [v0.12.49](https://github.com/Yelp/paasta/tree/v0.12.49) (2015-08-14)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.12.48...v0.12.49)

## [v0.12.48](https://github.com/Yelp/paasta/tree/v0.12.48) (2015-08-14)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.12.47...v0.12.48)

## [v0.12.47](https://github.com/Yelp/paasta/tree/v0.12.47) (2015-08-12)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.12.46...v0.12.47)

## [v0.12.46](https://github.com/Yelp/paasta/tree/v0.12.46) (2015-08-11)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.12.45...v0.12.46)

## [v0.12.45](https://github.com/Yelp/paasta/tree/v0.12.45) (2015-08-10)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.12.44...v0.12.45)

## [v0.12.44](https://github.com/Yelp/paasta/tree/v0.12.44) (2015-08-04)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.12.43...v0.12.44)

## [v0.12.43](https://github.com/Yelp/paasta/tree/v0.12.43) (2015-08-04)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.12.42...v0.12.43)

## [v0.12.42](https://github.com/Yelp/paasta/tree/v0.12.42) (2015-07-31)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.12.41...v0.12.42)

## [v0.12.41](https://github.com/Yelp/paasta/tree/v0.12.41) (2015-07-30)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.12.40...v0.12.41)

## [v0.12.40](https://github.com/Yelp/paasta/tree/v0.12.40) (2015-07-29)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.12.39...v0.12.40)

## [v0.12.39](https://github.com/Yelp/paasta/tree/v0.12.39) (2015-07-29)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.12.38...v0.12.39)

## [v0.12.38](https://github.com/Yelp/paasta/tree/v0.12.38) (2015-07-29)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.12.37...v0.12.38)

## [v0.12.37](https://github.com/Yelp/paasta/tree/v0.12.37) (2015-07-29)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.12.36...v0.12.37)

## [v0.12.36](https://github.com/Yelp/paasta/tree/v0.12.36) (2015-07-29)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.12.35...v0.12.36)

## [v0.12.35](https://github.com/Yelp/paasta/tree/v0.12.35) (2015-07-28)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.12.34...v0.12.35)

## [v0.12.34](https://github.com/Yelp/paasta/tree/v0.12.34) (2015-07-27)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.12.33...v0.12.34)

## [v0.12.33](https://github.com/Yelp/paasta/tree/v0.12.33) (2015-07-27)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.12.32...v0.12.33)

## [v0.12.32](https://github.com/Yelp/paasta/tree/v0.12.32) (2015-07-24)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.12.31...v0.12.32)

## [v0.12.31](https://github.com/Yelp/paasta/tree/v0.12.31) (2015-07-23)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.12.30...v0.12.31)

## [v0.12.30](https://github.com/Yelp/paasta/tree/v0.12.30) (2015-07-23)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.12.29...v0.12.30)

## [v0.12.29](https://github.com/Yelp/paasta/tree/v0.12.29) (2015-07-23)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.12.28...v0.12.29)

## [v0.12.28](https://github.com/Yelp/paasta/tree/v0.12.28) (2015-07-23)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.12.27...v0.12.28)

## [v0.12.27](https://github.com/Yelp/paasta/tree/v0.12.27) (2015-07-23)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.12.26...v0.12.27)

## [v0.12.26](https://github.com/Yelp/paasta/tree/v0.12.26) (2015-07-22)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.12.25...v0.12.26)

## [v0.12.25](https://github.com/Yelp/paasta/tree/v0.12.25) (2015-07-22)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.12.24...v0.12.25)

## [v0.12.24](https://github.com/Yelp/paasta/tree/v0.12.24) (2015-07-20)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.12.23...v0.12.24)

## [v0.12.23](https://github.com/Yelp/paasta/tree/v0.12.23) (2015-07-17)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.12.22...v0.12.23)

## [v0.12.22](https://github.com/Yelp/paasta/tree/v0.12.22) (2015-07-17)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.12.21...v0.12.22)

## [v0.12.21](https://github.com/Yelp/paasta/tree/v0.12.21) (2015-07-16)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.12.20...v0.12.21)

## [v0.12.20](https://github.com/Yelp/paasta/tree/v0.12.20) (2015-07-16)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.12.19...v0.12.20)

## [v0.12.19](https://github.com/Yelp/paasta/tree/v0.12.19) (2015-07-15)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.12.18...v0.12.19)

## [v0.12.18](https://github.com/Yelp/paasta/tree/v0.12.18) (2015-07-14)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.12.17...v0.12.18)

## [v0.12.17](https://github.com/Yelp/paasta/tree/v0.12.17) (2015-07-09)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.12.16...v0.12.17)

## [v0.12.16](https://github.com/Yelp/paasta/tree/v0.12.16) (2015-07-08)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.12.15...v0.12.16)

## [v0.12.15](https://github.com/Yelp/paasta/tree/v0.12.15) (2015-07-07)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.12.14...v0.12.15)

## [v0.12.14](https://github.com/Yelp/paasta/tree/v0.12.14) (2015-07-07)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.12.13...v0.12.14)

## [v0.12.13](https://github.com/Yelp/paasta/tree/v0.12.13) (2015-07-07)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.12.12...v0.12.13)

## [v0.12.12](https://github.com/Yelp/paasta/tree/v0.12.12) (2015-07-03)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.12.11...v0.12.12)

## [v0.12.11](https://github.com/Yelp/paasta/tree/v0.12.11) (2015-07-02)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.12.10...v0.12.11)

## [v0.12.10](https://github.com/Yelp/paasta/tree/v0.12.10) (2015-07-02)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.12.9...v0.12.10)

## [v0.12.9](https://github.com/Yelp/paasta/tree/v0.12.9) (2015-07-02)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.12.8...v0.12.9)

## [v0.12.8](https://github.com/Yelp/paasta/tree/v0.12.8) (2015-07-02)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.12.7...v0.12.8)

## [v0.12.7](https://github.com/Yelp/paasta/tree/v0.12.7) (2015-07-02)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.12.6...v0.12.7)

## [v0.12.6](https://github.com/Yelp/paasta/tree/v0.12.6) (2015-07-01)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.12.5...v0.12.6)

## [v0.12.5](https://github.com/Yelp/paasta/tree/v0.12.5) (2015-06-30)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.12.4...v0.12.5)

## [v0.12.4](https://github.com/Yelp/paasta/tree/v0.12.4) (2015-06-30)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.12.3...v0.12.4)

## [v0.12.3](https://github.com/Yelp/paasta/tree/v0.12.3) (2015-06-27)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.12.2...v0.12.3)

## [v0.12.2](https://github.com/Yelp/paasta/tree/v0.12.2) (2015-06-26)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.12.1...v0.12.2)

## [v0.12.1](https://github.com/Yelp/paasta/tree/v0.12.1) (2015-06-25)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.11.24...v0.12.1)

## [v0.11.24](https://github.com/Yelp/paasta/tree/v0.11.24) (2015-06-24)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.11.23...v0.11.24)

## [v0.11.23](https://github.com/Yelp/paasta/tree/v0.11.23) (2015-06-23)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.11.22...v0.11.23)

## [v0.11.22](https://github.com/Yelp/paasta/tree/v0.11.22) (2015-06-23)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.11.21...v0.11.22)

## [v0.11.21](https://github.com/Yelp/paasta/tree/v0.11.21) (2015-06-22)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.11.20...v0.11.21)

## [v0.11.20](https://github.com/Yelp/paasta/tree/v0.11.20) (2015-06-20)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.11.19...v0.11.20)

## [v0.11.19](https://github.com/Yelp/paasta/tree/v0.11.19) (2015-06-19)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.11.18...v0.11.19)

## [v0.11.18](https://github.com/Yelp/paasta/tree/v0.11.18) (2015-06-18)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.11.17...v0.11.18)

## [v0.11.17](https://github.com/Yelp/paasta/tree/v0.11.17) (2015-06-18)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.11.16...v0.11.17)

## [v0.11.16](https://github.com/Yelp/paasta/tree/v0.11.16) (2015-06-18)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.11.15...v0.11.16)

## [v0.11.15](https://github.com/Yelp/paasta/tree/v0.11.15) (2015-06-17)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.11.14...v0.11.15)

## [v0.11.14](https://github.com/Yelp/paasta/tree/v0.11.14) (2015-06-17)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.11.13...v0.11.14)

## [v0.11.13](https://github.com/Yelp/paasta/tree/v0.11.13) (2015-06-17)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.11.12...v0.11.13)

## [v0.11.12](https://github.com/Yelp/paasta/tree/v0.11.12) (2015-06-16)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.11.11...v0.11.12)

## [v0.11.11](https://github.com/Yelp/paasta/tree/v0.11.11) (2015-06-09)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.11.10...v0.11.11)

## [v0.11.10](https://github.com/Yelp/paasta/tree/v0.11.10) (2015-06-09)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.11.9...v0.11.10)

## [v0.11.9](https://github.com/Yelp/paasta/tree/v0.11.9) (2015-06-04)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.11.8...v0.11.9)

## [v0.11.8](https://github.com/Yelp/paasta/tree/v0.11.8) (2015-06-03)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.11.7...v0.11.8)

## [v0.11.7](https://github.com/Yelp/paasta/tree/v0.11.7) (2015-06-03)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.11.6...v0.11.7)

## [v0.11.6](https://github.com/Yelp/paasta/tree/v0.11.6) (2015-06-03)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.11.5...v0.11.6)

## [v0.11.5](https://github.com/Yelp/paasta/tree/v0.11.5) (2015-06-02)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.11.4...v0.11.5)

## [v0.11.4](https://github.com/Yelp/paasta/tree/v0.11.4) (2015-06-02)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.11.3...v0.11.4)

## [v0.11.3](https://github.com/Yelp/paasta/tree/v0.11.3) (2015-06-02)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.11.2...v0.11.3)

## [v0.11.2](https://github.com/Yelp/paasta/tree/v0.11.2) (2015-06-02)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.11.1...v0.11.2)

## [v0.11.1](https://github.com/Yelp/paasta/tree/v0.11.1) (2015-05-29)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.11.0...v0.11.1)

## [v0.11.0](https://github.com/Yelp/paasta/tree/v0.11.0) (2015-05-29)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.10.4...v0.11.0)

## [v0.10.4](https://github.com/Yelp/paasta/tree/v0.10.4) (2015-05-27)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.10.3...v0.10.4)

## [v0.10.3](https://github.com/Yelp/paasta/tree/v0.10.3) (2015-05-26)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.10.2...v0.10.3)

## [v0.10.2](https://github.com/Yelp/paasta/tree/v0.10.2) (2015-05-22)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.10.1...v0.10.2)

## [v0.10.1](https://github.com/Yelp/paasta/tree/v0.10.1) (2015-05-22)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.9.73...v0.10.1)

## [v0.9.73](https://github.com/Yelp/paasta/tree/v0.9.73) (2015-05-21)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.9.72...v0.9.73)

## [v0.9.72](https://github.com/Yelp/paasta/tree/v0.9.72) (2015-05-21)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.9.71...v0.9.72)

## [v0.9.71](https://github.com/Yelp/paasta/tree/v0.9.71) (2015-05-21)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.9.70...v0.9.71)

## [v0.9.70](https://github.com/Yelp/paasta/tree/v0.9.70) (2015-05-14)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.9.69...v0.9.70)

## [v0.9.69](https://github.com/Yelp/paasta/tree/v0.9.69) (2015-05-12)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.9.68...v0.9.69)

## [v0.9.68](https://github.com/Yelp/paasta/tree/v0.9.68) (2015-05-11)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.9.67...v0.9.68)

## [v0.9.67](https://github.com/Yelp/paasta/tree/v0.9.67) (2015-05-11)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.9.66...v0.9.67)

## [v0.9.66](https://github.com/Yelp/paasta/tree/v0.9.66) (2015-05-11)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.9.65...v0.9.66)

## [v0.9.65](https://github.com/Yelp/paasta/tree/v0.9.65) (2015-05-08)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.9.64...v0.9.65)

## [v0.9.64](https://github.com/Yelp/paasta/tree/v0.9.64) (2015-05-07)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.9.63...v0.9.64)

## [v0.9.63](https://github.com/Yelp/paasta/tree/v0.9.63) (2015-05-06)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.9.62...v0.9.63)

## [v0.9.62](https://github.com/Yelp/paasta/tree/v0.9.62) (2015-05-06)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.9.61...v0.9.62)

## [v0.9.61](https://github.com/Yelp/paasta/tree/v0.9.61) (2015-05-06)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.9.60...v0.9.61)

## [v0.9.60](https://github.com/Yelp/paasta/tree/v0.9.60) (2015-05-05)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.9.59...v0.9.60)

## [v0.9.59](https://github.com/Yelp/paasta/tree/v0.9.59) (2015-05-05)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.9.58...v0.9.59)

## [v0.9.58](https://github.com/Yelp/paasta/tree/v0.9.58) (2015-05-05)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.9.57...v0.9.58)

## [v0.9.57](https://github.com/Yelp/paasta/tree/v0.9.57) (2015-05-04)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.9.56...v0.9.57)

## [v0.9.56](https://github.com/Yelp/paasta/tree/v0.9.56) (2015-05-01)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.9.55...v0.9.56)

## [v0.9.55](https://github.com/Yelp/paasta/tree/v0.9.55) (2015-05-01)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.9.54...v0.9.55)

## [v0.9.54](https://github.com/Yelp/paasta/tree/v0.9.54) (2015-05-01)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.9.53...v0.9.54)

## [v0.9.53](https://github.com/Yelp/paasta/tree/v0.9.53) (2015-04-30)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.9.52...v0.9.53)

## [v0.9.52](https://github.com/Yelp/paasta/tree/v0.9.52) (2015-04-29)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.9.51...v0.9.52)

## [v0.9.51](https://github.com/Yelp/paasta/tree/v0.9.51) (2015-04-29)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.9.50...v0.9.51)

## [v0.9.50](https://github.com/Yelp/paasta/tree/v0.9.50) (2015-04-23)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.9.49...v0.9.50)

## [v0.9.49](https://github.com/Yelp/paasta/tree/v0.9.49) (2015-04-16)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.9.48...v0.9.49)

## [v0.9.48](https://github.com/Yelp/paasta/tree/v0.9.48) (2015-04-15)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.9.47...v0.9.48)

## [v0.9.47](https://github.com/Yelp/paasta/tree/v0.9.47) (2015-04-13)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.9.46...v0.9.47)

## [v0.9.46](https://github.com/Yelp/paasta/tree/v0.9.46) (2015-04-10)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.9.45...v0.9.46)

## [v0.9.45](https://github.com/Yelp/paasta/tree/v0.9.45) (2015-04-10)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.9.44...v0.9.45)

## [v0.9.44](https://github.com/Yelp/paasta/tree/v0.9.44) (2015-04-06)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.9.43...v0.9.44)

## [v0.9.43](https://github.com/Yelp/paasta/tree/v0.9.43) (2015-04-02)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.9.42...v0.9.43)

## [v0.9.42](https://github.com/Yelp/paasta/tree/v0.9.42) (2015-04-02)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.9.41...v0.9.42)

## [v0.9.41](https://github.com/Yelp/paasta/tree/v0.9.41) (2015-03-31)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.9.40...v0.9.41)

## [v0.9.40](https://github.com/Yelp/paasta/tree/v0.9.40) (2015-03-31)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.9.38...v0.9.40)

## [v0.9.38](https://github.com/Yelp/paasta/tree/v0.9.38) (2015-03-25)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.9.37...v0.9.38)

## [v0.9.37](https://github.com/Yelp/paasta/tree/v0.9.37) (2015-03-23)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.9.36...v0.9.37)

## [v0.9.36](https://github.com/Yelp/paasta/tree/v0.9.36) (2015-03-23)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.9.35...v0.9.36)

## [v0.9.35](https://github.com/Yelp/paasta/tree/v0.9.35) (2015-03-20)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.9.34...v0.9.35)

## [v0.9.34](https://github.com/Yelp/paasta/tree/v0.9.34) (2015-03-20)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.9.33...v0.9.34)

## [v0.9.33](https://github.com/Yelp/paasta/tree/v0.9.33) (2015-03-20)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.9.32...v0.9.33)

## [v0.9.32](https://github.com/Yelp/paasta/tree/v0.9.32) (2015-03-20)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.9.31...v0.9.32)

## [v0.9.31](https://github.com/Yelp/paasta/tree/v0.9.31) (2015-03-20)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.9.30...v0.9.31)

## [v0.9.30](https://github.com/Yelp/paasta/tree/v0.9.30) (2015-03-20)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.9.29...v0.9.30)

## [v0.9.29](https://github.com/Yelp/paasta/tree/v0.9.29) (2015-03-19)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.9.28...v0.9.29)

## [v0.9.28](https://github.com/Yelp/paasta/tree/v0.9.28) (2015-03-19)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.9.27...v0.9.28)

## [v0.9.27](https://github.com/Yelp/paasta/tree/v0.9.27) (2015-03-19)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.9.26...v0.9.27)

## [v0.9.26](https://github.com/Yelp/paasta/tree/v0.9.26) (2015-03-19)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.9.24...v0.9.26)

## [v0.9.24](https://github.com/Yelp/paasta/tree/v0.9.24) (2015-03-18)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.9.23...v0.9.24)

## [v0.9.23](https://github.com/Yelp/paasta/tree/v0.9.23) (2015-03-18)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.9.22...v0.9.23)

## [v0.9.22](https://github.com/Yelp/paasta/tree/v0.9.22) (2015-03-17)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.9.21...v0.9.22)

## [v0.9.21](https://github.com/Yelp/paasta/tree/v0.9.21) (2015-03-17)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.9.20...v0.9.21)

## [v0.9.20](https://github.com/Yelp/paasta/tree/v0.9.20) (2015-03-17)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.9.19...v0.9.20)

## [v0.9.19](https://github.com/Yelp/paasta/tree/v0.9.19) (2015-03-13)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.9.18...v0.9.19)

## [v0.9.18](https://github.com/Yelp/paasta/tree/v0.9.18) (2015-03-12)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.9.17...v0.9.18)

## [v0.9.17](https://github.com/Yelp/paasta/tree/v0.9.17) (2015-03-12)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.9.16...v0.9.17)

## [v0.9.16](https://github.com/Yelp/paasta/tree/v0.9.16) (2015-03-11)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.9.15...v0.9.16)

## [v0.9.15](https://github.com/Yelp/paasta/tree/v0.9.15) (2015-03-11)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.9.14...v0.9.15)

## [v0.9.14](https://github.com/Yelp/paasta/tree/v0.9.14) (2015-03-10)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.9.13...v0.9.14)

## [v0.9.13](https://github.com/Yelp/paasta/tree/v0.9.13) (2015-03-03)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.9.12...v0.9.13)

## [v0.9.12](https://github.com/Yelp/paasta/tree/v0.9.12) (2015-03-03)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.9.11...v0.9.12)

## [v0.9.11](https://github.com/Yelp/paasta/tree/v0.9.11) (2015-03-02)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.9.9...v0.9.11)

## [v0.9.9](https://github.com/Yelp/paasta/tree/v0.9.9) (2015-02-27)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.9.8...v0.9.9)

## [v0.9.8](https://github.com/Yelp/paasta/tree/v0.9.8) (2015-02-24)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.9.7...v0.9.8)

## [v0.9.7](https://github.com/Yelp/paasta/tree/v0.9.7) (2015-02-24)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.9.6...v0.9.7)

## [v0.9.6](https://github.com/Yelp/paasta/tree/v0.9.6) (2015-02-20)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.9.5...v0.9.6)

## [v0.9.5](https://github.com/Yelp/paasta/tree/v0.9.5) (2015-02-20)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.9.4...v0.9.5)

## [v0.9.4](https://github.com/Yelp/paasta/tree/v0.9.4) (2015-02-20)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.9.3...v0.9.4)

## [v0.9.3](https://github.com/Yelp/paasta/tree/v0.9.3) (2015-02-19)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.9.2...v0.9.3)

## [v0.9.2](https://github.com/Yelp/paasta/tree/v0.9.2) (2015-02-19)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.9.1...v0.9.2)

## [v0.9.1](https://github.com/Yelp/paasta/tree/v0.9.1) (2015-02-19)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.9.0...v0.9.1)

## [v0.9.0](https://github.com/Yelp/paasta/tree/v0.9.0) (2015-02-19)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.8.36...v0.9.0)

## [v0.8.36](https://github.com/Yelp/paasta/tree/v0.8.36) (2015-02-19)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.8.35...v0.8.36)

## [v0.8.35](https://github.com/Yelp/paasta/tree/v0.8.35) (2015-02-18)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.8.34...v0.8.35)

## [v0.8.34](https://github.com/Yelp/paasta/tree/v0.8.34) (2015-02-13)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.8.33...v0.8.34)

## [v0.8.33](https://github.com/Yelp/paasta/tree/v0.8.33) (2015-02-12)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.8.32...v0.8.33)

## [v0.8.32](https://github.com/Yelp/paasta/tree/v0.8.32) (2015-02-11)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.8.31...v0.8.32)

## [v0.8.31](https://github.com/Yelp/paasta/tree/v0.8.31) (2015-02-10)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.8.30...v0.8.31)

## [v0.8.30](https://github.com/Yelp/paasta/tree/v0.8.30) (2015-02-10)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.8.29...v0.8.30)

## [v0.8.29](https://github.com/Yelp/paasta/tree/v0.8.29) (2015-02-09)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.8.28...v0.8.29)

## [v0.8.28](https://github.com/Yelp/paasta/tree/v0.8.28) (2015-02-06)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.8.27...v0.8.28)

## [v0.8.27](https://github.com/Yelp/paasta/tree/v0.8.27) (2015-02-06)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.8.26...v0.8.27)

## [v0.8.26](https://github.com/Yelp/paasta/tree/v0.8.26) (2015-02-05)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.8.25...v0.8.26)

## [v0.8.25](https://github.com/Yelp/paasta/tree/v0.8.25) (2015-02-05)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.8.24...v0.8.25)

## [v0.8.24](https://github.com/Yelp/paasta/tree/v0.8.24) (2015-02-03)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.8.23...v0.8.24)

## [v0.8.23](https://github.com/Yelp/paasta/tree/v0.8.23) (2015-02-02)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.8.22...v0.8.23)

## [v0.8.22](https://github.com/Yelp/paasta/tree/v0.8.22) (2015-01-31)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.8.21...v0.8.22)

## [v0.8.21](https://github.com/Yelp/paasta/tree/v0.8.21) (2015-01-28)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.8.20...v0.8.21)

## [v0.8.20](https://github.com/Yelp/paasta/tree/v0.8.20) (2015-01-27)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.8.19...v0.8.20)

## [v0.8.19](https://github.com/Yelp/paasta/tree/v0.8.19) (2015-01-27)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.8.18...v0.8.19)

## [v0.8.18](https://github.com/Yelp/paasta/tree/v0.8.18) (2015-01-23)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.8.17...v0.8.18)

## [v0.8.17](https://github.com/Yelp/paasta/tree/v0.8.17) (2015-01-23)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.8.16...v0.8.17)

## [v0.8.16](https://github.com/Yelp/paasta/tree/v0.8.16) (2015-01-23)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.8.15...v0.8.16)

## [v0.8.15](https://github.com/Yelp/paasta/tree/v0.8.15) (2015-01-22)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.8.14...v0.8.15)

## [v0.8.14](https://github.com/Yelp/paasta/tree/v0.8.14) (2015-01-16)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.8.13...v0.8.14)

## [v0.8.13](https://github.com/Yelp/paasta/tree/v0.8.13) (2015-01-15)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.8.12...v0.8.13)

## [v0.8.12](https://github.com/Yelp/paasta/tree/v0.8.12) (2015-01-15)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.8.11...v0.8.12)

## [v0.8.11](https://github.com/Yelp/paasta/tree/v0.8.11) (2015-01-14)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.0.1...v0.8.11)

## [v0.0.1](https://github.com/Yelp/paasta/tree/v0.0.1) (2015-01-13)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.8.10...v0.0.1)

## [v0.8.10](https://github.com/Yelp/paasta/tree/v0.8.10) (2015-01-10)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.8.9...v0.8.10)

## [v0.8.9](https://github.com/Yelp/paasta/tree/v0.8.9) (2015-01-10)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.8.8...v0.8.9)

## [v0.8.8](https://github.com/Yelp/paasta/tree/v0.8.8) (2015-01-10)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.8.7...v0.8.8)

## [v0.8.7](https://github.com/Yelp/paasta/tree/v0.8.7) (2015-01-09)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.8.6...v0.8.7)

## [v0.8.6](https://github.com/Yelp/paasta/tree/v0.8.6) (2015-01-09)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.8.5...v0.8.6)

## [v0.8.5](https://github.com/Yelp/paasta/tree/v0.8.5) (2015-01-09)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.8.4...v0.8.5)

## [v0.8.4](https://github.com/Yelp/paasta/tree/v0.8.4) (2015-01-08)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.8.3...v0.8.4)

## [v0.8.3](https://github.com/Yelp/paasta/tree/v0.8.3) (2015-01-08)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.8.2...v0.8.3)

## [v0.8.2](https://github.com/Yelp/paasta/tree/v0.8.2) (2015-01-08)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.8.1...v0.8.2)

## [v0.8.1](https://github.com/Yelp/paasta/tree/v0.8.1) (2015-01-08)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.8.0...v0.8.1)

## [v0.8.0](https://github.com/Yelp/paasta/tree/v0.8.0) (2015-01-07)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.7.40...v0.8.0)

## [v0.7.40](https://github.com/Yelp/paasta/tree/v0.7.40) (2015-01-07)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.7.39...v0.7.40)

## [v0.7.39](https://github.com/Yelp/paasta/tree/v0.7.39) (2015-01-06)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.7.38...v0.7.39)

## [v0.7.38](https://github.com/Yelp/paasta/tree/v0.7.38) (2015-01-06)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.7.37...v0.7.38)

## [v0.7.37](https://github.com/Yelp/paasta/tree/v0.7.37) (2015-01-06)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.7.36...v0.7.37)

## [v0.7.36](https://github.com/Yelp/paasta/tree/v0.7.36) (2015-01-06)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.7.35...v0.7.36)

## [v0.7.35](https://github.com/Yelp/paasta/tree/v0.7.35) (2015-01-05)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.7.34...v0.7.35)

## [v0.7.34](https://github.com/Yelp/paasta/tree/v0.7.34) (2014-12-30)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.7.33...v0.7.34)

## [v0.7.33](https://github.com/Yelp/paasta/tree/v0.7.33) (2014-12-30)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.7.32...v0.7.33)

## [v0.7.32](https://github.com/Yelp/paasta/tree/v0.7.32) (2014-12-30)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.7.31...v0.7.32)

## [v0.7.31](https://github.com/Yelp/paasta/tree/v0.7.31) (2014-12-29)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.7.30...v0.7.31)

## [v0.7.30](https://github.com/Yelp/paasta/tree/v0.7.30) (2014-12-29)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.7.29...v0.7.30)

## [v0.7.29](https://github.com/Yelp/paasta/tree/v0.7.29) (2014-12-29)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.7.28...v0.7.29)

## [v0.7.28](https://github.com/Yelp/paasta/tree/v0.7.28) (2014-12-24)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.7.27...v0.7.28)

## [v0.7.27](https://github.com/Yelp/paasta/tree/v0.7.27) (2014-12-23)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.7.25...v0.7.27)

## [v0.7.25](https://github.com/Yelp/paasta/tree/v0.7.25) (2014-12-18)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.7.23...v0.7.25)

## [v0.7.23](https://github.com/Yelp/paasta/tree/v0.7.23) (2014-12-17)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.7.22...v0.7.23)

## [v0.7.22](https://github.com/Yelp/paasta/tree/v0.7.22) (2014-12-17)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.7.21...v0.7.22)

## [v0.7.21](https://github.com/Yelp/paasta/tree/v0.7.21) (2014-12-17)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.7.20...v0.7.21)

## [v0.7.20](https://github.com/Yelp/paasta/tree/v0.7.20) (2014-12-16)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.7.19...v0.7.20)

## [v0.7.19](https://github.com/Yelp/paasta/tree/v0.7.19) (2014-12-16)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.7.18...v0.7.19)

## [v0.7.18](https://github.com/Yelp/paasta/tree/v0.7.18) (2014-12-13)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.7.17...v0.7.18)

## [v0.7.17](https://github.com/Yelp/paasta/tree/v0.7.17) (2014-12-13)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.7.16...v0.7.17)

## [v0.7.16](https://github.com/Yelp/paasta/tree/v0.7.16) (2014-12-11)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.7.15...v0.7.16)

## [v0.7.15](https://github.com/Yelp/paasta/tree/v0.7.15) (2014-12-10)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.7.14...v0.7.15)

## [v0.7.14](https://github.com/Yelp/paasta/tree/v0.7.14) (2014-12-10)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.7.13...v0.7.14)

## [v0.7.13](https://github.com/Yelp/paasta/tree/v0.7.13) (2014-12-09)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.7.11...v0.7.13)

## [v0.7.11](https://github.com/Yelp/paasta/tree/v0.7.11) (2014-12-08)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.7.10...v0.7.11)

## [v0.7.10](https://github.com/Yelp/paasta/tree/v0.7.10) (2014-12-05)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.7.9...v0.7.10)

## [v0.7.9](https://github.com/Yelp/paasta/tree/v0.7.9) (2014-12-05)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.7.8...v0.7.9)

## [v0.7.8](https://github.com/Yelp/paasta/tree/v0.7.8) (2014-12-04)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.7.7...v0.7.8)

## [v0.7.7](https://github.com/Yelp/paasta/tree/v0.7.7) (2014-12-04)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.7.6...v0.7.7)

## [v0.7.6](https://github.com/Yelp/paasta/tree/v0.7.6) (2014-12-04)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.7.5...v0.7.6)

## [v0.7.5](https://github.com/Yelp/paasta/tree/v0.7.5) (2014-12-04)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.7.4...v0.7.5)

## [v0.7.4](https://github.com/Yelp/paasta/tree/v0.7.4) (2014-12-04)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.7.3...v0.7.4)

## [v0.7.3](https://github.com/Yelp/paasta/tree/v0.7.3) (2014-12-02)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.7.2...v0.7.3)

## [v0.7.2](https://github.com/Yelp/paasta/tree/v0.7.2) (2014-12-01)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.7.1...v0.7.2)

## [v0.7.1](https://github.com/Yelp/paasta/tree/v0.7.1) (2014-11-24)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.6.7...v0.7.1)

## [v0.6.7](https://github.com/Yelp/paasta/tree/v0.6.7) (2014-11-19)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.6.6...v0.6.7)

## [v0.6.6](https://github.com/Yelp/paasta/tree/v0.6.6) (2014-11-18)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.6.5...v0.6.6)

## [v0.6.5](https://github.com/Yelp/paasta/tree/v0.6.5) (2014-11-18)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.6.4...v0.6.5)

## [v0.6.4](https://github.com/Yelp/paasta/tree/v0.6.4) (2014-10-29)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.6.3...v0.6.4)

## [v0.6.3](https://github.com/Yelp/paasta/tree/v0.6.3) (2014-10-23)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.6.2...v0.6.3)

## [v0.6.2](https://github.com/Yelp/paasta/tree/v0.6.2) (2014-10-23)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.6.1...v0.6.2)

## [v0.6.1](https://github.com/Yelp/paasta/tree/v0.6.1) (2014-10-23)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.6.0...v0.6.1)

## [v0.6.0](https://github.com/Yelp/paasta/tree/v0.6.0) (2014-10-23)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.5.0...v0.6.0)

## [v0.5.0](https://github.com/Yelp/paasta/tree/v0.5.0) (2014-10-21)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.4.9...v0.5.0)

## [v0.4.9](https://github.com/Yelp/paasta/tree/v0.4.9) (2014-10-14)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.4.8...v0.4.9)

## [v0.4.8](https://github.com/Yelp/paasta/tree/v0.4.8) (2014-10-14)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.4.7...v0.4.8)

## [v0.4.7](https://github.com/Yelp/paasta/tree/v0.4.7) (2014-10-10)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.4.6...v0.4.7)

## [v0.4.6](https://github.com/Yelp/paasta/tree/v0.4.6) (2014-10-09)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.4.5...v0.4.6)

## [v0.4.5](https://github.com/Yelp/paasta/tree/v0.4.5) (2014-10-08)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.4.4...v0.4.5)

## [v0.4.4](https://github.com/Yelp/paasta/tree/v0.4.4) (2014-10-07)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.4.3...v0.4.4)

## [v0.4.3](https://github.com/Yelp/paasta/tree/v0.4.3) (2014-10-02)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.4.2...v0.4.3)

## [v0.4.2](https://github.com/Yelp/paasta/tree/v0.4.2) (2014-09-18)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.4.1...v0.4.2)

## [v0.4.1](https://github.com/Yelp/paasta/tree/v0.4.1) (2014-09-02)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.4.0...v0.4.1)

## [v0.4.0](https://github.com/Yelp/paasta/tree/v0.4.0) (2014-08-28)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.3.5...v0.4.0)

## [v0.3.5](https://github.com/Yelp/paasta/tree/v0.3.5) (2014-08-26)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.3.4...v0.3.5)

## [v0.3.4](https://github.com/Yelp/paasta/tree/v0.3.4) (2014-08-19)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.3.3...v0.3.4)

## [v0.3.3](https://github.com/Yelp/paasta/tree/v0.3.3) (2014-08-18)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.3.2...v0.3.3)

## [v0.3.2](https://github.com/Yelp/paasta/tree/v0.3.2) (2014-08-17)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.3.1...v0.3.2)

## [v0.3.1](https://github.com/Yelp/paasta/tree/v0.3.1) (2014-08-17)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.3.0...v0.3.1)

## [v0.3.0](https://github.com/Yelp/paasta/tree/v0.3.0) (2014-08-15)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.2.14...v0.3.0)

## [v0.2.14](https://github.com/Yelp/paasta/tree/v0.2.14) (2014-08-15)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.2.13...v0.2.14)

## [v0.2.13](https://github.com/Yelp/paasta/tree/v0.2.13) (2014-08-15)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.2.12...v0.2.13)

## [v0.2.12](https://github.com/Yelp/paasta/tree/v0.2.12) (2014-08-15)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.2.11...v0.2.12)

## [v0.2.11](https://github.com/Yelp/paasta/tree/v0.2.11) (2014-08-14)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.2.10...v0.2.11)

## [v0.2.10](https://github.com/Yelp/paasta/tree/v0.2.10) (2014-08-14)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.2.8...v0.2.10)

## [v0.2.8](https://github.com/Yelp/paasta/tree/v0.2.8) (2014-08-13)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.2.7...v0.2.8)

## [v0.2.7](https://github.com/Yelp/paasta/tree/v0.2.7) (2014-08-13)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.2.6...v0.2.7)

## [v0.2.6](https://github.com/Yelp/paasta/tree/v0.2.6) (2014-08-12)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.2.5...v0.2.6)

## [v0.2.5](https://github.com/Yelp/paasta/tree/v0.2.5) (2014-08-09)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.2.4...v0.2.5)

## [v0.2.4](https://github.com/Yelp/paasta/tree/v0.2.4) (2014-08-09)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.2.3...v0.2.4)

## [v0.2.3](https://github.com/Yelp/paasta/tree/v0.2.3) (2014-08-08)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.2.2...v0.2.3)

## [v0.2.2](https://github.com/Yelp/paasta/tree/v0.2.2) (2014-08-08)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.2.1...v0.2.2)

## [v0.2.1](https://github.com/Yelp/paasta/tree/v0.2.1) (2014-08-02)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.2.0...v0.2.1)

## [v0.2.0](https://github.com/Yelp/paasta/tree/v0.2.0) (2014-08-01)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.1.31...v0.2.0)

## [v0.1.31](https://github.com/Yelp/paasta/tree/v0.1.31) (2014-08-01)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.1.30...v0.1.31)

## [v0.1.30](https://github.com/Yelp/paasta/tree/v0.1.30) (2014-08-01)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.1.29...v0.1.30)

## [v0.1.29](https://github.com/Yelp/paasta/tree/v0.1.29) (2014-08-01)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.1.28...v0.1.29)

## [v0.1.28](https://github.com/Yelp/paasta/tree/v0.1.28) (2014-08-01)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.1.27...v0.1.28)

## [v0.1.27](https://github.com/Yelp/paasta/tree/v0.1.27) (2014-07-31)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.1.26...v0.1.27)

## [v0.1.26](https://github.com/Yelp/paasta/tree/v0.1.26) (2014-07-31)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.1.25...v0.1.26)

## [v0.1.25](https://github.com/Yelp/paasta/tree/v0.1.25) (2014-07-31)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.1.24...v0.1.25)

## [v0.1.24](https://github.com/Yelp/paasta/tree/v0.1.24) (2014-07-30)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.1.23...v0.1.24)

## [v0.1.23](https://github.com/Yelp/paasta/tree/v0.1.23) (2014-07-30)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.1.22...v0.1.23)

## [v0.1.22](https://github.com/Yelp/paasta/tree/v0.1.22) (2014-07-29)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.1.21...v0.1.22)

## [v0.1.21](https://github.com/Yelp/paasta/tree/v0.1.21) (2014-07-29)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.1.20...v0.1.21)

## [v0.1.20](https://github.com/Yelp/paasta/tree/v0.1.20) (2014-07-28)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.1.19...v0.1.20)

## [v0.1.19](https://github.com/Yelp/paasta/tree/v0.1.19) (2014-07-28)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.1.18...v0.1.19)

## [v0.1.18](https://github.com/Yelp/paasta/tree/v0.1.18) (2014-07-25)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.1.17...v0.1.18)

## [v0.1.17](https://github.com/Yelp/paasta/tree/v0.1.17) (2014-07-24)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.1.16...v0.1.17)

## [v0.1.16](https://github.com/Yelp/paasta/tree/v0.1.16) (2014-07-24)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.1.15...v0.1.16)

## [v0.1.15](https://github.com/Yelp/paasta/tree/v0.1.15) (2014-07-23)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.1.14...v0.1.15)

## [v0.1.14](https://github.com/Yelp/paasta/tree/v0.1.14) (2014-07-23)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.1.13...v0.1.14)

## [v0.1.13](https://github.com/Yelp/paasta/tree/v0.1.13) (2014-07-22)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.1.12...v0.1.13)

## [v0.1.12](https://github.com/Yelp/paasta/tree/v0.1.12) (2014-07-22)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.1.11...v0.1.12)

## [v0.1.11](https://github.com/Yelp/paasta/tree/v0.1.11) (2014-07-22)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.1.10...v0.1.11)

## [v0.1.10](https://github.com/Yelp/paasta/tree/v0.1.10) (2014-07-22)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.1.9...v0.1.10)

## [v0.1.9](https://github.com/Yelp/paasta/tree/v0.1.9) (2014-07-22)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.1.8...v0.1.9)

## [v0.1.8](https://github.com/Yelp/paasta/tree/v0.1.8) (2014-07-22)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.1.7...v0.1.8)

## [v0.1.7](https://github.com/Yelp/paasta/tree/v0.1.7) (2014-07-22)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.1.6...v0.1.7)

## [v0.1.6](https://github.com/Yelp/paasta/tree/v0.1.6) (2014-07-21)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.1.5...v0.1.6)

## [v0.1.5](https://github.com/Yelp/paasta/tree/v0.1.5) (2014-07-18)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.1.4...v0.1.5)

## [v0.1.4](https://github.com/Yelp/paasta/tree/v0.1.4) (2014-07-17)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.1.3...v0.1.4)

## [v0.1.3](https://github.com/Yelp/paasta/tree/v0.1.3) (2014-07-16)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.1.2...v0.1.3)

## [v0.1.2](https://github.com/Yelp/paasta/tree/v0.1.2) (2014-07-15)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.1.1...v0.1.2)

## [v0.1.1](https://github.com/Yelp/paasta/tree/v0.1.1) (2014-07-15)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.0.2...v0.1.1)

## [v0.0.2](https://github.com/Yelp/paasta/tree/v0.0.2) (2014-07-07)


\* *This Change Log was automatically generated by [github_changelog_generator](https://github.com/skywinder/Github-Changelog-Generator)*