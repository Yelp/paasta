# Change Log

## [v0.67.5](https://github.com/Yelp/paasta/tree/v0.67.5) (2017-09-20)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.67.4...v0.67.5)

**Merged pull requests:**

- in local-run do not try to bind to a port if healthcheck is not defined [\#1514](https://github.com/Yelp/paasta/pull/1514) ([somic](https://github.com/somic))
- fix another mypy error [\#1510](https://github.com/Yelp/paasta/pull/1510) ([matthewbentley](https://github.com/matthewbentley))
- PAASTA-12576: Optimize check\_marathon\_service runtime. [\#1494](https://github.com/Yelp/paasta/pull/1494) ([oktopuz](https://github.com/oktopuz))
- Add script to generate HAProxy map file [\#1442](https://github.com/Yelp/paasta/pull/1442) ([jvperrin](https://github.com/jvperrin))

## [v0.67.4](https://github.com/Yelp/paasta/tree/v0.67.4) (2017-09-13)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.67.3...v0.67.4)

**Merged pull requests:**

- Don't try to drain unhealthy instances when scaling the cluster down [\#1508](https://github.com/Yelp/paasta/pull/1508) ([matthewbentley](https://github.com/matthewbentley))

## [v0.67.3](https://github.com/Yelp/paasta/tree/v0.67.3) (2017-09-13)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.67.2...v0.67.3)

## [v0.67.2](https://github.com/Yelp/paasta/tree/v0.67.2) (2017-09-13)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.67.1...v0.67.2)

**Merged pull requests:**

- Fix use of system\_paasta\_config.get, fallout of \#1426 [\#1509](https://github.com/Yelp/paasta/pull/1509) ([EvanKrall](https://github.com/EvanKrall))

## [v0.67.1](https://github.com/Yelp/paasta/tree/v0.67.1) (2017-09-12)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.67.0...v0.67.1)

**Closed issues:**

- Cluster autoscaler fails on resource with more than 200 instances [\#1501](https://github.com/Yelp/paasta/issues/1501)

**Merged pull requests:**

- Chunk calls to aws describe instances to 199 at a time [\#1506](https://github.com/Yelp/paasta/pull/1506) ([matthewbentley](https://github.com/matthewbentley))
- Fix type error, where whitelist gets checked against none in mesos\_tools [\#1505](https://github.com/Yelp/paasta/pull/1505) ([matthewbentley](https://github.com/matthewbentley))
- Cluster autoscaler ignore unfufilled sfr [\#1504](https://github.com/Yelp/paasta/pull/1504) ([matfra](https://github.com/matfra))

## [v0.67.0](https://github.com/Yelp/paasta/tree/v0.67.0) (2017-09-12)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.66.39...v0.67.0)

**Merged pull requests:**

- Toggle check\_oom\_events on a per-instance basis. [\#1500](https://github.com/Yelp/paasta/pull/1500) ([oktopuz](https://github.com/oktopuz))
- Use new deadsnakes ppa [\#1498](https://github.com/Yelp/paasta/pull/1498) ([solarkennedy](https://github.com/solarkennedy))
- Security check params passed to script [\#1497](https://github.com/Yelp/paasta/pull/1497) ([transcedentalia](https://github.com/transcedentalia))
- remote runs in paasta\_serviceinit [\#1489](https://github.com/Yelp/paasta/pull/1489) ([keymone](https://github.com/keymone))
- Add a prefilter before reading service configuration files [\#1488](https://github.com/Yelp/paasta/pull/1488) ([henryzhangsta](https://github.com/henryzhangsta))
- Try to reduce the verbosity of deployd logs [\#1486](https://github.com/Yelp/paasta/pull/1486) ([solarkennedy](https://github.com/solarkennedy))
- Remove trailing slashes before de-duping volume mounts [\#1449](https://github.com/Yelp/paasta/pull/1449) ([jglukasik](https://github.com/jglukasik))
- Add type annotations and a mypy tox env [\#1426](https://github.com/Yelp/paasta/pull/1426) ([EvanKrall](https://github.com/EvanKrall))

## [v0.66.39](https://github.com/Yelp/paasta/tree/v0.66.39) (2017-09-04)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.66.38...v0.66.39)

**Merged pull requests:**

- De-prioritise startup/public config bounces [\#1490](https://github.com/Yelp/paasta/pull/1490) ([mattmb](https://github.com/mattmb))

## [v0.66.38](https://github.com/Yelp/paasta/tree/v0.66.38) (2017-09-04)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.66.37...v0.66.38)

**Merged pull requests:**

- checks whether volumes exist before local-run can attempt to bind [\#1485](https://github.com/Yelp/paasta/pull/1485) ([fboxwala](https://github.com/fboxwala))
- adjusts get\_marathon\_services\_for\_nerve to handle missing marathon files [\#1484](https://github.com/Yelp/paasta/pull/1484) ([fboxwala](https://github.com/fboxwala))
- include environment vars in remote-run [\#1482](https://github.com/Yelp/paasta/pull/1482) ([Rob-Johnson](https://github.com/Rob-Johnson))
- Make deployd bounce queue a priority queue [\#1466](https://github.com/Yelp/paasta/pull/1466) ([mattmb](https://github.com/mattmb))

## [v0.66.37](https://github.com/Yelp/paasta/tree/v0.66.37) (2017-08-29)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.66.36...v0.66.37)

**Closed issues:**

- Prune merged branches from github.com/yelp/paasta remote [\#1478](https://github.com/Yelp/paasta/issues/1478)

**Merged pull requests:**

- Package check\_oom\_events as a script. [\#1483](https://github.com/Yelp/paasta/pull/1483) ([oktopuz](https://github.com/oktopuz))
- Match process names with spaces and slashes in oom-logger [\#1481](https://github.com/Yelp/paasta/pull/1481) ([oktopuz](https://github.com/oktopuz))
- Add 64MB to memory-swap to work around PAASTA-12450 [\#1480](https://github.com/Yelp/paasta/pull/1480) ([oktopuz](https://github.com/oktopuz))
- Fix condition within PyPI deploy step. [\#1476](https://github.com/Yelp/paasta/pull/1476) ([ssk2](https://github.com/ssk2))
- check\_oom\_events initial commit [\#1450](https://github.com/Yelp/paasta/pull/1450) ([oktopuz](https://github.com/oktopuz))

## [v0.66.36](https://github.com/Yelp/paasta/tree/v0.66.36) (2017-08-23)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.66.35...v0.66.36)

**Merged pull requests:**

- Make paasta status show the service name [\#1477](https://github.com/Yelp/paasta/pull/1477) ([nhandler](https://github.com/nhandler))
- Status by owner, round 2 [\#1474](https://github.com/Yelp/paasta/pull/1474) ([matthewbentley](https://github.com/matthewbentley))

## [v0.66.35](https://github.com/Yelp/paasta/tree/v0.66.35) (2017-08-22)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.66.34...v0.66.35)

**Merged pull requests:**

- bump up task\_processing sha [\#1475](https://github.com/Yelp/paasta/pull/1475) ([huadongliu](https://github.com/huadongliu))
- Handle http error in check\_capacity [\#1473](https://github.com/Yelp/paasta/pull/1473) ([matthewbentley](https://github.com/matthewbentley))
- Enable deployd for the example cluster [\#1472](https://github.com/Yelp/paasta/pull/1472) ([mattmb](https://github.com/mattmb))
- Upgrade pre-commit and hooks [\#1471](https://github.com/Yelp/paasta/pull/1471) ([asottile](https://github.com/asottile))
- Allow deployd bounce rates to be floats and change the default [\#1469](https://github.com/Yelp/paasta/pull/1469) ([solarkennedy](https://github.com/solarkennedy))
- Paasta args mixer rewrite [\#1462](https://github.com/Yelp/paasta/pull/1462) ([matthewbentley](https://github.com/matthewbentley))
- check\_chronos\_jobs to take job runtime into account [\#1438](https://github.com/Yelp/paasta/pull/1438) ([oktopuz](https://github.com/oktopuz))

## [v0.66.34](https://github.com/Yelp/paasta/tree/v0.66.34) (2017-08-21)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.66.33...v0.66.34)

**Closed issues:**

- `paasta logs --components chronos` fails with TypeError: cannot use a string pattern on a bytes-like object [\#1413](https://github.com/Yelp/paasta/issues/1413)

**Merged pull requests:**

- Remove validation that prevents github remotes [\#1470](https://github.com/Yelp/paasta/pull/1470) ([ddelnano](https://github.com/ddelnano))
- Decode marathon and chronos streams as utf-8 [\#1464](https://github.com/Yelp/paasta/pull/1464) ([ddelnano](https://github.com/ddelnano))

## [v0.66.33](https://github.com/Yelp/paasta/tree/v0.66.33) (2017-08-18)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.66.32...v0.66.33)

**Merged pull requests:**

- Fix metastatus table formatting for GPUs [\#1468](https://github.com/Yelp/paasta/pull/1468) ([matthewbentley](https://github.com/matthewbentley))
- Allow port ranges for CIDR rules [\#1467](https://github.com/Yelp/paasta/pull/1467) ([chriskuehl](https://github.com/chriskuehl))

## [v0.66.32](https://github.com/Yelp/paasta/tree/v0.66.32) (2017-08-17)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.66.31...v0.66.32)

**Closed issues:**

- Add a docker\_opts\_whitelist, to support somewhat arbitrary docker args [\#1411](https://github.com/Yelp/paasta/issues/1411)

**Merged pull requests:**

- Factor bounce\_margin\_factor into wait-for-deployment [\#1460](https://github.com/Yelp/paasta/pull/1460) ([chriskuehl](https://github.com/chriskuehl))
- Allow 'extra\_docker\_args' [\#1458](https://github.com/Yelp/paasta/pull/1458) ([matthewbentley](https://github.com/matthewbentley))

## [v0.66.31](https://github.com/Yelp/paasta/tree/v0.66.31) (2017-08-17)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.66.30...v0.66.31)

## [v0.66.30](https://github.com/Yelp/paasta/tree/v0.66.30) (2017-08-17)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.66.29...v0.66.30)

**Merged pull requests:**

- split up metastatus checks [\#1452](https://github.com/Yelp/paasta/pull/1452) ([matthewbentley](https://github.com/matthewbentley))

## [v0.66.29](https://github.com/Yelp/paasta/tree/v0.66.29) (2017-08-16)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.66.28...v0.66.29)

**Merged pull requests:**

- Bump taskproc [\#1465](https://github.com/Yelp/paasta/pull/1465) ([sagar8192](https://github.com/sagar8192))
- Add gpu resource for remote-run and cli \(for hook validation\) [\#1455](https://github.com/Yelp/paasta/pull/1455) ([charleskwwan](https://github.com/charleskwwan))

## [v0.66.28](https://github.com/Yelp/paasta/tree/v0.66.28) (2017-08-16)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.66.27...v0.66.28)

## [v0.66.27](https://github.com/Yelp/paasta/tree/v0.66.27) (2017-08-16)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.66.26...v0.66.27)

**Merged pull requests:**

- Fix scaling SFRs to less than 1 [\#1463](https://github.com/Yelp/paasta/pull/1463) ([mattmb](https://github.com/mattmb))
- Remove six compatibility layer [\#1456](https://github.com/Yelp/paasta/pull/1456) ([asottile](https://github.com/asottile))
- Make paasta without args compat with py3 [\#1428](https://github.com/Yelp/paasta/pull/1428) ([solarkennedy](https://github.com/solarkennedy))

## [v0.66.26](https://github.com/Yelp/paasta/tree/v0.66.26) (2017-08-14)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.66.24...v0.66.26)

**Merged pull requests:**

- Support https in marathon healthchecks [\#1457](https://github.com/Yelp/paasta/pull/1457) ([solarkennedy](https://github.com/solarkennedy))
- Upgrade to marathon 1.4.6 [\#1454](https://github.com/Yelp/paasta/pull/1454) ([nhandler](https://github.com/nhandler))
- Bump taskproc to latest [\#1451](https://github.com/Yelp/paasta/pull/1451) ([sagar8192](https://github.com/sagar8192))
- only consider maintenance role for utilization [\#1448](https://github.com/Yelp/paasta/pull/1448) ([Rob-Johnson](https://github.com/Rob-Johnson))
- add dynamodb deps to example cluster [\#1447](https://github.com/Yelp/paasta/pull/1447) ([Rob-Johnson](https://github.com/Rob-Johnson))
- Make deployd startup oracle optional [\#1444](https://github.com/Yelp/paasta/pull/1444) ([mattmb](https://github.com/mattmb))
- Support multiple puppet service ports [\#1433](https://github.com/Yelp/paasta/pull/1433) ([jolynch](https://github.com/jolynch))
- Use a version of chronos-python that doesn't warn on non v3 [\#1429](https://github.com/Yelp/paasta/pull/1429) ([solarkennedy](https://github.com/solarkennedy))
- Durable storage of information about tasks for native scheduler [\#1345](https://github.com/Yelp/paasta/pull/1345) ([EvanKrall](https://github.com/EvanKrall))

## [v0.66.24](https://github.com/Yelp/paasta/tree/v0.66.24) (2017-08-09)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.66.23...v0.66.24)

## [v0.66.23](https://github.com/Yelp/paasta/tree/v0.66.23) (2017-08-09)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.66.22...v0.66.23)

**Merged pull requests:**

- check\_capacity should be added as a script [\#1446](https://github.com/Yelp/paasta/pull/1446) ([matthewbentley](https://github.com/matthewbentley))

## [v0.66.22](https://github.com/Yelp/paasta/tree/v0.66.22) (2017-08-09)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.66.21...v0.66.22)

**Merged pull requests:**

- Fix resource api [\#1395](https://github.com/Yelp/paasta/pull/1395) ([matthewbentley](https://github.com/matthewbentley))
- Add capacity\_check health check [\#1394](https://github.com/Yelp/paasta/pull/1394) ([matthewbentley](https://github.com/matthewbentley))

## [v0.66.21](https://github.com/Yelp/paasta/tree/v0.66.21) (2017-08-09)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.66.20...v0.66.21)

**Merged pull requests:**

- Dedupe local run volumes using get\_volumes [\#1443](https://github.com/Yelp/paasta/pull/1443) ([EvanKrall](https://github.com/EvanKrall))

## [v0.66.20](https://github.com/Yelp/paasta/tree/v0.66.20) (2017-08-08)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.66.19...v0.66.20)

**Merged pull requests:**

- Inject the PAASTA\_DEPLOY\_GROUP env variable [\#1440](https://github.com/Yelp/paasta/pull/1440) ([jglukasik](https://github.com/jglukasik))
- Revert "Merge pull request \#1417 from chriskuehl/whitelist-google-public-dns" [\#1437](https://github.com/Yelp/paasta/pull/1437) ([chriskuehl](https://github.com/chriskuehl))
- Convert the playground and mesos dockerfiles to py36 [\#1435](https://github.com/Yelp/paasta/pull/1435) ([asottile](https://github.com/asottile))
- Make deployd skip deploys on broken instances [\#1434](https://github.com/Yelp/paasta/pull/1434) ([solarkennedy](https://github.com/solarkennedy))
- Reduce some requirements [\#1432](https://github.com/Yelp/paasta/pull/1432) ([asottile](https://github.com/asottile))
- Convert marathon docker image to xenial [\#1431](https://github.com/Yelp/paasta/pull/1431) ([asottile](https://github.com/asottile))
- Add support for whitelisting CIDR/port combinations [\#1430](https://github.com/Yelp/paasta/pull/1430) ([chriskuehl](https://github.com/chriskuehl))
- pass instance arg from remote-run to remote\_run [\#1427](https://github.com/Yelp/paasta/pull/1427) ([keymone](https://github.com/keymone))
- Add process name to the oom log [\#1424](https://github.com/Yelp/paasta/pull/1424) ([oktopuz](https://github.com/oktopuz))
- Convert more dockerfiles to xenial [\#1421](https://github.com/Yelp/paasta/pull/1421) ([asottile](https://github.com/asottile))
- Fail wait-for-deployment if a cluster has no configured API endpoints [\#1414](https://github.com/Yelp/paasta/pull/1414) ([oktopuz](https://github.com/oktopuz))
- Bump task-proc and fix a unit test. [\#1403](https://github.com/Yelp/paasta/pull/1403) ([sagar8192](https://github.com/sagar8192))
- Hooks use python 3 defaults [\#1391](https://github.com/Yelp/paasta/pull/1391) ([asottile](https://github.com/asottile))

## [v0.66.19](https://github.com/Yelp/paasta/tree/v0.66.19) (2017-08-02)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.66.18...v0.66.19)

## [v0.66.18](https://github.com/Yelp/paasta/tree/v0.66.18) (2017-08-02)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.66.17...v0.66.18)

**Closed issues:**

- Hostnames should not end with minus signs [\#1422](https://github.com/Yelp/paasta/issues/1422)

**Merged pull requests:**

- strip trailing hostname dashes [\#1423](https://github.com/Yelp/paasta/pull/1423) ([bchess](https://github.com/bchess))
- Apply a docker best practice to itest\_trusty / itest\_xenial [\#1420](https://github.com/Yelp/paasta/pull/1420) ([asottile](https://github.com/asottile))
- drop retrying executor from the stack [\#1418](https://github.com/Yelp/paasta/pull/1418) ([keymone](https://github.com/keymone))
- Use xenial for more dockerfiles [\#1416](https://github.com/Yelp/paasta/pull/1416) ([asottile](https://github.com/asottile))

## [v0.66.17](https://github.com/Yelp/paasta/tree/v0.66.17) (2017-08-02)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.66.16...v0.66.17)

**Merged pull requests:**

- Add a sane timeout when talking to hacheck [\#1419](https://github.com/Yelp/paasta/pull/1419) ([mattmb](https://github.com/mattmb))
- add a note describing Yelp's fork of Chronos. [\#1415](https://github.com/Yelp/paasta/pull/1415) ([Rob-Johnson](https://github.com/Rob-Johnson))

## [v0.66.16](https://github.com/Yelp/paasta/tree/v0.66.16) (2017-08-02)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.66.15...v0.66.16)

**Closed issues:**

- Enable CORS for the REST API [\#1373](https://github.com/Yelp/paasta/issues/1373)

**Merged pull requests:**

- Always whitelist Google Public DNS [\#1417](https://github.com/Yelp/paasta/pull/1417) ([chriskuehl](https://github.com/chriskuehl))
- Remove unused dockerfiles/itest/itest [\#1410](https://github.com/Yelp/paasta/pull/1410) ([asottile](https://github.com/asottile))
- Add CORS to API endpoints [\#1407](https://github.com/Yelp/paasta/pull/1407) ([magicmark](https://github.com/magicmark))
- Upgrade add-trailing-comma [\#1404](https://github.com/Yelp/paasta/pull/1404) ([asottile](https://github.com/asottile))

## [v0.66.15](https://github.com/Yelp/paasta/tree/v0.66.15) (2017-08-01)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.66.14...v0.66.15)

## [v0.66.14](https://github.com/Yelp/paasta/tree/v0.66.14) (2017-07-31)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.66.13...v0.66.14)

**Merged pull requests:**

- When checking the registry, fallback to trying http as well [\#1412](https://github.com/Yelp/paasta/pull/1412) ([bchess](https://github.com/bchess))
- Remove python2-specific requirements [\#1405](https://github.com/Yelp/paasta/pull/1405) ([asottile](https://github.com/asottile))
- use dynamodb persister for paasta remote-run [\#1388](https://github.com/Yelp/paasta/pull/1388) ([Rob-Johnson](https://github.com/Rob-Johnson))

## [v0.66.13](https://github.com/Yelp/paasta/tree/v0.66.13) (2017-07-28)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.66.12...v0.66.13)

## [v0.66.12](https://github.com/Yelp/paasta/tree/v0.66.12) (2017-07-28)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.66.11...v0.66.12)

**Merged pull requests:**

- Allow setting the shm-size [\#1409](https://github.com/Yelp/paasta/pull/1409) ([matthewbentley](https://github.com/matthewbentley))

## [v0.66.11](https://github.com/Yelp/paasta/tree/v0.66.11) (2017-07-26)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.66.10...v0.66.11)

**Merged pull requests:**

- Bug fixes to firewall\_update related to py3 upgrade [\#1402](https://github.com/Yelp/paasta/pull/1402) ([bchess](https://github.com/bchess))
- Make paasta\_api docker image use python3.6 [\#1401](https://github.com/Yelp/paasta/pull/1401) ([asottile](https://github.com/asottile))
- Handle NoSlavesException from list\_marathon\_service\_instances [\#1399](https://github.com/Yelp/paasta/pull/1399) ([jglukasik](https://github.com/jglukasik))

## [v0.66.10](https://github.com/Yelp/paasta/tree/v0.66.10) (2017-07-25)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.66.9...v0.66.10)

**Merged pull requests:**

- Cleanup dockerfiles more [\#1400](https://github.com/Yelp/paasta/pull/1400) ([asottile](https://github.com/asottile))
- CI test branches prefixed with `travis-ci-` [\#1398](https://github.com/Yelp/paasta/pull/1398) ([asottile](https://github.com/asottile))
- Improve dockerfiles [\#1397](https://github.com/Yelp/paasta/pull/1397) ([asottile](https://github.com/asottile))

## [v0.66.9](https://github.com/Yelp/paasta/tree/v0.66.9) (2017-07-24)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.66.8...v0.66.9)

**Merged pull requests:**

- Only get slave data from the paasta api on verbose mode [\#1392](https://github.com/Yelp/paasta/pull/1392) ([solarkennedy](https://github.com/solarkennedy))
- Add agent count to paasta metastatus -vv [\#1387](https://github.com/Yelp/paasta/pull/1387) ([matthewbentley](https://github.com/matthewbentley))
- paasta\_oom\_logger [\#1382](https://github.com/Yelp/paasta/pull/1382) ([oktopuz](https://github.com/oktopuz))

## [v0.66.8](https://github.com/Yelp/paasta/tree/v0.66.8) (2017-07-21)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.66.7...v0.66.8)

**Merged pull requests:**

- One more zk.set to use bytes [\#1390](https://github.com/Yelp/paasta/pull/1390) ([huadongliu](https://github.com/huadongliu))

## [v0.66.7](https://github.com/Yelp/paasta/tree/v0.66.7) (2017-07-20)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.66.6...v0.66.7)

**Merged pull requests:**

- Use bytes when zk.set and handle mesos read timeout exceptions [\#1386](https://github.com/Yelp/paasta/pull/1386) ([solarkennedy](https://github.com/solarkennedy))
- Upgrade add-trailing-comma to 0.6.1 [\#1384](https://github.com/Yelp/paasta/pull/1384) ([asottile](https://github.com/asottile))
- Fix worker exception handling [\#1381](https://github.com/Yelp/paasta/pull/1381) ([mattmb](https://github.com/mattmb))
- Fix bug where paasta fsm was suggesting ports that were already in use [\#1378](https://github.com/Yelp/paasta/pull/1378) ([EvanKrall](https://github.com/EvanKrall))
- Dedupe the bounce queue [\#1364](https://github.com/Yelp/paasta/pull/1364) ([mattmb](https://github.com/mattmb))
- Reduce travis builder time [\#1337](https://github.com/Yelp/paasta/pull/1337) ([EvanKrall](https://github.com/EvanKrall))

## [v0.66.6](https://github.com/Yelp/paasta/tree/v0.66.6) (2017-07-19)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.66.5...v0.66.6)

**Merged pull requests:**

- Call flush\(\) in paasta\_print [\#1383](https://github.com/Yelp/paasta/pull/1383) ([chriskuehl](https://github.com/chriskuehl))

## [v0.66.5](https://github.com/Yelp/paasta/tree/v0.66.5) (2017-07-18)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.66.4...v0.66.5)

**Closed issues:**

- API appears to be single threaded [\#1374](https://github.com/Yelp/paasta/issues/1374)

**Merged pull requests:**

- use task processing plugin registry instead of direct imports [\#1379](https://github.com/Yelp/paasta/pull/1379) ([keymone](https://github.com/keymone))
- Upgrade asottile/add-trailing-comma to 0.5.1 [\#1377](https://github.com/Yelp/paasta/pull/1377) ([asottile](https://github.com/asottile))
- Python3 bytes [\#1376](https://github.com/Yelp/paasta/pull/1376) ([solarkennedy](https://github.com/solarkennedy))
- Monkey patch the standard library to make the api non-blocking [\#1375](https://github.com/Yelp/paasta/pull/1375) ([matthewbentley](https://github.com/matthewbentley))
- Resolve more autopep8 / add-trailing-comma conflicts [\#1372](https://github.com/Yelp/paasta/pull/1372) ([asottile](https://github.com/asottile))
- Manually resolved add-trailing-comma changes [\#1371](https://github.com/Yelp/paasta/pull/1371) ([asottile](https://github.com/asottile))
- Fix remote run signal handler [\#1370](https://github.com/Yelp/paasta/pull/1370) ([huadongliu](https://github.com/huadongliu))
- Fix the type of user\_port to avoid TypeError in socket.socket [\#1362](https://github.com/Yelp/paasta/pull/1362) ([ronin13](https://github.com/ronin13))
- Ask marathon to embed tasks not failures [\#1354](https://github.com/Yelp/paasta/pull/1354) ([mattmb](https://github.com/mattmb))
- Allow for https healthchecks [\#1352](https://github.com/Yelp/paasta/pull/1352) ([jglukasik](https://github.com/jglukasik))

## [v0.66.4](https://github.com/Yelp/paasta/tree/v0.66.4) (2017-07-14)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.66.3...v0.66.4)

**Merged pull requests:**

- Scribereader was not py3 compatible until 0.2.6 [\#1369](https://github.com/Yelp/paasta/pull/1369) ([jolynch](https://github.com/jolynch))
- Use asottile/add-trailing-comma [\#1367](https://github.com/Yelp/paasta/pull/1367) ([asottile](https://github.com/asottile))
- Upgrade pre-commit and hooks [\#1366](https://github.com/Yelp/paasta/pull/1366) ([asottile](https://github.com/asottile))

## [v0.66.3](https://github.com/Yelp/paasta/tree/v0.66.3) (2017-07-13)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.66.2...v0.66.3)

**Merged pull requests:**

- Fixing some encoding errors uncovered in dev/stage [\#1365](https://github.com/Yelp/paasta/pull/1365) ([jolynch](https://github.com/jolynch))
- paasta\_oom\_finder, initial commit [\#1359](https://github.com/Yelp/paasta/pull/1359) ([oktopuz](https://github.com/oktopuz))
- Only write yaml services file when there is a difference in config content [\#1358](https://github.com/Yelp/paasta/pull/1358) ([thebostik](https://github.com/thebostik))

## [v0.66.2](https://github.com/Yelp/paasta/tree/v0.66.2) (2017-07-13)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.66.1...v0.66.2)

**Merged pull requests:**

- Fixup firewall.py for py3 [\#1361](https://github.com/Yelp/paasta/pull/1361) ([jolynch](https://github.com/jolynch))

## [v0.66.1](https://github.com/Yelp/paasta/tree/v0.66.1) (2017-07-13)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.66.0...v0.66.1)

**Merged pull requests:**

- Pysensu is not py36 compatible until 0.3.4 [\#1360](https://github.com/Yelp/paasta/pull/1360) ([jolynch](https://github.com/jolynch))

## [v0.66.0](https://github.com/Yelp/paasta/tree/v0.66.0) (2017-07-12)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.65.38...v0.66.0)

**Merged pull requests:**

- Port to python 3 [\#1341](https://github.com/Yelp/paasta/pull/1341) ([jolynch](https://github.com/jolynch))

## [v0.65.38](https://github.com/Yelp/paasta/tree/v0.65.38) (2017-07-12)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.65.37...v0.65.38)

## [v0.65.37](https://github.com/Yelp/paasta/tree/v0.65.37) (2017-07-12)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.65.36...v0.65.37)

**Merged pull requests:**

- prepare task\_config before initializing mesos stack [\#1357](https://github.com/Yelp/paasta/pull/1357) ([keymone](https://github.com/keymone))
- Catch InvalidJobNameError in deployd [\#1355](https://github.com/Yelp/paasta/pull/1355) ([mattmb](https://github.com/mattmb))

## [v0.65.36](https://github.com/Yelp/paasta/tree/v0.65.36) (2017-07-11)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.65.35...v0.65.36)

**Closed issues:**

- paasta\_itests don't run the api server [\#1339](https://github.com/Yelp/paasta/issues/1339)

**Merged pull requests:**

- Itest readme [\#1353](https://github.com/Yelp/paasta/pull/1353) ([matthewbentley](https://github.com/matthewbentley))
- Add paasta metastatus utilization api [\#1332](https://github.com/Yelp/paasta/pull/1332) ([matthewbentley](https://github.com/matthewbentley))
- Remote-run on taskproc [\#1325](https://github.com/Yelp/paasta/pull/1325) ([keymone](https://github.com/keymone))

## [v0.65.35](https://github.com/Yelp/paasta/tree/v0.65.35) (2017-07-10)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.65.34...v0.65.35)

**Merged pull requests:**

- Fix deployd updating instance count [\#1350](https://github.com/Yelp/paasta/pull/1350) ([mattmb](https://github.com/mattmb))
- Make maintenance polling frequency configurable [\#1346](https://github.com/Yelp/paasta/pull/1346) ([mattmb](https://github.com/mattmb))

## [v0.65.34](https://github.com/Yelp/paasta/tree/v0.65.34) (2017-07-06)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.65.33...v0.65.34)

**Merged pull requests:**

- Fix deployd prioritising bouncing services [\#1349](https://github.com/Yelp/paasta/pull/1349) ([mattmb](https://github.com/mattmb))

## [v0.65.33](https://github.com/Yelp/paasta/tree/v0.65.33) (2017-07-05)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.65.32...v0.65.33)

**Merged pull requests:**

- Changing firewall-related paths to be /var/lib/paasta instead of /var/run/paasta [\#1347](https://github.com/Yelp/paasta/pull/1347) ([bchess](https://github.com/bchess))
- Fix log line in deployd [\#1344](https://github.com/Yelp/paasta/pull/1344) ([mattmb](https://github.com/mattmb))
- Don't fail firewall updates when a deleted service still has running containers [\#1343](https://github.com/Yelp/paasta/pull/1343) ([chriskuehl](https://github.com/chriskuehl))
- Restore old signal handler on exit [\#1327](https://github.com/Yelp/paasta/pull/1327) ([tiras-j](https://github.com/tiras-j))

## [v0.65.32](https://github.com/Yelp/paasta/tree/v0.65.32) (2017-07-03)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.65.31...v0.65.32)

**Merged pull requests:**

- Make deployd prioritise bouncing services on start [\#1340](https://github.com/Yelp/paasta/pull/1340) ([mattmb](https://github.com/mattmb))
- Fix maintenance watcher [\#1338](https://github.com/Yelp/paasta/pull/1338) ([mattmb](https://github.com/mattmb))

## [v0.65.31](https://github.com/Yelp/paasta/tree/v0.65.31) (2017-06-30)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.65.30...v0.65.31)

**Merged pull requests:**

- Simplify logging for deployd [\#1342](https://github.com/Yelp/paasta/pull/1342) ([mattmb](https://github.com/mattmb))
- Add exponential back off deployd [\#1336](https://github.com/Yelp/paasta/pull/1336) ([mattmb](https://github.com/mattmb))
- Fix issue in handling SlaveDoesNotExist exception [\#1335](https://github.com/Yelp/paasta/pull/1335) ([jglukasik](https://github.com/jglukasik))
- Make mark and wait for deployment commands accept a short git sha [\#1329](https://github.com/Yelp/paasta/pull/1329) ([nhandler](https://github.com/nhandler))
- Catch IOErrors when writing to log files, so you can e.g. use paasta restart on boxes where you don't have write permission to the log file. [\#1270](https://github.com/Yelp/paasta/pull/1270) ([EvanKrall](https://github.com/EvanKrall))

## [v0.65.30](https://github.com/Yelp/paasta/tree/v0.65.30) (2017-06-28)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.65.29...v0.65.30)

**Merged pull requests:**

- Fixing the security-check notification system [\#1331](https://github.com/Yelp/paasta/pull/1331) ([transcedentalia](https://github.com/transcedentalia))
- Print service name when paasta\_print log line [\#1330](https://github.com/Yelp/paasta/pull/1330) ([jglukasik](https://github.com/jglukasik))
- Catch any exception running setup\_marathon\_job [\#1320](https://github.com/Yelp/paasta/pull/1320) ([mattmb](https://github.com/mattmb))

## [v0.65.29](https://github.com/Yelp/paasta/tree/v0.65.29) (2017-06-27)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.65.28...v0.65.29)

**Merged pull requests:**

- Fix rounding error: cluster autoscaler [\#1333](https://github.com/Yelp/paasta/pull/1333) ([mattmb](https://github.com/mattmb))

## [v0.65.28](https://github.com/Yelp/paasta/tree/v0.65.28) (2017-06-22)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.65.27...v0.65.28)

**Merged pull requests:**

- add apt.dockerproject.org for travis runs [\#1328](https://github.com/Yelp/paasta/pull/1328) ([bchess](https://github.com/bchess))
- Fix paasta local-run port argument [\#1323](https://github.com/Yelp/paasta/pull/1323) ([drolando](https://github.com/drolando))
- security-check documentation updates [\#1315](https://github.com/Yelp/paasta/pull/1315) ([transcedentalia](https://github.com/transcedentalia))
- Adding a timed flock to firewall operations [\#1314](https://github.com/Yelp/paasta/pull/1314) ([bchess](https://github.com/bchess))
- check\_chronos\_jobs considers rerun tasks, allowing paasta rerun to resolve a failed job [\#1312](https://github.com/Yelp/paasta/pull/1312) ([matthewbentley](https://github.com/matthewbentley))

## [v0.65.27](https://github.com/Yelp/paasta/tree/v0.65.27) (2017-06-21)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.65.26...v0.65.27)

**Merged pull requests:**

- Better error message telling you which cluster the problem was with [\#1324](https://github.com/Yelp/paasta/pull/1324) ([bobtfish](https://github.com/bobtfish))
- Switch from package\_data to manifest file to fix missing package data in internal wheels [\#1322](https://github.com/Yelp/paasta/pull/1322) ([chriskuehl](https://github.com/chriskuehl))
- Add ScribeHandler to paasta-deployd [\#1321](https://github.com/Yelp/paasta/pull/1321) ([mattmb](https://github.com/mattmb))
- Fix check-chronos-jobs when schedule timezone is None [\#1317](https://github.com/Yelp/paasta/pull/1317) ([oktopuz](https://github.com/oktopuz))
- Catch SlaveDoesNotExist exception in autoscaler [\#1299](https://github.com/Yelp/paasta/pull/1299) ([jglukasik](https://github.com/jglukasik))

## [v0.65.26](https://github.com/Yelp/paasta/tree/v0.65.26) (2017-06-20)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.65.25...v0.65.26)

**Merged pull requests:**

- Tell us what git url actually failed [\#1319](https://github.com/Yelp/paasta/pull/1319) ([bobtfish](https://github.com/bobtfish))
- fix fitness evaluation of mesos slaves [\#1318](https://github.com/Yelp/paasta/pull/1318) ([Rob-Johnson](https://github.com/Rob-Johnson))
- Remove mesos.interface now that we use pymesos. [\#1303](https://github.com/Yelp/paasta/pull/1303) ([EvanKrall](https://github.com/EvanKrall))

## [v0.65.25](https://github.com/Yelp/paasta/tree/v0.65.25) (2017-06-19)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.65.24...v0.65.25)

**Merged pull requests:**

- Upgrade to mesos 1.3.0 [\#1316](https://github.com/Yelp/paasta/pull/1316) ([nhandler](https://github.com/nhandler))
- Upgrade itests to use Marathon 1.4.5 [\#1311](https://github.com/Yelp/paasta/pull/1311) ([nhandler](https://github.com/nhandler))
- Replace acceptable\_delay with alert\_after in check\_chronos\_jobs [\#1304](https://github.com/Yelp/paasta/pull/1304) ([oktopuz](https://github.com/oktopuz))
- Allow for per-service docker registry [\#1300](https://github.com/Yelp/paasta/pull/1300) ([jglukasik](https://github.com/jglukasik))
- Make setup\_marathon\_job take care of slack instance slots on scaledown [\#1255](https://github.com/Yelp/paasta/pull/1255) ([solarkennedy](https://github.com/solarkennedy))

## [v0.65.24](https://github.com/Yelp/paasta/tree/v0.65.24) (2017-06-15)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.65.23...v0.65.24)

**Merged pull requests:**

- Allow return traffic for established connections [\#1310](https://github.com/Yelp/paasta/pull/1310) ([chriskuehl](https://github.com/chriskuehl))
- Tolerate weird instance types in firewall update [\#1309](https://github.com/Yelp/paasta/pull/1309) ([chriskuehl](https://github.com/chriskuehl))

## [v0.65.23](https://github.com/Yelp/paasta/tree/v0.65.23) (2017-06-15)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.65.22...v0.65.23)

**Closed issues:**

- We need more aggressive timeouts on drain/stop\_draining [\#1287](https://github.com/Yelp/paasta/issues/1287)

**Merged pull requests:**

- Allow all services to access scribe, metrics-relay, sensu [\#1308](https://github.com/Yelp/paasta/pull/1308) ([chriskuehl](https://github.com/chriskuehl))
- Add timeout to HTTP draining requests [\#1307](https://github.com/Yelp/paasta/pull/1307) ([jvperrin](https://github.com/jvperrin))
- Allow all services access to DNS [\#1306](https://github.com/Yelp/paasta/pull/1306) ([chriskuehl](https://github.com/chriskuehl))
- Use private PyPI when available [\#1260](https://github.com/Yelp/paasta/pull/1260) ([chriskuehl](https://github.com/chriskuehl))

## [v0.65.22](https://github.com/Yelp/paasta/tree/v0.65.22) (2017-06-14)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.65.21...v0.65.22)

## [v0.65.21](https://github.com/Yelp/paasta/tree/v0.65.21) (2017-06-14)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.65.20...v0.65.21)

**Merged pull requests:**

- Add comments on iptables ACCEPT lines [\#1305](https://github.com/Yelp/paasta/pull/1305) ([bchess](https://github.com/bchess))
- Enforce ordering on iptables rules [\#1302](https://github.com/Yelp/paasta/pull/1302) ([bchess](https://github.com/bchess))
- stricter handling of iptables parameters [\#1301](https://github.com/Yelp/paasta/pull/1301) ([bchess](https://github.com/bchess))
- let autoscaler measure capacity at region level instead of superregion [\#1271](https://github.com/Yelp/paasta/pull/1271) ([somic](https://github.com/somic))

## [v0.65.20](https://github.com/Yelp/paasta/tree/v0.65.20) (2017-06-13)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.65.19...v0.65.20)

**Merged pull requests:**

- Allow paasta rerun to rerun disabled chronos jobs [\#1298](https://github.com/Yelp/paasta/pull/1298) ([matthewbentley](https://github.com/matthewbentley))

## [v0.65.19](https://github.com/Yelp/paasta/tree/v0.65.19) (2017-06-12)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.65.18...v0.65.19)

**Merged pull requests:**

- fix erroneous calculation of per-slave usage [\#1296](https://github.com/Yelp/paasta/pull/1296) ([Rob-Johnson](https://github.com/Rob-Johnson))
- batch calls to describe instance status [\#1292](https://github.com/Yelp/paasta/pull/1292) ([Rob-Johnson](https://github.com/Rob-Johnson))
- Re-order replication check output to list CRITICAL before OK [\#1277](https://github.com/Yelp/paasta/pull/1277) ([matthewbentley](https://github.com/matthewbentley))
- sensu alert triggered when the security-check fails [\#1205](https://github.com/Yelp/paasta/pull/1205) ([transcedentalia](https://github.com/transcedentalia))

## [v0.65.18](https://github.com/Yelp/paasta/tree/v0.65.18) (2017-06-12)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.65.17...v0.65.18)

**Merged pull requests:**

- Fix rerun graph itest failures [\#1297](https://github.com/Yelp/paasta/pull/1297) ([macisamuele](https://github.com/macisamuele))
- chronos\_rerun.clone\_job should modify only cloned config [\#1295](https://github.com/Yelp/paasta/pull/1295) ([macisamuele](https://github.com/macisamuele))
- Add unittest for run\_chronos\_rerun with run\_all\_related\_jobs flag [\#1294](https://github.com/Yelp/paasta/pull/1294) ([macisamuele](https://github.com/macisamuele))

## [v0.65.17](https://github.com/Yelp/paasta/tree/v0.65.17) (2017-06-12)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.65.16...v0.65.17)

**Merged pull requests:**

- Fix a potential problem when -v and --rerun-type=graph are passed to paasta rerun [\#1280](https://github.com/Yelp/paasta/pull/1280) ([matthewbentley](https://github.com/matthewbentley))

## [v0.65.16](https://github.com/Yelp/paasta/tree/v0.65.16) (2017-06-09)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.65.15...v0.65.16)

**Merged pull requests:**

- Revert "Merge pull request \#1257 from Yelp/PAASTA-11186-chronos-job-a… [\#1293](https://github.com/Yelp/paasta/pull/1293) ([oktopuz](https://github.com/oktopuz))

## [v0.65.15](https://github.com/Yelp/paasta/tree/v0.65.15) (2017-06-09)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.65.14...v0.65.15)

**Merged pull requests:**

- Add soa\_dir as field in InstanceConfig and subclasses [\#1274](https://github.com/Yelp/paasta/pull/1274) ([jglukasik](https://github.com/jglukasik))

## [v0.65.14](https://github.com/Yelp/paasta/tree/v0.65.14) (2017-06-09)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.65.13...v0.65.14)

**Merged pull requests:**

- batch calls to describe\_instance\_status [\#1291](https://github.com/Yelp/paasta/pull/1291) ([Rob-Johnson](https://github.com/Rob-Johnson))

## [v0.65.13](https://github.com/Yelp/paasta/tree/v0.65.13) (2017-06-09)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.65.12...v0.65.13)

**Merged pull requests:**

- only count non-terminal tasks in metastatus [\#1290](https://github.com/Yelp/paasta/pull/1290) ([Rob-Johnson](https://github.com/Rob-Johnson))

## [v0.65.12](https://github.com/Yelp/paasta/tree/v0.65.12) (2017-06-09)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.65.11...v0.65.12)

**Merged pull requests:**

- docker\_wrapper inserts firewall rules in preparation of running [\#1286](https://github.com/Yelp/paasta/pull/1286) ([bchess](https://github.com/bchess))

## [v0.65.11](https://github.com/Yelp/paasta/tree/v0.65.11) (2017-06-09)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.65.10...v0.65.11)

**Merged pull requests:**

- Revert "Revert "Revert "default healthcheck type to MESOS\_HTTP""" [\#1289](https://github.com/Yelp/paasta/pull/1289) ([mattmb](https://github.com/mattmb))

## [v0.65.10](https://github.com/Yelp/paasta/tree/v0.65.10) (2017-06-08)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.65.9...v0.65.10)

**Merged pull requests:**

- Add support for logging with prefix and limits [\#1273](https://github.com/Yelp/paasta/pull/1273) ([bchess](https://github.com/bchess))

## [v0.65.9](https://github.com/Yelp/paasta/tree/v0.65.9) (2017-06-08)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.65.8...v0.65.9)

**Merged pull requests:**

- Revert "default healthcheck type to MESOS\_HTTP" [\#1285](https://github.com/Yelp/paasta/pull/1285) ([nhandler](https://github.com/nhandler))
- add support for --env-file to docker\_wrapper [\#1282](https://github.com/Yelp/paasta/pull/1282) ([bchess](https://github.com/bchess))
- Make acceptable delay in check\_chronos\_jobs configurable [\#1257](https://github.com/Yelp/paasta/pull/1257) ([oktopuz](https://github.com/oktopuz))

## [v0.65.8](https://github.com/Yelp/paasta/tree/v0.65.8) (2017-06-08)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.65.7...v0.65.8)

**Merged pull requests:**

- Catch OSError watching new folder [\#1284](https://github.com/Yelp/paasta/pull/1284) ([mattmb](https://github.com/mattmb))

## [v0.65.7](https://github.com/Yelp/paasta/tree/v0.65.7) (2017-06-08)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.65.6...v0.65.7)

## [v0.65.6](https://github.com/Yelp/paasta/tree/v0.65.6) (2017-06-08)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.65.5...v0.65.6)

**Merged pull requests:**

- default healthcheck type to MESOS\_HTTP [\#1283](https://github.com/Yelp/paasta/pull/1283) ([Rob-Johnson](https://github.com/Rob-Johnson))

## [v0.65.5](https://github.com/Yelp/paasta/tree/v0.65.5) (2017-06-08)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.65.4...v0.65.5)

**Merged pull requests:**

- Deployd itest [\#1254](https://github.com/Yelp/paasta/pull/1254) ([mattmb](https://github.com/mattmb))

## [v0.65.4](https://github.com/Yelp/paasta/tree/v0.65.4) (2017-06-08)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.65.3...v0.65.4)

**Merged pull requests:**

- inotify actually does updates, and include backends in iptables rules [\#1279](https://github.com/Yelp/paasta/pull/1279) ([bchess](https://github.com/bchess))
- fix call to describe\_instance\_status [\#1278](https://github.com/Yelp/paasta/pull/1278) ([Rob-Johnson](https://github.com/Rob-Johnson))
- Stop implying that start and restart are different. [\#1275](https://github.com/Yelp/paasta/pull/1275) ([EvanKrall](https://github.com/EvanKrall))

## [v0.65.3](https://github.com/Yelp/paasta/tree/v0.65.3) (2017-06-05)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.65.2...v0.65.3)

**Merged pull requests:**

- Port the native scheduler to use pymesos [\#1267](https://github.com/Yelp/paasta/pull/1267) ([jolynch](https://github.com/jolynch))

## [v0.65.2](https://github.com/Yelp/paasta/tree/v0.65.2) (2017-06-05)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.65.1...v0.65.2)

**Merged pull requests:**

- Make firewall cronjob parse dependencies correctly [\#1272](https://github.com/Yelp/paasta/pull/1272) ([chriskuehl](https://github.com/chriskuehl))
- Skip check for existing image in example cluster [\#1268](https://github.com/Yelp/paasta/pull/1268) ([mattmb](https://github.com/mattmb))
- Remove old task\_processing and add new requirement on the git repo [\#1266](https://github.com/Yelp/paasta/pull/1266) ([jolynch](https://github.com/jolynch))
- Refactor get\_draining\_hosts [\#1265](https://github.com/Yelp/paasta/pull/1265) ([jglukasik](https://github.com/jglukasik))
- Support nerve body aware healthcheck [\#1264](https://github.com/Yelp/paasta/pull/1264) ([huadongliu](https://github.com/huadongliu))
- Catch ValueErrors in processing {white,black}lists [\#1263](https://github.com/Yelp/paasta/pull/1263) ([jglukasik](https://github.com/jglukasik))

## [v0.65.1](https://github.com/Yelp/paasta/tree/v0.65.1) (2017-05-31)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.65.0...v0.65.1)

**Merged pull requests:**

- Correct some typos [\#1262](https://github.com/Yelp/paasta/pull/1262) ([danielhoherd](https://github.com/danielhoherd))
- Start to hook up iptables rules with real world state [\#1250](https://github.com/Yelp/paasta/pull/1250) ([chriskuehl](https://github.com/chriskuehl))
- Allow `paasta rerun` of dependent jobs [\#1230](https://github.com/Yelp/paasta/pull/1230) ([macisamuele](https://github.com/macisamuele))

## [v0.65.0](https://github.com/Yelp/paasta/tree/v0.65.0) (2017-05-30)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.64.0...v0.65.0)

## [v0.64.0](https://github.com/Yelp/paasta/tree/v0.64.0) (2017-05-25)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.63.9...v0.64.0)

**Merged pull requests:**

- Fix deployd watcher NoDockerImage exception [\#1252](https://github.com/Yelp/paasta/pull/1252) ([mattmb](https://github.com/mattmb))
- Deployd actions for dying threads [\#1251](https://github.com/Yelp/paasta/pull/1251) ([mattmb](https://github.com/mattmb))
- Paasta 6795 rollback check sha [\#1249](https://github.com/Yelp/paasta/pull/1249) ([oktopuz](https://github.com/oktopuz))
- Handle ConnectionError when getting list of tasks in smartstack [\#1247](https://github.com/Yelp/paasta/pull/1247) ([jglukasik](https://github.com/jglukasik))
- report when an instance doesn't match anything [\#1245](https://github.com/Yelp/paasta/pull/1245) ([oktopuz](https://github.com/oktopuz))

## [v0.63.9](https://github.com/Yelp/paasta/tree/v0.63.9) (2017-05-23)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.63.8...v0.63.9)

**Merged pull requests:**

- Fix paasta labels from being removed from marathon config [\#1253](https://github.com/Yelp/paasta/pull/1253) ([bchess](https://github.com/bchess))

## [v0.63.8](https://github.com/Yelp/paasta/tree/v0.63.8) (2017-05-22)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.63.7...v0.63.8)

**Merged pull requests:**

- Add firewall-related keys to schema validation for Marathon and Chronos [\#1246](https://github.com/Yelp/paasta/pull/1246) ([chriskuehl](https://github.com/chriskuehl))

## [v0.63.7](https://github.com/Yelp/paasta/tree/v0.63.7) (2017-05-22)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.63.6...v0.63.7)

**Merged pull requests:**

- Add cron option to paasta\_firewall\_update [\#1244](https://github.com/Yelp/paasta/pull/1244) ([chriskuehl](https://github.com/chriskuehl))
- Use tox-pip-extensions when available [\#1243](https://github.com/Yelp/paasta/pull/1243) ([chriskuehl](https://github.com/chriskuehl))
- Remove quotes around environment variables in local-run [\#1241](https://github.com/Yelp/paasta/pull/1241) ([oktopuz](https://github.com/oktopuz))
- Add code for working with iptables [\#1236](https://github.com/Yelp/paasta/pull/1236) ([chriskuehl](https://github.com/chriskuehl))
- Dependency & security support, and inotify-based synapse file watcher [\#1234](https://github.com/Yelp/paasta/pull/1234) ([bchess](https://github.com/bchess))

## [v0.63.6](https://github.com/Yelp/paasta/tree/v0.63.6) (2017-05-19)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.63.5...v0.63.6)

**Closed issues:**

- Shelling out to flock in unit tests [\#1239](https://github.com/Yelp/paasta/issues/1239)
- Bug in how we parse tron dates [\#1229](https://github.com/Yelp/paasta/issues/1229)

**Merged pull requests:**

- test\_mac\_address no longer depends on /usr/bin/flock [\#1242](https://github.com/Yelp/paasta/pull/1242) ([bchess](https://github.com/bchess))
- Make deployd check marathon ID before bouncing [\#1240](https://github.com/Yelp/paasta/pull/1240) ([mattmb](https://github.com/mattmb))
- add support for uris to native scheduler [\#1238](https://github.com/Yelp/paasta/pull/1238) ([Rob-Johnson](https://github.com/Rob-Johnson))
- Split startup vs big bounce rate [\#1237](https://github.com/Yelp/paasta/pull/1237) ([mattmb](https://github.com/mattmb))
- Pull autoscaling forecasters into separate library [\#1233](https://github.com/Yelp/paasta/pull/1233) ([EvanKrall](https://github.com/EvanKrall))

## [v0.63.5](https://github.com/Yelp/paasta/tree/v0.63.5) (2017-05-17)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.63.4...v0.63.5)

**Merged pull requests:**

- Speed up processing inbox loop [\#1235](https://github.com/Yelp/paasta/pull/1235) ([mattmb](https://github.com/mattmb))

## [v0.63.4](https://github.com/Yelp/paasta/tree/v0.63.4) (2017-05-16)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.63.3...v0.63.4)

**Merged pull requests:**

- Make paasta check -y look at the correct git\_url [\#1232](https://github.com/Yelp/paasta/pull/1232) ([nhandler](https://github.com/nhandler))
- InstanceConfig is not a dict [\#1231](https://github.com/Yelp/paasta/pull/1231) ([bchess](https://github.com/bchess))
- Support adhoc.yaml in paasta check  [\#1225](https://github.com/Yelp/paasta/pull/1225) ([oktopuz](https://github.com/oktopuz))
- Add a timeout for when git server cannot be reached [\#1223](https://github.com/Yelp/paasta/pull/1223) ([jglukasik](https://github.com/jglukasik))
- reduce v2/tasks call in check marathon replication [\#1222](https://github.com/Yelp/paasta/pull/1222) ([Rob-Johnson](https://github.com/Rob-Johnson))
- Package task processing [\#1221](https://github.com/Yelp/paasta/pull/1221) ([jolynch](https://github.com/jolynch))
- Don't log when bounce "doesn't do anything" [\#1217](https://github.com/Yelp/paasta/pull/1217) ([jglukasik](https://github.com/jglukasik))
- Implement a forecaster that uses linear regression over a trailing window to estimate slope and level of the load. [\#1211](https://github.com/Yelp/paasta/pull/1211) ([EvanKrall](https://github.com/EvanKrall))
- executor\_id is always equal to task\_id [\#1208](https://github.com/Yelp/paasta/pull/1208) ([oktopuz](https://github.com/oktopuz))

## [v0.63.3](https://github.com/Yelp/paasta/tree/v0.63.3) (2017-05-11)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.63.2...v0.63.3)

**Merged pull requests:**

- \[taskproc\] fix async example [\#1220](https://github.com/Yelp/paasta/pull/1220) ([keymone](https://github.com/keymone))
- \[taskproc\] use secret file for authentication [\#1219](https://github.com/Yelp/paasta/pull/1219) ([Rob-Johnson](https://github.com/Rob-Johnson))
- \[taskproc\] subscription runner and more [\#1218](https://github.com/Yelp/paasta/pull/1218) ([keymone](https://github.com/keymone))
- Add maintenance + public config watchers [\#1188](https://github.com/Yelp/paasta/pull/1188) ([mattmb](https://github.com/mattmb))

## [v0.63.2](https://github.com/Yelp/paasta/tree/v0.63.2) (2017-05-11)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.63.1...v0.63.2)

**Merged pull requests:**

- When data is missing for a task, ignore it, instead of having it skew utilization downwards. [\#1215](https://github.com/Yelp/paasta/pull/1215) ([EvanKrall](https://github.com/EvanKrall))
- Runnable task examples \(PR for PR\) [\#1213](https://github.com/Yelp/paasta/pull/1213) ([keymone](https://github.com/keymone))
- proportional decision policy: clamp to min/max instances even if load is good enough. [\#1212](https://github.com/Yelp/paasta/pull/1212) ([EvanKrall](https://github.com/EvanKrall))
- Initial implementation of task-processor [\#1210](https://github.com/Yelp/paasta/pull/1210) ([Rob-Johnson](https://github.com/Rob-Johnson))
- Upgrade yelp-clog [\#1209](https://github.com/Yelp/paasta/pull/1209) ([asottile](https://github.com/asottile))
- wait-for-deployment will no longer block on Deploying or Waiting \#1169 [\#1193](https://github.com/Yelp/paasta/pull/1193) ([somic](https://github.com/somic))

## [v0.63.1](https://github.com/Yelp/paasta/tree/v0.63.1) (2017-05-10)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.63.0...v0.63.1)

**Merged pull requests:**

- Don't pingpong with sys.modules for \_\_file\_\_ [\#1207](https://github.com/Yelp/paasta/pull/1207) ([asottile](https://github.com/asottile))
- Don't install python2.6 backports for paasta [\#1206](https://github.com/Yelp/paasta/pull/1206) ([asottile](https://github.com/asottile))
- ensure adhoc scheduler service config has cmd [\#1201](https://github.com/Yelp/paasta/pull/1201) ([keymone](https://github.com/keymone))
- Catch NoSlavesAvailableError errors. PAASTA-8692 [\#1200](https://github.com/Yelp/paasta/pull/1200) ([solarkennedy](https://github.com/solarkennedy))
- Fix tags being null when checking registry [\#1199](https://github.com/Yelp/paasta/pull/1199) ([mattmb](https://github.com/mattmb))
- \[WIP\] Task processing: fleshing out the interface [\#1198](https://github.com/Yelp/paasta/pull/1198) ([keymone](https://github.com/keymone))
- connect to random master for remote-run [\#1197](https://github.com/Yelp/paasta/pull/1197) ([keymone](https://github.com/keymone))
- Task proc scheduler [\#1194](https://github.com/Yelp/paasta/pull/1194) ([Rob-Johnson](https://github.com/Rob-Johnson))
- Add cluster name to deployd metrics [\#1187](https://github.com/Yelp/paasta/pull/1187) ([mattmb](https://github.com/mattmb))



\* *This Change Log was automatically generated by [github_changelog_generator](https://github.com/skywinder/Github-Changelog-Generator)*
