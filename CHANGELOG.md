# Change Log

## [Unreleased](https://github.com/Yelp/paasta/tree/HEAD)

[Full Changelog](https://github.com/Yelp/paasta/compare/v0.68.8...HEAD)

**Closed issues:**

- Cluster autoscaler gives up if too many slaves aren't found when scaling down cancelled\_running resource [\#1619](https://github.com/Yelp/paasta/issues/1619)

**Merged pull requests:**

- Support arrays in dashboard\_links.json [\#1633](https://github.com/Yelp/paasta/pull/1633) ([oktopuz](https://github.com/oktopuz))
- Allows the cluster autoscaler to act on cancelled\_running sfrs with missing instances [\#1620](https://github.com/Yelp/paasta/pull/1620) ([matthewbentley](https://github.com/matthewbentley))
- Always fix the slack of old marathon apps when bouncing [\#1605](https://github.com/Yelp/paasta/pull/1605) ([solarkennedy](https://github.com/solarkennedy))
- deprecates PID policy [\#1579](https://github.com/Yelp/paasta/pull/1579) ([fboxwala](https://github.com/fboxwala))

## [v0.68.8](https://github.com/Yelp/paasta/tree/v0.68.8) (2017-11-20)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.68.7...v0.68.8)

**Merged pull requests:**

- P1: Fix metrics names [\#1631](https://github.com/Yelp/paasta/pull/1631) ([matthewbentley](https://github.com/matthewbentley))
- Always consider num\_at\_risk\_tasks when calculating instance\_counts du… [\#1623](https://github.com/Yelp/paasta/pull/1623) ([solarkennedy](https://github.com/solarkennedy))

## [v0.68.7](https://github.com/Yelp/paasta/tree/v0.68.7) (2017-11-20)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.68.6...v0.68.7)

**Closed issues:**

- RFC: Emit metrics from cluster autoscaler [\#1600](https://github.com/Yelp/paasta/issues/1600)

**Merged pull requests:**

- Reuse paasta\_itest images [\#1625](https://github.com/Yelp/paasta/pull/1625) ([oktopuz](https://github.com/oktopuz))
- Move deployd metrics to be generic [\#1621](https://github.com/Yelp/paasta/pull/1621) ([matthewbentley](https://github.com/matthewbentley))
- Emit metrics from cluster autoscaler [\#1614](https://github.com/Yelp/paasta/pull/1614) ([matthewbentley](https://github.com/matthewbentley))
- Adjust check\_mesos\_duplicate\_frameworks for sharding [\#1609](https://github.com/Yelp/paasta/pull/1609) ([oktopuz](https://github.com/oktopuz))

## [v0.68.6](https://github.com/Yelp/paasta/tree/v0.68.6) (2017-11-14)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.68.5...v0.68.6)

**Merged pull requests:**

- Fix silly bug in service autoscaler where I was passing a MarathonClients object instead of a list of MarathonClient objects. [\#1626](https://github.com/Yelp/paasta/pull/1626) ([EvanKrall](https://github.com/EvanKrall))

## [v0.68.5](https://github.com/Yelp/paasta/tree/v0.68.5) (2017-11-13)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.68.4...v0.68.5)

**Merged pull requests:**

- Updated task-processing to 0.0.5 [\#1624](https://github.com/Yelp/paasta/pull/1624) ([huadongliu](https://github.com/huadongliu))
- Make service autoscaler work with multiple marathon shards. [\#1618](https://github.com/Yelp/paasta/pull/1618) ([EvanKrall](https://github.com/EvanKrall))

## [v0.68.4](https://github.com/Yelp/paasta/tree/v0.68.4) (2017-11-10)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.68.3...v0.68.4)

**Merged pull requests:**

- Make PaaSTA API work with sharded marathon [\#1617](https://github.com/Yelp/paasta/pull/1617) ([oktopuz](https://github.com/oktopuz))
- RFC: Add cluster autoscaler docs [\#1616](https://github.com/Yelp/paasta/pull/1616) ([matthewbentley](https://github.com/matthewbentley))
- Make "paasta local-run" try to use the same port repeatedly [\#1615](https://github.com/Yelp/paasta/pull/1615) ([chriskuehl](https://github.com/chriskuehl))
- make graceful-app-drain using marathon sharded code [\#1613](https://github.com/Yelp/paasta/pull/1613) ([oktopuz](https://github.com/oktopuz))
- Make paasta local-run lock on pulls [\#1608](https://github.com/Yelp/paasta/pull/1608) ([solarkennedy](https://github.com/solarkennedy))
- Add bounce logic for json secret files [\#1607](https://github.com/Yelp/paasta/pull/1607) ([mattmb](https://github.com/mattmb))
- skip unreachable tasks when draining [\#1595](https://github.com/Yelp/paasta/pull/1595) ([somic](https://github.com/somic))
- Added type annotations to generate\_deployments\_for\_service [\#1594](https://github.com/Yelp/paasta/pull/1594) ([solarkennedy](https://github.com/solarkennedy))
- Make check\_marathon\_services\_replication work with sharded marathon [\#1590](https://github.com/Yelp/paasta/pull/1590) ([EvanKrall](https://github.com/EvanKrall))

## [v0.68.3](https://github.com/Yelp/paasta/tree/v0.68.3) (2017-11-08)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.68.2...v0.68.3)

**Merged pull requests:**

- increase read timeout for maintenance api calls [\#1612](https://github.com/Yelp/paasta/pull/1612) ([somic](https://github.com/somic))
- Make paasta\_get\_num\_deployments using marathon sharded code [\#1611](https://github.com/Yelp/paasta/pull/1611) ([oktopuz](https://github.com/oktopuz))
- delete\_old\_marathon\_deployments to use marathon sharded code [\#1610](https://github.com/Yelp/paasta/pull/1610) ([oktopuz](https://github.com/oktopuz))
- Cluster autoscaler typing [\#1606](https://github.com/Yelp/paasta/pull/1606) ([matthewbentley](https://github.com/matthewbentley))
- Fixed paasta\_remote\_run and added task logging executor [\#1603](https://github.com/Yelp/paasta/pull/1603) ([huadongliu](https://github.com/huadongliu))

## [v0.68.2](https://github.com/Yelp/paasta/tree/v0.68.2) (2017-11-06)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.68.1...v0.68.2)

**Closed issues:**

- make autoscaling\_lib unit tests not sleep [\#1589](https://github.com/Yelp/paasta/issues/1589)

**Merged pull requests:**

- extract\_args doesn't return a constraint [\#1602](https://github.com/Yelp/paasta/pull/1602) ([Rob-Johnson](https://github.com/Rob-Johnson))
- Remove unnessesary calls to sleep in test\_autoscaling\_cluster\_lib [\#1599](https://github.com/Yelp/paasta/pull/1599) ([matthewbentley](https://github.com/matthewbentley))
- paasta status: fix string interpolation in error message [\#1598](https://github.com/Yelp/paasta/pull/1598) ([chriskuehl](https://github.com/chriskuehl))
- check\_mesos\_outdated\_tasks [\#1596](https://github.com/Yelp/paasta/pull/1596) ([oktopuz](https://github.com/oktopuz))
- \[cluster autoscaler\] expand logic around scaling cancelled resources [\#1584](https://github.com/Yelp/paasta/pull/1584) ([matthewbentley](https://github.com/matthewbentley))

## [v0.68.1](https://github.com/Yelp/paasta/tree/v0.68.1) (2017-11-02)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.68.0...v0.68.1)

**Merged pull requests:**

- Pass a list of clients, not a MarathonClients object, to get\_marathon\_apps\_with\_clients in get\_at\_risk\_service\_instances. Add type annotations to deployd.common and deployd.watchers and their tests. [\#1597](https://github.com/Yelp/paasta/pull/1597) ([EvanKrall](https://github.com/EvanKrall))
- allow specifying pool in adhoc schema [\#1593](https://github.com/Yelp/paasta/pull/1593) ([Rob-Johnson](https://github.com/Rob-Johnson))
- Fix autoscaling info for metastatus [\#1592](https://github.com/Yelp/paasta/pull/1592) ([matthewbentley](https://github.com/matthewbentley))
- Fix error when a pool isn't ready when the cluster autoscaler tries to run [\#1591](https://github.com/Yelp/paasta/pull/1591) ([matthewbentley](https://github.com/matthewbentley))
- uses meteorite to push autoscale events to signalfx [\#1588](https://github.com/Yelp/paasta/pull/1588) ([fboxwala](https://github.com/fboxwala))
- set the pool correctly in remote-run launches [\#1577](https://github.com/Yelp/paasta/pull/1577) ([Rob-Johnson](https://github.com/Rob-Johnson))

## [v0.68.0](https://github.com/Yelp/paasta/tree/v0.68.0) (2017-10-31)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.67.22...v0.68.0)

**Closed issues:**

- make cluster autoscaler only call state once [\#1583](https://github.com/Yelp/paasta/issues/1583)
- Add Toggles for deployd Watchers [\#1542](https://github.com/Yelp/paasta/issues/1542)
- remote\_run doesn't do reconciliation? [\#1540](https://github.com/Yelp/paasta/issues/1540)

**Merged pull requests:**

- Only call state once on the cluster autoscaler [\#1586](https://github.com/Yelp/paasta/pull/1586) ([matthewbentley](https://github.com/matthewbentley))
- adds functionality to enable specific watchers in deployd [\#1573](https://github.com/Yelp/paasta/pull/1573) ([fboxwala](https://github.com/fboxwala))
- Rendezvous hashing and sharded setup\_marathon\_job, cleanup\_marathon\_job, marathon\_serviceinit, list\_marathon\_service\_instances [\#1552](https://github.com/Yelp/paasta/pull/1552) ([EvanKrall](https://github.com/EvanKrall))

## [v0.67.22](https://github.com/Yelp/paasta/tree/v0.67.22) (2017-10-26)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.67.21...v0.67.22)

**Closed issues:**

- Autoscaling docs are not up-to-date [\#1487](https://github.com/Yelp/paasta/issues/1487)

**Merged pull requests:**

- Update autoscaling docs [\#1585](https://github.com/Yelp/paasta/pull/1585) ([fboxwala](https://github.com/fboxwala))

## [v0.67.21](https://github.com/Yelp/paasta/tree/v0.67.21) (2017-10-26)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.67.20...v0.67.21)

**Merged pull requests:**

- The cluster autoscaler needs to full state to calulate usage [\#1582](https://github.com/Yelp/paasta/pull/1582) ([matthewbentley](https://github.com/matthewbentley))
- do not undrain unreachable tasks [\#1580](https://github.com/Yelp/paasta/pull/1580) ([somic](https://github.com/somic))
- reduce http timeout in mesos\_maintenance [\#1578](https://github.com/Yelp/paasta/pull/1578) ([somic](https://github.com/somic))
- include expiration only when draining in HacheckDrainMethod [\#1575](https://github.com/Yelp/paasta/pull/1575) ([somic](https://github.com/somic))
- reduce hacheck timeout in drain\_lib [\#1571](https://github.com/Yelp/paasta/pull/1571) ([somic](https://github.com/somic))
- Only try to unreserve maint reserved resources if there are some [\#1567](https://github.com/Yelp/paasta/pull/1567) ([nhandler](https://github.com/nhandler))
- Update setup.py to use marathon \>= 0.9.2 [\#1564](https://github.com/Yelp/paasta/pull/1564) ([jglukasik](https://github.com/jglukasik))

## [v0.67.20](https://github.com/Yelp/paasta/tree/v0.67.20) (2017-10-24)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.67.19...v0.67.20)

## [v0.67.19](https://github.com/Yelp/paasta/tree/v0.67.19) (2017-10-24)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.67.18...v0.67.19)

**Merged pull requests:**

- Fix cleanup\_chronos\_jobs for jobs with no interval [\#1576](https://github.com/Yelp/paasta/pull/1576) ([oktopuz](https://github.com/oktopuz))
- Allow deployd to handle the case where a service is not configured. F… [\#1574](https://github.com/Yelp/paasta/pull/1574) ([solarkennedy](https://github.com/solarkennedy))
- make paasta metastatus marathon-shard aware [\#1558](https://github.com/Yelp/paasta/pull/1558) ([oktopuz](https://github.com/oktopuz))

## [v0.67.18](https://github.com/Yelp/paasta/tree/v0.67.18) (2017-10-23)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.67.17...v0.67.18)

**Merged pull requests:**

- Filter out slaves from the slaves list if not in mesos state [\#1560](https://github.com/Yelp/paasta/pull/1560) ([matthewbentley](https://github.com/matthewbentley))
- Async scale down cluster resource \(ie parallel draining+killing of slaves in an sfr/asg\) [\#1496](https://github.com/Yelp/paasta/pull/1496) ([matthewbentley](https://github.com/matthewbentley))

## [v0.67.17](https://github.com/Yelp/paasta/tree/v0.67.17) (2017-10-23)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.67.16...v0.67.17)

**Closed issues:**

- deployd crashes on startup if a marathon app exists for nonexistant service [\#1565](https://github.com/Yelp/paasta/issues/1565)

**Merged pull requests:**

- use caching clients in metastatus [\#1570](https://github.com/Yelp/paasta/pull/1570) ([keymone](https://github.com/keymone))
- replace mesos.cfg.Config [\#1569](https://github.com/Yelp/paasta/pull/1569) ([keymone](https://github.com/keymone))

## [v0.67.16](https://github.com/Yelp/paasta/tree/v0.67.16) (2017-10-20)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.67.15...v0.67.16)

**Merged pull requests:**

- Handle being unable to get a tmp job's full config [\#1572](https://github.com/Yelp/paasta/pull/1572) ([jglukasik](https://github.com/jglukasik))

## [v0.67.15](https://github.com/Yelp/paasta/tree/v0.67.15) (2017-10-20)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.67.14...v0.67.15)

**Merged pull requests:**

- Bump up taskproc [\#1568](https://github.com/Yelp/paasta/pull/1568) ([huadongliu](https://github.com/huadongliu))
- fix mypy failures [\#1566](https://github.com/Yelp/paasta/pull/1566) ([somic](https://github.com/somic))
- Make chronos itest use xenial and upate gpg key for bintray [\#1563](https://github.com/Yelp/paasta/pull/1563) ([solarkennedy](https://github.com/solarkennedy))
- Update taskproc [\#1561](https://github.com/Yelp/paasta/pull/1561) ([huadongliu](https://github.com/huadongliu))
- Only remove tmp jobs if they finished longer than one schedule interv… [\#1559](https://github.com/Yelp/paasta/pull/1559) ([jglukasik](https://github.com/jglukasik))
- Allow metastatus to use cached mesos state and frameworks [\#1556](https://github.com/Yelp/paasta/pull/1556) ([keymone](https://github.com/keymone))
- add broadcast\_log\_all\_services\_running\_here [\#1554](https://github.com/Yelp/paasta/pull/1554) ([somic](https://github.com/somic))
- Added sharded marathon servers to the paasta\_itests and example\_cluster [\#1534](https://github.com/Yelp/paasta/pull/1534) ([solarkennedy](https://github.com/solarkennedy))

## [v0.67.14](https://github.com/Yelp/paasta/tree/v0.67.14) (2017-10-16)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.67.13...v0.67.14)

**Closed issues:**

- include info on why offers are being rejected for a task in paasta status [\#1408](https://github.com/Yelp/paasta/issues/1408)

**Merged pull requests:**

- Add potential reasons for app deployments being stalled to paasta status and api [\#1557](https://github.com/Yelp/paasta/pull/1557) ([matthewbentley](https://github.com/matthewbentley))
- Add type annotations to marathon-related things [\#1551](https://github.com/Yelp/paasta/pull/1551) ([EvanKrall](https://github.com/EvanKrall))

## [v0.67.13](https://github.com/Yelp/paasta/tree/v0.67.13) (2017-10-11)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.67.12...v0.67.13)

**Merged pull requests:**

- Don't fail wait\_for\_deployment if no marathon instances match the deploy group in question. [\#1555](https://github.com/Yelp/paasta/pull/1555) ([EvanKrall](https://github.com/EvanKrall))
- Use sensu-plugin-python from pypi [\#1553](https://github.com/Yelp/paasta/pull/1553) ([asottile](https://github.com/asottile))
- Paasta 13123 another way [\#1550](https://github.com/Yelp/paasta/pull/1550) ([oktopuz](https://github.com/oktopuz))
- Use a uuid when doing local-run to give them unique ids [\#1549](https://github.com/Yelp/paasta/pull/1549) ([solarkennedy](https://github.com/solarkennedy))
- Upgrade pre-commit and migrate to pygrep [\#1548](https://github.com/Yelp/paasta/pull/1548) ([asottile](https://github.com/asottile))

## [v0.67.12](https://github.com/Yelp/paasta/tree/v0.67.12) (2017-10-06)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.67.11...v0.67.12)

**Merged pull requests:**

- Only fall for mesos task counts on the paasta api in verbose mode [\#1547](https://github.com/Yelp/paasta/pull/1547) ([solarkennedy](https://github.com/solarkennedy))
- Make all mesos api calls use the paasta user agent [\#1546](https://github.com/Yelp/paasta/pull/1546) ([solarkennedy](https://github.com/solarkennedy))
- Revert "temporarily ignore orphan\_tasks in mesos\_tools.get\_all\_runnining\_tasks" [\#1545](https://github.com/Yelp/paasta/pull/1545) ([somic](https://github.com/somic))
- Make wait-for-deployment only inspect marathon instances [\#1544](https://github.com/Yelp/paasta/pull/1544) ([solarkennedy](https://github.com/solarkennedy))

## [v0.67.11](https://github.com/Yelp/paasta/tree/v0.67.11) (2017-10-05)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.67.10...v0.67.11)

**Closed issues:**

- Cluster autoscaler crashes on mesos \(marathon?\) leader election [\#1503](https://github.com/Yelp/paasta/issues/1503)

**Merged pull requests:**

- Make paasta status ignore adhoc instances again [\#1539](https://github.com/Yelp/paasta/pull/1539) ([solarkennedy](https://github.com/solarkennedy))
- local-run will detach only if healthcheck is needed [\#1538](https://github.com/Yelp/paasta/pull/1538) ([somic](https://github.com/somic))
- temporarily ignore orphan\_tasks in mesos\_tools.get\_all\_running\_tasks [\#1537](https://github.com/Yelp/paasta/pull/1537) ([somic](https://github.com/somic))
- Make cluster autoscaler gracefully handle a mesos leader election [\#1507](https://github.com/Yelp/paasta/pull/1507) ([matthewbentley](https://github.com/matthewbentley))

## [v0.67.10](https://github.com/Yelp/paasta/tree/v0.67.10) (2017-10-03)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.67.9...v0.67.10)

**Merged pull requests:**

- Fix typo in api swagger [\#1536](https://github.com/Yelp/paasta/pull/1536) ([EvanKrall](https://github.com/EvanKrall))
- Emit meteorite counters to track paasta\_remote\_run failures [\#1535](https://github.com/Yelp/paasta/pull/1535) ([huadongliu](https://github.com/huadongliu))

## [v0.67.9](https://github.com/Yelp/paasta/tree/v0.67.9) (2017-10-02)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.67.8...v0.67.9)

**Closed issues:**

- Sensu's python plugin is looking for a maintainer [\#1529](https://github.com/Yelp/paasta/issues/1529)

**Merged pull requests:**

- Append previous clients to the right list [\#1533](https://github.com/Yelp/paasta/pull/1533) ([EvanKrall](https://github.com/EvanKrall))
- Add yelp\_paasta\_helpers to extra\_requirements\_yelp [\#1531](https://github.com/Yelp/paasta/pull/1531) ([oktopuz](https://github.com/oktopuz))
- Fix typo in docs [\#1530](https://github.com/Yelp/paasta/pull/1530) ([chriskuehl](https://github.com/chriskuehl))
- remove limited instance count warning [\#1527](https://github.com/Yelp/paasta/pull/1527) ([somic](https://github.com/somic))
- Updating security-check message [\#1526](https://github.com/Yelp/paasta/pull/1526) ([transcedentalia](https://github.com/transcedentalia))
- Add more debug messages to the cluster autoscaler [\#1525](https://github.com/Yelp/paasta/pull/1525) ([matthewbentley](https://github.com/matthewbentley))
- Apollo 205 detector status 36b [\#1524](https://github.com/Yelp/paasta/pull/1524) ([philipmulcahy](https://github.com/philipmulcahy))
- Added in config loading and client generation for sharded marathon configurations [\#1523](https://github.com/Yelp/paasta/pull/1523) ([solarkennedy](https://github.com/solarkennedy))
- Add chronos to status api [\#1491](https://github.com/Yelp/paasta/pull/1491) ([matthewbentley](https://github.com/matthewbentley))

## [v0.67.8](https://github.com/Yelp/paasta/tree/v0.67.8) (2017-09-25)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.67.7...v0.67.8)

**Closed issues:**

- paasta\_cleanup\_maintenance is trying to remove resources for taskproc [\#1519](https://github.com/Yelp/paasta/issues/1519)

**Merged pull requests:**

- \[remote-run\] log verbosity from args [\#1522](https://github.com/Yelp/paasta/pull/1522) ([keymone](https://github.com/keymone))
- bump taskproc to current master [\#1521](https://github.com/Yelp/paasta/pull/1521) ([keymone](https://github.com/keymone))
- Only unreserve resources reserved for the purpose of maintenance [\#1520](https://github.com/Yelp/paasta/pull/1520) ([nhandler](https://github.com/nhandler))
- Enable check\_oom\_events for everyone [\#1515](https://github.com/Yelp/paasta/pull/1515) ([oktopuz](https://github.com/oktopuz))

## [v0.67.7](https://github.com/Yelp/paasta/tree/v0.67.7) (2017-09-22)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.67.6...v0.67.7)

**Merged pull requests:**

- Make cleanup maintenance pass if unreserving fails [\#1516](https://github.com/Yelp/paasta/pull/1516) ([mattmb](https://github.com/mattmb))

## [v0.67.6](https://github.com/Yelp/paasta/tree/v0.67.6) (2017-09-22)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.67.5...v0.67.6)

**Merged pull requests:**

- Fix typo s/resoures/resources/g [\#1518](https://github.com/Yelp/paasta/pull/1518) ([nhandler](https://github.com/nhandler))
- \[remote-run\] print nicer error messages [\#1517](https://github.com/Yelp/paasta/pull/1517) ([keymone](https://github.com/keymone))
- Consider tasks from all versions of a service when autoscaling. PAASTA-12983 [\#1513](https://github.com/Yelp/paasta/pull/1513) ([EvanKrall](https://github.com/EvanKrall))
- use MESOS\_HTTP{,s} healthchecks everywhere [\#1512](https://github.com/Yelp/paasta/pull/1512) ([Rob-Johnson](https://github.com/Rob-Johnson))

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



\* *This Change Log was automatically generated by [github_changelog_generator](https://github.com/skywinder/Github-Changelog-Generator)*
