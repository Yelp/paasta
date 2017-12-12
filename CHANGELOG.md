# Change Log

## [0.69.6](https://github.com/Yelp/paasta/tree/0.69.6) (2017-12-07)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.69.5...0.69.6)

**Merged pull requests:**

- Perfscript [\#1649](https://github.com/Yelp/paasta/pull/1649) ([fboxwala](https://github.com/fboxwala))
- Make metastatus able to group by multiple values properly [\#1642](https://github.com/Yelp/paasta/pull/1642) ([EvanKrall](https://github.com/EvanKrall))
- pytest.fixture for SystemPaastaConfig  [\#1641](https://github.com/Yelp/paasta/pull/1641) ([oktopuz](https://github.com/oktopuz))
- Instance configs experiments [\#1492](https://github.com/Yelp/paasta/pull/1492) ([oktopuz](https://github.com/oktopuz))

## [v0.69.5](https://github.com/Yelp/paasta/tree/v0.69.5) (2017-12-06)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.69.4...v0.69.5)

**Merged pull requests:**

- Fixes a small typo in the autoscaling documentation [\#1650](https://github.com/Yelp/paasta/pull/1650) ([fhats](https://github.com/fhats))
- A few python3 things [\#1648](https://github.com/Yelp/paasta/pull/1648) ([asottile](https://github.com/asottile))
- Make marathon\_dashboard work for non-sharded clusters [\#1647](https://github.com/Yelp/paasta/pull/1647) ([nhandler](https://github.com/nhandler))
- add variables to string interpolation [\#1645](https://github.com/Yelp/paasta/pull/1645) ([Rob-Johnson](https://github.com/Rob-Johnson))

## [v0.69.4](https://github.com/Yelp/paasta/tree/v0.69.4) (2017-12-05)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.69.3...v0.69.4)

**Merged pull requests:**

- Fix mypy for cluster\_boost [\#1646](https://github.com/Yelp/paasta/pull/1646) ([nhandler](https://github.com/nhandler))
- Initial Support for a Sharded Marathon Dashboard [\#1601](https://github.com/Yelp/paasta/pull/1601) ([nhandler](https://github.com/nhandler))

## [v0.69.3](https://github.com/Yelp/paasta/tree/v0.69.3) (2017-12-04)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.69.1...v0.69.3)

**Merged pull requests:**

- v0.69.2 [\#1644](https://github.com/Yelp/paasta/pull/1644) ([oktopuz](https://github.com/oktopuz))
- paasta local-run --no-healthcheck shouldn't publish a container's ports [\#1643](https://github.com/Yelp/paasta/pull/1643) ([oktopuz](https://github.com/oktopuz))
- Adding Cluster Boost feature \(ready to merge\) [\#1587](https://github.com/Yelp/paasta/pull/1587) ([matfra](https://github.com/matfra))

## [v0.69.1](https://github.com/Yelp/paasta/tree/v0.69.1) (2017-12-01)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.69.0...v0.69.1)

**Merged pull requests:**

- Fix paused autoscaler logic bug [\#1640](https://github.com/Yelp/paasta/pull/1640) ([solarkennedy](https://github.com/solarkennedy))

## [v0.69.0](https://github.com/Yelp/paasta/tree/v0.69.0) (2017-11-30)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.68.12...v0.69.0)

**Merged pull requests:**

- Convert cli/cmds/sysdig.py to sharded marathon and clean up unsharded code/test config. [\#1628](https://github.com/Yelp/paasta/pull/1628) ([EvanKrall](https://github.com/EvanKrall))

## [v0.68.12](https://github.com/Yelp/paasta/tree/v0.68.12) (2017-11-29)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.68.11...v0.68.12)

**Merged pull requests:**

- Pass MarathonClients, not list of MarathonClient, to get\_autoscaling\_info in status\_marathon\_job\_verbose. [\#1639](https://github.com/Yelp/paasta/pull/1639) ([EvanKrall](https://github.com/EvanKrall))
- Don't docker-compose pull in example\_cluster; see bc3029fb3f01eadb931105eea480c9ec21c18ea8 [\#1638](https://github.com/Yelp/paasta/pull/1638) ([EvanKrall](https://github.com/EvanKrall))
- Show marathon dashboard links in paasta status [\#1636](https://github.com/Yelp/paasta/pull/1636) ([oktopuz](https://github.com/oktopuz))
- Tweak mypy settings to allow for more granular type checking [\#1627](https://github.com/Yelp/paasta/pull/1627) ([EvanKrall](https://github.com/EvanKrall))

## [v0.68.11](https://github.com/Yelp/paasta/tree/v0.68.11) (2017-11-28)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.68.10...v0.68.11)

## [v0.68.10](https://github.com/Yelp/paasta/tree/v0.68.10) (2017-11-27)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.68.9...v0.68.10)

**Merged pull requests:**

- Add feature to manually pause the autoscaler [\#1622](https://github.com/Yelp/paasta/pull/1622) ([fboxwala](https://github.com/fboxwala))

## [v0.68.9](https://github.com/Yelp/paasta/tree/v0.68.9) (2017-11-23)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.68.8...v0.68.9)

**Closed issues:**

- Cluster autoscaler gives up if too many slaves aren't found when scaling down cancelled\_running resource [\#1619](https://github.com/Yelp/paasta/issues/1619)

**Merged pull requests:**

- Fix TypeError in status\_marathon\_job\_verbose [\#1635](https://github.com/Yelp/paasta/pull/1635) ([oktopuz](https://github.com/oktopuz))
- fix bug in autoscaler for sharded marathon [\#1634](https://github.com/Yelp/paasta/pull/1634) ([Rob-Johnson](https://github.com/Rob-Johnson))
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



\* *This Change Log was automatically generated by [github_changelog_generator](https://github.com/skywinder/Github-Changelog-Generator)*
