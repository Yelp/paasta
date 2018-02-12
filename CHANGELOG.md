# Change Log

## [0.69.28](https://github.com/Yelp/paasta/tree/0.69.28) (2018-02-12)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.69.27...0.69.28)

**Merged pull requests:**

- Update marathon schema to allow instance names to begin with an underscore [\#1725](https://github.com/Yelp/paasta/pull/1725) ([nhandler](https://github.com/nhandler))
- Rounding capacity to terminate too [\#1724](https://github.com/Yelp/paasta/pull/1724) ([matfra](https://github.com/matfra))

## [v0.69.27](https://github.com/Yelp/paasta/tree/v0.69.27) (2018-02-07)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.69.26...v0.69.27)

**Merged pull requests:**

- Give cpu\_burst\_allowance a 10% margin for underprovisioned services [\#1723](https://github.com/Yelp/paasta/pull/1723) ([solarkennedy](https://github.com/solarkennedy))
- Stop allowing nerve\_ns in marathon files [\#1721](https://github.com/Yelp/paasta/pull/1721) ([nhandler](https://github.com/nhandler))
- Use marathon 1.4.11 in itests [\#1720](https://github.com/Yelp/paasta/pull/1720) ([nhandler](https://github.com/nhandler))
- Revert "Drop paasta lucid image" [\#1719](https://github.com/Yelp/paasta/pull/1719) ([macisamuele](https://github.com/macisamuele))
- Fix mesos dockerfile \(example\_cluster\) and minor docker file updates [\#1718](https://github.com/Yelp/paasta/pull/1718) ([macisamuele](https://github.com/macisamuele))
- Make paasta status display information about all marathon apps, even without -v. [\#1717](https://github.com/Yelp/paasta/pull/1717) ([EvanKrall](https://github.com/EvanKrall))
- paasta deployd watches for new watches when autoscaling is enabled first time [\#1716](https://github.com/Yelp/paasta/pull/1716) ([chlgit](https://github.com/chlgit))
- Run jupyter notebook as non-root, added spark driver options [\#1715](https://github.com/Yelp/paasta/pull/1715) ([huadongliu](https://github.com/huadongliu))
- Make paasta local-run detach only when there is a healthcheck to run. [\#1712](https://github.com/Yelp/paasta/pull/1712) ([solarkennedy](https://github.com/solarkennedy))
- Update disk docs [\#1710](https://github.com/Yelp/paasta/pull/1710) ([solarkennedy](https://github.com/solarkennedy))

## [v0.69.26](https://github.com/Yelp/paasta/tree/v0.69.26) (2018-01-23)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.69.24...v0.69.26)

**Merged pull requests:**

- 1st iteration of paasta spark-run [\#1709](https://github.com/Yelp/paasta/pull/1709) ([huadongliu](https://github.com/huadongliu))

## [v0.69.24](https://github.com/Yelp/paasta/tree/v0.69.24) (2018-01-19)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.69.23...v0.69.24)

**Merged pull requests:**

- Filtering out the delta cpu\_time, not absolute cpu\_time [\#1708](https://github.com/Yelp/paasta/pull/1708) ([matfra](https://github.com/matfra))

## [v0.69.23](https://github.com/Yelp/paasta/tree/v0.69.23) (2018-01-18)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.69.22...v0.69.23)

**Closed issues:**

- Old Marathon apps can fail to be cleaned up \(stuck bounces\) [\#1706](https://github.com/Yelp/paasta/issues/1706)

**Merged pull requests:**

- Make paasta\_tools.bounce\_lib.kill\_old\_ids raise exceptions for deployd [\#1707](https://github.com/Yelp/paasta/pull/1707) ([nhandler](https://github.com/nhandler))
- Rename the bespoke-autoscale command to just 'autoscale' [\#1705](https://github.com/Yelp/paasta/pull/1705) ([solarkennedy](https://github.com/solarkennedy))
- Always emit 'instances' to signalfx for knowing what the desired instance count is [\#1704](https://github.com/Yelp/paasta/pull/1704) ([solarkennedy](https://github.com/solarkennedy))
- Make sure tasks in old\_app\_at\_risk\_tasks get passed to bounce\_func. [\#1703](https://github.com/Yelp/paasta/pull/1703) ([EvanKrall](https://github.com/EvanKrall))
- Add missing space [\#1702](https://github.com/Yelp/paasta/pull/1702) ([nhandler](https://github.com/nhandler))
- Tweaks to paasta rightsizer script. [\#1701](https://github.com/Yelp/paasta/pull/1701) ([thebostik](https://github.com/thebostik))

## [v0.69.22](https://github.com/Yelp/paasta/tree/v0.69.22) (2018-01-15)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.69.21...v0.69.22)

**Merged pull requests:**

- Check marathon replication on non-default reg [\#1700](https://github.com/Yelp/paasta/pull/1700) ([mattmb](https://github.com/mattmb))
- Tried to clarify the paasta stop message again [\#1691](https://github.com/Yelp/paasta/pull/1691) ([solarkennedy](https://github.com/solarkennedy))

## [v0.69.21](https://github.com/Yelp/paasta/tree/v0.69.21) (2018-01-12)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.69.20...v0.69.21)

**Merged pull requests:**

- Fix randomly closing ZK connections in paasta-api [\#1699](https://github.com/Yelp/paasta/pull/1699) ([mattmb](https://github.com/mattmb))
- validate error message improvement [\#1698](https://github.com/Yelp/paasta/pull/1698) ([chlgit](https://github.com/chlgit))

## [v0.69.20](https://github.com/Yelp/paasta/tree/v0.69.20) (2018-01-10)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.69.19...v0.69.20)

**Merged pull requests:**

- Close connections to Zookeeper properly [\#1697](https://github.com/Yelp/paasta/pull/1697) ([keymone](https://github.com/keymone))
- Revert the Mesos CPU sanity check and added Filter mesos cpu value [\#1690](https://github.com/Yelp/paasta/pull/1690) ([matfra](https://github.com/matfra))
- Enabled paasta\_remote\_run to connect to diffferent mesos clusters [\#1680](https://github.com/Yelp/paasta/pull/1680) ([huadongliu](https://github.com/huadongliu))

## [v0.69.19](https://github.com/Yelp/paasta/tree/v0.69.19) (2018-01-10)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.69.18...v0.69.19)

**Merged pull requests:**

- Reduce deployd log verbosity [\#1695](https://github.com/Yelp/paasta/pull/1695) ([mattmb](https://github.com/mattmb))
- Bump drain timeout for hacheck [\#1694](https://github.com/Yelp/paasta/pull/1694) ([mattmb](https://github.com/mattmb))

## [v0.69.18](https://github.com/Yelp/paasta/tree/v0.69.18) (2018-01-10)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.69.17...v0.69.18)

**Merged pull requests:**

- When calling deploy\_marathon\_service from deployd, only fetch apps from relevant marathons. [\#1692](https://github.com/Yelp/paasta/pull/1692) ([EvanKrall](https://github.com/EvanKrall))
- Use marathon 1.4.10 in itests [\#1689](https://github.com/Yelp/paasta/pull/1689) ([nhandler](https://github.com/nhandler))
- Added a script to emit additional paasta metrics [\#1674](https://github.com/Yelp/paasta/pull/1674) ([solarkennedy](https://github.com/solarkennedy))
- Added a bespoke-autoscale cli helper [\#1671](https://github.com/Yelp/paasta/pull/1671) ([solarkennedy](https://github.com/solarkennedy))

## [v0.69.17](https://github.com/Yelp/paasta/tree/v0.69.17) (2018-01-02)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.69.16...v0.69.17)

**Merged pull requests:**

- Fixed the output when asking for cluster boost status [\#1688](https://github.com/Yelp/paasta/pull/1688) ([matfra](https://github.com/matfra))

## [v0.69.16](https://github.com/Yelp/paasta/tree/v0.69.16) (2017-12-24)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.69.15...v0.69.16)

**Merged pull requests:**

- If you don't specify a default value, then dict.pop KeyErrors if the key doesn't exist. [\#1687](https://github.com/Yelp/paasta/pull/1687) ([EvanKrall](https://github.com/EvanKrall))
- Fixed override flag on paasta\_cluster\_boost and print status for any action [\#1686](https://github.com/Yelp/paasta/pull/1686) ([matfra](https://github.com/matfra))

## [v0.69.15](https://github.com/Yelp/paasta/tree/v0.69.15) (2017-12-22)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.69.14...v0.69.15)

**Merged pull requests:**

- Pop stats from cache instead of del'ing, so we don't get a KeyError if the stats aren't cached. [\#1684](https://github.com/Yelp/paasta/pull/1684) ([EvanKrall](https://github.com/EvanKrall))
- Perform paasta status ssh in parallel [\#1679](https://github.com/Yelp/paasta/pull/1679) ([henryzhangsta](https://github.com/henryzhangsta))

## [v0.69.14](https://github.com/Yelp/paasta/tree/v0.69.14) (2017-12-22)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.69.13...v0.69.14)

**Merged pull requests:**

- Bumping version of docker-compose in tox [\#1683](https://github.com/Yelp/paasta/pull/1683) ([matfra](https://github.com/matfra))
- Allow auto hostname unique in non-prod clusters [\#1682](https://github.com/Yelp/paasta/pull/1682) ([nhandler](https://github.com/nhandler))
- Ignore all .cache directories [\#1681](https://github.com/Yelp/paasta/pull/1681) ([henryzhangsta](https://github.com/henryzhangsta))
- Wait for deployment refactoring [\#1676](https://github.com/Yelp/paasta/pull/1676) ([oktopuz](https://github.com/oktopuz))

## [v0.69.13](https://github.com/Yelp/paasta/tree/v0.69.13) (2017-12-22)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.69.12...v0.69.13)

**Merged pull requests:**

- Better cluster boost [\#1677](https://github.com/Yelp/paasta/pull/1677) ([matfra](https://github.com/matfra))

## [v0.69.12](https://github.com/Yelp/paasta/tree/v0.69.12) (2017-12-21)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.69.11...v0.69.12)

**Merged pull requests:**

- Clear stats cache before fetching a second time [\#1678](https://github.com/Yelp/paasta/pull/1678) ([EvanKrall](https://github.com/EvanKrall))
- Remove extraneous `\# noqa` comments. [\#1675](https://github.com/Yelp/paasta/pull/1675) ([asottile](https://github.com/asottile))

## [v0.69.11](https://github.com/Yelp/paasta/tree/v0.69.11) (2017-12-21)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.69.10...v0.69.11)

**Merged pull requests:**

- Sanity-check mesos\_cpu data. [\#1669](https://github.com/Yelp/paasta/pull/1669) ([EvanKrall](https://github.com/EvanKrall))

## [v0.69.10](https://github.com/Yelp/paasta/tree/v0.69.10) (2017-12-20)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.69.9...v0.69.10)

**Closed issues:**

- Delete Me [\#1673](https://github.com/Yelp/paasta/issues/1673)

**Merged pull requests:**

- help wanted -- fix issue 842 [\#1670](https://github.com/Yelp/paasta/pull/1670) ([white105](https://github.com/white105))
- adds a try/except to update\_soa\_memcpu [\#1667](https://github.com/Yelp/paasta/pull/1667) ([fboxwala](https://github.com/fboxwala))
- Skip wait-for-deployment if mark-for-deployment failed [\#1666](https://github.com/Yelp/paasta/pull/1666) ([oktopuz](https://github.com/oktopuz))
- adds -p to update\_soa review branch command [\#1665](https://github.com/Yelp/paasta/pull/1665) ([fboxwala](https://github.com/fboxwala))
- Fix tail discovery, date handling and upgrade yelp\_clog [\#1662](https://github.com/Yelp/paasta/pull/1662) ([fede1024](https://github.com/fede1024))
- RFC: Automatically add \['hostname', 'UNIQUE'\] to small services [\#1652](https://github.com/Yelp/paasta/pull/1652) ([nhandler](https://github.com/nhandler))

## [v0.69.9](https://github.com/Yelp/paasta/tree/v0.69.9) (2017-12-15)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.69.8...v0.69.9)

**Merged pull requests:**

- Avoid using default event loop on secondary threads. [\#1664](https://github.com/Yelp/paasta/pull/1664) ([EvanKrall](https://github.com/EvanKrall))
- Perfscript [\#1663](https://github.com/Yelp/paasta/pull/1663) ([fboxwala](https://github.com/fboxwala))
- Make setup\_marathon\_job ignore instances starting with \_ [\#1655](https://github.com/Yelp/paasta/pull/1655) ([nhandler](https://github.com/nhandler))

## [v0.69.8](https://github.com/Yelp/paasta/tree/v0.69.8) (2017-12-14)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.69.7...v0.69.8)

**Merged pull requests:**

- PAASTA-13596: fix local\_run with healthcheck\_only [\#1659](https://github.com/Yelp/paasta/pull/1659) ([oktopuz](https://github.com/oktopuz))
- Reexec paasta local-run --pull with sudo when the effective uid is not 0 [\#1656](https://github.com/Yelp/paasta/pull/1656) ([oktopuz](https://github.com/oktopuz))
- Convert drain\_lib to asyncio so we can drain in parallel. [\#1653](https://github.com/Yelp/paasta/pull/1653) ([EvanKrall](https://github.com/EvanKrall))

## [v0.69.7](https://github.com/Yelp/paasta/tree/v0.69.7) (2017-12-12)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.69.6...v0.69.7)

**Merged pull requests:**

- bugfixes for pause\_service\_autoscaler [\#1658](https://github.com/Yelp/paasta/pull/1658) ([fboxwala](https://github.com/fboxwala))
- Passing boost arguments only when action is set [\#1657](https://github.com/Yelp/paasta/pull/1657) ([matfra](https://github.com/matfra))
- deletes pid decision code [\#1654](https://github.com/Yelp/paasta/pull/1654) ([fboxwala](https://github.com/fboxwala))
- Add remedy commands to the check\_mesos\_outdated\_tasks output [\#1651](https://github.com/Yelp/paasta/pull/1651) ([oktopuz](https://github.com/oktopuz))

## [v0.69.6](https://github.com/Yelp/paasta/tree/v0.69.6) (2017-12-07)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.69.5...v0.69.6)

**Merged pull requests:**

- Perfscript [\#1649](https://github.com/Yelp/paasta/pull/1649) ([fboxwala](https://github.com/fboxwala))
- Make metastatus able to group by multiple values properly [\#1642](https://github.com/Yelp/paasta/pull/1642) ([EvanKrall](https://github.com/EvanKrall))
- pytest.fixture for SystemPaastaConfig  [\#1641](https://github.com/Yelp/paasta/pull/1641) ([oktopuz](https://github.com/oktopuz))

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
