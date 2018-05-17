# Change Log

## [0.72.0](https://github.com/Yelp/paasta/tree/0.72.0) (2018-05-17)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.71.0...0.72.0)

**Merged pull requests:**

- Don't load deployments if a docker\_image is provided in remote-run [\#1837](https://github.com/Yelp/paasta/pull/1837) ([solarkennedy](https://github.com/solarkennedy))
- Don't get header so that config diff is accurate [\#1833](https://github.com/Yelp/paasta/pull/1833) ([qui](https://github.com/qui))
- Ping Slack with authors [\#1832](https://github.com/Yelp/paasta/pull/1832) ([solarkennedy](https://github.com/solarkennedy))
- Encode newlines before sending them to stdin [\#1831](https://github.com/Yelp/paasta/pull/1831) ([solarkennedy](https://github.com/solarkennedy))
- Allow specifying a custom image for remote-run [\#1830](https://github.com/Yelp/paasta/pull/1830) ([solarkennedy](https://github.com/solarkennedy))
- Added a get\_authors remote git command for better slack notifications [\#1829](https://github.com/Yelp/paasta/pull/1829) ([solarkennedy](https://github.com/solarkennedy))

## [v0.71.0](https://github.com/Yelp/paasta/tree/v0.71.0) (2018-05-11)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.70.11...v0.71.0)

**Merged pull requests:**

- default to using the system paasta config for the remote-run cluster [\#1826](https://github.com/Yelp/paasta/pull/1826) ([Rob-Johnson](https://github.com/Rob-Johnson))
- Deploy scripts for Tron configs [\#1812](https://github.com/Yelp/paasta/pull/1812) ([qui](https://github.com/qui))

## [v0.70.11](https://github.com/Yelp/paasta/tree/v0.70.11) (2018-05-10)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.70.10...v0.70.11)

**Merged pull requests:**

- don't set utilization\_error to -1 for cancelled\_running SFR unless we… [\#1828](https://github.com/Yelp/paasta/pull/1828) ([stug](https://github.com/stug))
- Catch NoSlavesAvailableError in deployd [\#1827](https://github.com/Yelp/paasta/pull/1827) ([mattmb](https://github.com/mattmb))
- First pass at native mark-for-deployment slack notifications [\#1821](https://github.com/Yelp/paasta/pull/1821) ([solarkennedy](https://github.com/solarkennedy))

## [v0.70.10](https://github.com/Yelp/paasta/tree/v0.70.10) (2018-05-08)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.70.9...v0.70.10)

**Merged pull requests:**

- Update task-processing to 0.0.8 [\#1825](https://github.com/Yelp/paasta/pull/1825) ([vkhromov](https://github.com/vkhromov))

## [v0.70.9](https://github.com/Yelp/paasta/tree/v0.70.9) (2018-05-08)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.70.8...v0.70.9)

**Merged pull requests:**

- Fix help text for `paasta remote-run` [\#1823](https://github.com/Yelp/paasta/pull/1823) ([ealter](https://github.com/ealter))
- Build packages for bionic [\#1822](https://github.com/Yelp/paasta/pull/1822) ([EvanKrall](https://github.com/EvanKrall))
- Add a flag to disable reservation on maint calls [\#1820](https://github.com/Yelp/paasta/pull/1820) ([mattmb](https://github.com/mattmb))
- Added spark\_args option to accommodate arbitrary Spark configuration [\#1819](https://github.com/Yelp/paasta/pull/1819) ([huadongliu](https://github.com/huadongliu))
- Use soa\_dir in paasta info. Fixes \#1813 [\#1817](https://github.com/Yelp/paasta/pull/1817) ([solarkennedy](https://github.com/solarkennedy))
- Added a slack class to make it easy to post notifications to slack. [\#1816](https://github.com/Yelp/paasta/pull/1816) ([solarkennedy](https://github.com/solarkennedy))

## [v0.70.8](https://github.com/Yelp/paasta/tree/v0.70.8) (2018-05-04)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.70.7...v0.70.8)

**Merged pull requests:**

- Update Jenkinsfile to point at internal mirror instead of directly at Github [\#1818](https://github.com/Yelp/paasta/pull/1818) ([EvanKrall](https://github.com/EvanKrall))
- don't abort downscaling an SFR/ASG if there are unregistered instance… [\#1805](https://github.com/Yelp/paasta/pull/1805) ([stug](https://github.com/stug))

## [v0.70.7](https://github.com/Yelp/paasta/tree/v0.70.7) (2018-05-03)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.70.6...v0.70.7)

**Closed issues:**

- paasta info does not show runbook for target soa\_dir [\#1813](https://github.com/Yelp/paasta/issues/1813)

## [v0.70.6](https://github.com/Yelp/paasta/tree/v0.70.6) (2018-05-02)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.70.5...v0.70.6)

**Merged pull requests:**

- Bump hacheck timeouts to avoid mass-timeouts when draining many tasks at once. [\#1815](https://github.com/Yelp/paasta/pull/1815) ([EvanKrall](https://github.com/EvanKrall))
- Do not use --it for spark-submit and jupyter [\#1814](https://github.com/Yelp/paasta/pull/1814) ([huadongliu](https://github.com/huadongliu))
- Initial Jenkinsfile implementation [\#1811](https://github.com/Yelp/paasta/pull/1811) ([EvanKrall](https://github.com/EvanKrall))

## [v0.70.5](https://github.com/Yelp/paasta/tree/v0.70.5) (2018-04-26)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.70.4...v0.70.5)

**Merged pull requests:**

- Print tracebacks when catching drain method exceptions [\#1810](https://github.com/Yelp/paasta/pull/1810) ([EvanKrall](https://github.com/EvanKrall))
- Make spark-run jupyter kernel culling configurable [\#1809](https://github.com/Yelp/paasta/pull/1809) ([huadongliu](https://github.com/huadongliu))
- Make paasta logs print out scribereader commands in verbose mode [\#1808](https://github.com/Yelp/paasta/pull/1808) ([solarkennedy](https://github.com/solarkennedy))

## [v0.70.4](https://github.com/Yelp/paasta/tree/v0.70.4) (2018-04-24)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.70.3...v0.70.4)

**Merged pull requests:**

- TASKPROC-194: increase the default staging timeout for remote-run [\#1806](https://github.com/Yelp/paasta/pull/1806) ([Rob-Johnson](https://github.com/Rob-Johnson))
- Make chronos bounce secret aware [\#1803](https://github.com/Yelp/paasta/pull/1803) ([mattmb](https://github.com/mattmb))
- Added Docker labels to spark-run executors [\#1802](https://github.com/Yelp/paasta/pull/1802) ([huadongliu](https://github.com/huadongliu))
- Upgrade tox-pip-extensions [\#1801](https://github.com/Yelp/paasta/pull/1801) ([mattmb](https://github.com/mattmb))

## [v0.70.3](https://github.com/Yelp/paasta/tree/v0.70.3) (2018-04-16)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.70.2...v0.70.3)

**Merged pull requests:**

- Move healthcheck\_port docs into the smartstack.yaml section [\#1800](https://github.com/Yelp/paasta/pull/1800) ([solarkennedy](https://github.com/solarkennedy))
- Add script to monitor for containers sharing an IP [\#1799](https://github.com/Yelp/paasta/pull/1799) ([qui](https://github.com/qui))

## [v0.70.2](https://github.com/Yelp/paasta/tree/v0.70.2) (2018-04-13)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.70.1...v0.70.2)

**Merged pull requests:**

- Added timeout for spark-run Jupyter kernels [\#1798](https://github.com/Yelp/paasta/pull/1798) ([huadongliu](https://github.com/huadongliu))
- add options for fetching aws credentials in spark-run [\#1797](https://github.com/Yelp/paasta/pull/1797) ([stug](https://github.com/stug))
- Make sure spark-run executor\_cores is less than max\_cores [\#1796](https://github.com/Yelp/paasta/pull/1796) ([huadongliu](https://github.com/huadongliu))
- Fix autoscaler watcher missing new service [\#1795](https://github.com/Yelp/paasta/pull/1795) ([mattmb](https://github.com/mattmb))
- Bump task-processing to 0.0.7 [\#1794](https://github.com/Yelp/paasta/pull/1794) ([vkhromov](https://github.com/vkhromov))
- make cluster autoscaler resilient to scaling down when not all instan… [\#1792](https://github.com/Yelp/paasta/pull/1792) ([stug](https://github.com/stug))
- Use rbt to expand the right-sizer review group. [\#1791](https://github.com/Yelp/paasta/pull/1791) ([thebostik](https://github.com/thebostik))
- Make bigger warnings on unsafe bounce methods [\#1790](https://github.com/Yelp/paasta/pull/1790) ([solarkennedy](https://github.com/solarkennedy))

## [v0.70.1](https://github.com/Yelp/paasta/tree/v0.70.1) (2018-04-10)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.70.0...v0.70.1)

**Merged pull requests:**

- Fix internal build [\#1793](https://github.com/Yelp/paasta/pull/1793) ([EvanKrall](https://github.com/EvanKrall))



\* *This Change Log was automatically generated by [github_changelog_generator](https://github.com/skywinder/Github-Changelog-Generator)*