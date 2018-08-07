# Change Log

## [0.78.0](https://github.com/Yelp/paasta/tree/0.78.0) (2018-08-07)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.78.0...0.78.0)

**Merged pull requests:**

- Removed EOF message from paasta status output. [\#1926](https://github.com/Yelp/paasta/pull/1926) ([solarkennedy](https://github.com/solarkennedy))
- Added docs and schema for the container\_port setting [\#1925](https://github.com/Yelp/paasta/pull/1925) ([solarkennedy](https://github.com/solarkennedy))
- Parallelize fetching haproxy state in get\_happy\_tasks, and limit the number of hosts to query. [\#1924](https://github.com/Yelp/paasta/pull/1924) ([EvanKrall](https://github.com/EvanKrall))
- Allow chronos rerun to skip services that have not been deployed [\#1915](https://github.com/Yelp/paasta/pull/1915) ([solarkennedy](https://github.com/solarkennedy))

## [v0.78.0](https://github.com/Yelp/paasta/tree/v0.78.0) (2018-08-03)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.77.2...v0.78.0)

**Merged pull requests:**

- Move list\_teams to monitoring\_tools [\#1922](https://github.com/Yelp/paasta/pull/1922) ([solarkennedy](https://github.com/solarkennedy))
- Added more descriptive errors to NoDeploymentsAvailable errors [\#1921](https://github.com/Yelp/paasta/pull/1921) ([solarkennedy](https://github.com/solarkennedy))
- Set k8s deployment spec version [\#1919](https://github.com/Yelp/paasta/pull/1919) ([mattmb](https://github.com/mattmb))
- For k8s deployments PUT not PATCH [\#1918](https://github.com/Yelp/paasta/pull/1918) ([mattmb](https://github.com/mattmb))
- fix the location of list\_kubernetes\_service\_instances [\#1916](https://github.com/Yelp/paasta/pull/1916) ([Rob-Johnson](https://github.com/Rob-Johnson))
- Added a remote\_run option to override dynamodb aws\_region config [\#1914](https://github.com/Yelp/paasta/pull/1914) ([huadongliu](https://github.com/huadongliu))
- Unified \_\_repr\_\_ across paasta instance objects and send cluster into all TronActionConfigs [\#1913](https://github.com/Yelp/paasta/pull/1913) ([solarkennedy](https://github.com/solarkennedy))
- RFC: smartstack errors meteorite emitter [\#1908](https://github.com/Yelp/paasta/pull/1908) ([solarkennedy](https://github.com/solarkennedy))
- validate valid team name [\#1904](https://github.com/Yelp/paasta/pull/1904) ([chlgit](https://github.com/chlgit))
- Gracefully drain all smartstack namespaces instead of just the first one [\#1895](https://github.com/Yelp/paasta/pull/1895) ([Rob-Johnson](https://github.com/Rob-Johnson))

## [v0.77.2](https://github.com/Yelp/paasta/tree/v0.77.2) (2018-07-23)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.77.1...v0.77.2)

## [v0.77.1](https://github.com/Yelp/paasta/tree/v0.77.1) (2018-07-23)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.77.0...v0.77.1)

**Merged pull requests:**

- Add support for tron retries\_delay attribute [\#1912](https://github.com/Yelp/paasta/pull/1912) ([keymone](https://github.com/keymone))
- Add f to string that should be f string [\#1909](https://github.com/Yelp/paasta/pull/1909) ([matthewbentley](https://github.com/matthewbentley))
- Add better documentation for local-run [\#1907](https://github.com/Yelp/paasta/pull/1907) ([MasterObvious](https://github.com/MasterObvious))
- Add cpu + mem constraints to k8s [\#1906](https://github.com/Yelp/paasta/pull/1906) ([mattmb](https://github.com/mattmb))
- Catch exceptions on format\_marathon\_app\_dict [\#1905](https://github.com/Yelp/paasta/pull/1905) ([mattmb](https://github.com/mattmb))
- Local run: Don't ask me to pick a deploy group when there's only one [\#1902](https://github.com/Yelp/paasta/pull/1902) ([mjksmith](https://github.com/mjksmith))
- Don't load deployments for Tron validation [\#1901](https://github.com/Yelp/paasta/pull/1901) ([qui](https://github.com/qui))
- Add a metric for leader elections [\#1900](https://github.com/Yelp/paasta/pull/1900) ([mattmb](https://github.com/mattmb))
- Tron docs [\#1897](https://github.com/Yelp/paasta/pull/1897) ([solarkennedy](https://github.com/solarkennedy))

## [v0.77.0](https://github.com/Yelp/paasta/tree/v0.77.0) (2018-07-16)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.76.0...v0.77.0)

**Merged pull requests:**

- Update getting\_started.rst [\#1899](https://github.com/Yelp/paasta/pull/1899) ([vasanth3045](https://github.com/vasanth3045))
- Add --user flag to `paasta docker\_exec` [\#1898](https://github.com/Yelp/paasta/pull/1898) ([chriskuehl](https://github.com/chriskuehl))
- Upgrade docutils to try to fix doc building [\#1896](https://github.com/Yelp/paasta/pull/1896) ([solarkennedy](https://github.com/solarkennedy))
- Starting point for validation of Tron configs [\#1893](https://github.com/Yelp/paasta/pull/1893) ([qui](https://github.com/qui))
- k8s AWS EBS static persistent volumes [\#1891](https://github.com/Yelp/paasta/pull/1891) ([vkhromov](https://github.com/vkhromov))

## [v0.76.0](https://github.com/Yelp/paasta/tree/v0.76.0) (2018-07-11)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.75.4...v0.76.0)

**Merged pull requests:**

- Add detect-secrets pre-commit hook [\#1894](https://github.com/Yelp/paasta/pull/1894) ([KevinHock](https://github.com/KevinHock))
- Support slack\_channels in sensu notifications [\#1879](https://github.com/Yelp/paasta/pull/1879) ([solarkennedy](https://github.com/solarkennedy))
- Configure default volumes and dockercfg location for Tron [\#1868](https://github.com/Yelp/paasta/pull/1868) ([qui](https://github.com/qui))

## [v0.75.4](https://github.com/Yelp/paasta/tree/v0.75.4) (2018-07-06)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.75.3...v0.75.4)

**Merged pull requests:**

- Minor fix to k8s nerve readiness check [\#1892](https://github.com/Yelp/paasta/pull/1892) ([mattmb](https://github.com/mattmb))
- K8s types instead of JSONs [\#1890](https://github.com/Yelp/paasta/pull/1890) ([vkhromov](https://github.com/vkhromov))
- Fix paasta restart for k8s [\#1889](https://github.com/Yelp/paasta/pull/1889) ([mattmb](https://github.com/mattmb))

## [v0.75.3](https://github.com/Yelp/paasta/tree/v0.75.3) (2018-07-04)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.75.2...v0.75.3)

**Merged pull requests:**

- Make travis fail if we break example cluster [\#1887](https://github.com/Yelp/paasta/pull/1887) ([mattmb](https://github.com/mattmb))
- Add kubernetes as an instance type [\#1886](https://github.com/Yelp/paasta/pull/1886) ([mattmb](https://github.com/mattmb))

## [v0.75.2](https://github.com/Yelp/paasta/tree/v0.75.2) (2018-07-03)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.75.1...v0.75.2)

**Merged pull requests:**

- Fix `docker\_wrapper.py` doesn't see environment variables [\#1885](https://github.com/Yelp/paasta/pull/1885) ([vkhromov](https://github.com/vkhromov))
- \[secret.py\] Nudge people to use the same prefixed env var name as secret name [\#1884](https://github.com/Yelp/paasta/pull/1884) ([KevinHock](https://github.com/KevinHock))
- Count all non marathon tasks as batch tasks [\#1882](https://github.com/Yelp/paasta/pull/1882) ([huadongliu](https://github.com/huadongliu))
- choose a host in the same pool that the service runs in to query for … [\#1881](https://github.com/Yelp/paasta/pull/1881) ([stug](https://github.com/stug))
- Deploy expected\_runtime field for tronfig [\#1880](https://github.com/Yelp/paasta/pull/1880) ([qui](https://github.com/qui))

## [v0.75.1](https://github.com/Yelp/paasta/tree/v0.75.1) (2018-07-02)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.75.0...v0.75.1)

## [v0.75.0](https://github.com/Yelp/paasta/tree/v0.75.0) (2018-07-02)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.74.9...v0.75.0)

**Merged pull requests:**

- Fix broken link to soa\_configs.html [\#1883](https://github.com/Yelp/paasta/pull/1883) ([jackchi](https://github.com/jackchi))
- Edit paasta add secret instructions [\#1878](https://github.com/Yelp/paasta/pull/1878) ([KevinHock](https://github.com/KevinHock))
- Removed classic service replication monitoring [\#1877](https://github.com/Yelp/paasta/pull/1877) ([solarkennedy](https://github.com/solarkennedy))
- Don't abort scaling if there are many orphaned instances, but do warn… [\#1876](https://github.com/Yelp/paasta/pull/1876) ([stug](https://github.com/stug))
- Don't pass environment variable values to Docker via command line args [\#1875](https://github.com/Yelp/paasta/pull/1875) ([vkhromov](https://github.com/vkhromov))
- Fix check registered slaves aws [\#1874](https://github.com/Yelp/paasta/pull/1874) ([davent](https://github.com/davent))
- Add support for kubernetes services [\#1872](https://github.com/Yelp/paasta/pull/1872) ([mattmb](https://github.com/mattmb))
- Print out the jenkins url when available to more slack notifications [\#1870](https://github.com/Yelp/paasta/pull/1870) ([solarkennedy](https://github.com/solarkennedy))

## [v0.74.9](https://github.com/Yelp/paasta/tree/v0.74.9) (2018-06-28)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.74.8...v0.74.9)

**Merged pull requests:**

- Relax the format of spark-run pyspark, spark-submit, and spark-shell … [\#1873](https://github.com/Yelp/paasta/pull/1873) ([huadongliu](https://github.com/huadongliu))
- Added spark-run command to start a spark history server [\#1871](https://github.com/Yelp/paasta/pull/1871) ([huadongliu](https://github.com/huadongliu))
- Print a friendlier error to local-run users when secret decryption fails [\#1869](https://github.com/Yelp/paasta/pull/1869) ([solarkennedy](https://github.com/solarkennedy))

## [v0.74.8](https://github.com/Yelp/paasta/tree/v0.74.8) (2018-06-20)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.74.7...v0.74.8)

**Merged pull requests:**

- Maybe fix pypi upload [\#1867](https://github.com/Yelp/paasta/pull/1867) ([mattmb](https://github.com/mattmb))
- ensure cmd is not None before interpolating [\#1866](https://github.com/Yelp/paasta/pull/1866) ([Rob-Johnson](https://github.com/Rob-Johnson))
- \[API\] Added a viewer for get\_services\_for\_cluster [\#1865](https://github.com/Yelp/paasta/pull/1865) ([transcedentalia](https://github.com/transcedentalia))

## [v0.74.7](https://github.com/Yelp/paasta/tree/v0.74.7) (2018-06-19)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.74.6...v0.74.7)

**Merged pull requests:**

- Even more async fixes. [\#1864](https://github.com/Yelp/paasta/pull/1864) ([EvanKrall](https://github.com/EvanKrall))

## [v0.74.6](https://github.com/Yelp/paasta/tree/v0.74.6) (2018-06-14)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.74.5...v0.74.6)

**Merged pull requests:**

- Fix more async problems [\#1863](https://github.com/Yelp/paasta/pull/1863) ([EvanKrall](https://github.com/EvanKrall))
- Fix example cluster build [\#1862](https://github.com/Yelp/paasta/pull/1862) ([mattmb](https://github.com/mattmb))
- Update constraint format for Tron [\#1861](https://github.com/Yelp/paasta/pull/1861) ([qui](https://github.com/qui))
- APOLLO-652: Make get\_containers\_and\_ips.py compatible with tron/batch [\#1859](https://github.com/Yelp/paasta/pull/1859) ([ronin13](https://github.com/ronin13))

## [v0.74.5](https://github.com/Yelp/paasta/tree/v0.74.5) (2018-06-12)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.74.4...v0.74.5)

**Merged pull requests:**

- \[tron\] Fix fetching tron configs when jobs is None [\#1860](https://github.com/Yelp/paasta/pull/1860) ([keymone](https://github.com/keymone))
- pyupgrade learns how to f-string [\#1855](https://github.com/Yelp/paasta/pull/1855) ([asottile](https://github.com/asottile))

## [v0.74.4](https://github.com/Yelp/paasta/tree/v0.74.4) (2018-06-11)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.74.3...v0.74.4)

**Merged pull requests:**

- Bump boto packages [\#1858](https://github.com/Yelp/paasta/pull/1858) ([keymone](https://github.com/keymone))

## [v0.74.3](https://github.com/Yelp/paasta/tree/v0.74.3) (2018-06-11)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.74.2...v0.74.3)

**Merged pull requests:**

- Bump boto requirements to fix trusty build [\#1857](https://github.com/Yelp/paasta/pull/1857) ([keymone](https://github.com/keymone))

## [v0.74.2](https://github.com/Yelp/paasta/tree/v0.74.2) (2018-06-08)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.74.1...v0.74.2)

**Closed issues:**

- Spark-run leaks AWS credentials into process title [\#1851](https://github.com/Yelp/paasta/issues/1851)

**Merged pull requests:**

- \[tron tools\] correctly skip deployment of unchanged tron namespaces [\#1856](https://github.com/Yelp/paasta/pull/1856) ([keymone](https://github.com/keymone))
- Added safer passing of credentials to spark [\#1853](https://github.com/Yelp/paasta/pull/1853) ([Qmando](https://github.com/Qmando))

## [v0.74.1](https://github.com/Yelp/paasta/tree/v0.74.1) (2018-06-06)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.74.0...v0.74.1)

**Merged pull requests:**

- Fix some places that I forgot to update for the async change [\#1852](https://github.com/Yelp/paasta/pull/1852) ([EvanKrall](https://github.com/EvanKrall))
- reduce cluster autoscaler aggressiveness in scaling down last instanc… [\#1850](https://github.com/Yelp/paasta/pull/1850) ([stug](https://github.com/stug))

## [v0.74.0](https://github.com/Yelp/paasta/tree/v0.74.0) (2018-06-05)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.73.3...v0.74.0)

## [v0.73.3](https://github.com/Yelp/paasta/tree/v0.73.3) (2018-06-04)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.73.1...v0.73.3)

**Merged pull requests:**

- Split out MAX\_CLUSTER\_DELTA for cluster autoscaler [\#1849](https://github.com/Yelp/paasta/pull/1849) ([mattmb](https://github.com/mattmb))
- check-ast is no longer useful now that paasta is py3+ [\#1848](https://github.com/Yelp/paasta/pull/1848) ([asottile](https://github.com/asottile))
- Add a custom key for "sfn\_autoscaling" to marathon\_schema. [\#1847](https://github.com/Yelp/paasta/pull/1847) ([sagar8192](https://github.com/sagar8192))
- \[tron tools\] update multiple namespaces in setup\_tron\_namespace [\#1846](https://github.com/Yelp/paasta/pull/1846) ([keymone](https://github.com/keymone))
- Upgrade pre-commit hooks [\#1845](https://github.com/Yelp/paasta/pull/1845) ([asottile](https://github.com/asottile))
- Remove a few unused requirements [\#1844](https://github.com/Yelp/paasta/pull/1844) ([asottile](https://github.com/asottile))
- \[tron tools\] error on namespace conflict [\#1843](https://github.com/Yelp/paasta/pull/1843) ([keymone](https://github.com/keymone))
- Cleanup cleanup action name [\#1842](https://github.com/Yelp/paasta/pull/1842) ([keymone](https://github.com/keymone))
- Parallelize mesos task fetching in paasta status -v [\#1841](https://github.com/Yelp/paasta/pull/1841) ([EvanKrall](https://github.com/EvanKrall))

## [v0.73.1](https://github.com/Yelp/paasta/tree/v0.73.1) (2018-05-22)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.73.0...v0.73.1)

**Merged pull requests:**

- Don't send slack noifications if the old and new commits are the same [\#1840](https://github.com/Yelp/paasta/pull/1840) ([solarkennedy](https://github.com/solarkennedy))
- Make SpotAutoscaler cleanup cancelled\_running SFR with no instances [\#1839](https://github.com/Yelp/paasta/pull/1839) ([stug](https://github.com/stug))

## [v0.73.0](https://github.com/Yelp/paasta/tree/v0.73.0) (2018-05-21)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.72.0...v0.73.0)

**Merged pull requests:**

- read tron config files from service directories [\#1836](https://github.com/Yelp/paasta/pull/1836) ([chlgit](https://github.com/chlgit))



\* *This Change Log was automatically generated by [github_changelog_generator](https://github.com/skywinder/Github-Changelog-Generator)*