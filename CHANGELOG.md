# Change Log

## [v0.47.2](https://github.com/Yelp/paasta/tree/v0.47.2) (2016-09-12)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.46.4...v0.47.2)

**Merged pull requests:**

- Removed references to unused healchechk\_port [\#745](https://github.com/Yelp/paasta/pull/745) ([solarkennedy](https://github.com/solarkennedy))
- \[WIP\] Move mesos\_maintenance stuff into its own file [\#742](https://github.com/Yelp/paasta/pull/742) ([solarkennedy](https://github.com/solarkennedy))
- Make get\_job\_for\_service\_instance only return one chronos job ever [\#733](https://github.com/Yelp/paasta/pull/733) ([solarkennedy](https://github.com/solarkennedy))
- Make marathon\_serviceinit filter mesos tasks properly [\#731](https://github.com/Yelp/paasta/pull/731) ([solarkennedy](https://github.com/solarkennedy))

## [v0.46.4](https://github.com/Yelp/paasta/tree/v0.46.4) (2016-09-09)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.46.3...v0.46.4)

**Merged pull requests:**

- Add api to docker playground [\#739](https://github.com/Yelp/paasta/pull/739) ([mattmb](https://github.com/mattmb))
- Autoscaler box decision [\#728](https://github.com/Yelp/paasta/pull/728) ([mattmb](https://github.com/mattmb))

## [v0.46.3](https://github.com/Yelp/paasta/tree/v0.46.3) (2016-09-09)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.46.2...v0.46.3)

**Merged pull requests:**

- Fix potential paasta\_metastatus DivizionByZero [\#741](https://github.com/Yelp/paasta/pull/741) ([nhandler](https://github.com/nhandler))

## [v0.46.2](https://github.com/Yelp/paasta/tree/v0.46.2) (2016-09-08)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.46.1...v0.46.2)

**Merged pull requests:**

- Disable caching on wait\_for\_app\_to\_launch\_tasks [\#738](https://github.com/Yelp/paasta/pull/738) ([solarkennedy](https://github.com/solarkennedy))
- added a flag to list\_marathon\_service\_instances to only show bouncing… [\#736](https://github.com/Yelp/paasta/pull/736) ([mjksmith](https://github.com/mjksmith))

## [v0.46.1](https://github.com/Yelp/paasta/tree/v0.46.1) (2016-09-08)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.46.0...v0.46.1)

**Closed issues:**

- Documentation showing wrong graph for downthenup [\#735](https://github.com/Yelp/paasta/issues/735)

**Merged pull requests:**

- Add backoff\_factor and max\_launch\_delay seconds to marathon schema [\#737](https://github.com/Yelp/paasta/pull/737) ([nhandler](https://github.com/nhandler))
- Adding dependencies [\#734](https://github.com/Yelp/paasta/pull/734) ([mattmb](https://github.com/mattmb))
- Disable service\_configuration\_lib caching of service configs in paasta-api [\#732](https://github.com/Yelp/paasta/pull/732) ([huadongliu](https://github.com/huadongliu))
- Add percentages for attribute usage in metastatus [\#730](https://github.com/Yelp/paasta/pull/730) ([mattmb](https://github.com/mattmb))
- Add pytz to setup.py [\#727](https://github.com/Yelp/paasta/pull/727) ([mattmb](https://github.com/mattmb))
- Remove bounce\_progress sensu ttl alert [\#725](https://github.com/Yelp/paasta/pull/725) ([solarkennedy](https://github.com/solarkennedy))
- Replace paasta-api client with swagger client [\#724](https://github.com/Yelp/paasta/pull/724) ([huadongliu](https://github.com/huadongliu))

## [v0.46.0](https://github.com/Yelp/paasta/tree/v0.46.0) (2016-09-06)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.45.0...v0.46.0)

**Merged pull requests:**

- fix brokeness from get\_mesos\_slaves\_grouped\_by\_attribute [\#729](https://github.com/Yelp/paasta/pull/729) ([Rob-Johnson](https://github.com/Rob-Johnson))

## [v0.45.0](https://github.com/Yelp/paasta/tree/v0.45.0) (2016-09-06)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.44.0...v0.45.0)

**Closed issues:**

- paasta check assumes that your code lives in git.yelpcorp.com:services/ [\#722](https://github.com/Yelp/paasta/issues/722)
- Upgrade mesos to 1.0.1 [\#637](https://github.com/Yelp/paasta/issues/637)

**Merged pull requests:**

- Link to open source docs instead of internal trac. [\#726](https://github.com/Yelp/paasta/pull/726) ([ealter](https://github.com/ealter))
- move tron related tools to a sub package [\#723](https://github.com/Yelp/paasta/pull/723) ([Rob-Johnson](https://github.com/Rob-Johnson))
- Revert "Replace paasta-api client with swagger client" [\#721](https://github.com/Yelp/paasta/pull/721) ([huadongliu](https://github.com/huadongliu))
- Make paasta logs verbose by default [\#719](https://github.com/Yelp/paasta/pull/719) ([solarkennedy](https://github.com/solarkennedy))
- Added skeleton for paasta\_maint is\_safe\_to\_drain [\#717](https://github.com/Yelp/paasta/pull/717) ([solarkennedy](https://github.com/solarkennedy))
- upgraded mesos to 1.0.1 [\#716](https://github.com/Yelp/paasta/pull/716) ([mjksmith](https://github.com/mjksmith))
- Make autoscaler log record pool name [\#715](https://github.com/Yelp/paasta/pull/715) ([mattmb](https://github.com/mattmb))
- Manpage Version of Generated Docs [\#714](https://github.com/Yelp/paasta/pull/714) ([nhandler](https://github.com/nhandler))
- Removed external tron dependency [\#713](https://github.com/Yelp/paasta/pull/713) ([huadongliu](https://github.com/huadongliu))
- Added new and improved bounce docs [\#712](https://github.com/Yelp/paasta/pull/712) ([solarkennedy](https://github.com/solarkennedy))
- Remove yelpy links to jenkins pipeline [\#704](https://github.com/Yelp/paasta/pull/704) ([kkellyy](https://github.com/kkellyy))
- Remove print statements [\#693](https://github.com/Yelp/paasta/pull/693) ([mattmb](https://github.com/mattmb))
- Added a paasta status option to use the api endpoint [\#691](https://github.com/Yelp/paasta/pull/691) ([huadongliu](https://github.com/huadongliu))
- paasta itest should stream output to stdout for easier debugging. [\#636](https://github.com/Yelp/paasta/pull/636) ([EvanKrall](https://github.com/EvanKrall))

## [v0.44.0](https://github.com/Yelp/paasta/tree/v0.44.0) (2016-08-26)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.43.0...v0.44.0)

**Closed issues:**

- paasta metastatus is no longer accurate in -vv and -vvv [\#708](https://github.com/Yelp/paasta/issues/708)

**Merged pull requests:**

- use the full state endpoint in metastatus again [\#709](https://github.com/Yelp/paasta/pull/709) ([Rob-Johnson](https://github.com/Rob-Johnson))
- Removed zookeper scaling itests [\#707](https://github.com/Yelp/paasta/pull/707) ([solarkennedy](https://github.com/solarkennedy))
- use the /slaves endpoint rather than /state where possible [\#706](https://github.com/Yelp/paasta/pull/706) ([Rob-Johnson](https://github.com/Rob-Johnson))

## [v0.43.0](https://github.com/Yelp/paasta/tree/v0.43.0) (2016-08-25)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.42.0...v0.43.0)

**Merged pull requests:**

- add ulimit and cap\_add settings to json schema [\#705](https://github.com/Yelp/paasta/pull/705) ([gstarnberger](https://github.com/gstarnberger))
- Add back paasta-api itests [\#675](https://github.com/Yelp/paasta/pull/675) ([huadongliu](https://github.com/huadongliu))

## [v0.42.0](https://github.com/Yelp/paasta/tree/v0.42.0) (2016-08-24)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.41.0...v0.42.0)

**Closed issues:**

- local-run ignores -y for guessing cluster [\#702](https://github.com/Yelp/paasta/issues/702)

**Merged pull requests:**

- pass soa\_dir into guess\_cluster/get\_default\_cluster\_for\_service. [\#703](https://github.com/Yelp/paasta/pull/703) ([EvanKrall](https://github.com/EvanKrall))
- Add PAASTA\_DOCKER\_IMAGE environment variable. [\#697](https://github.com/Yelp/paasta/pull/697) ([EvanKrall](https://github.com/EvanKrall))
- Add version to metastatus [\#689](https://github.com/Yelp/paasta/pull/689) ([mattmb](https://github.com/mattmb))

## [v0.41.0](https://github.com/Yelp/paasta/tree/v0.41.0) (2016-08-23)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.40.0...v0.41.0)

**Closed issues:**

- paasta local-run --dry-run no longer works [\#698](https://github.com/Yelp/paasta/issues/698)

**Merged pull requests:**

- Fix local-run --dry-run [\#700](https://github.com/Yelp/paasta/pull/700) ([chriskuehl](https://github.com/chriskuehl))
- delete duplicate code in metastatus [\#692](https://github.com/Yelp/paasta/pull/692) ([Rob-Johnson](https://github.com/Rob-Johnson))
- allow to pass --cap-add arguments to docker [\#690](https://github.com/Yelp/paasta/pull/690) ([gstarnberger](https://github.com/gstarnberger))
- Make log message clearer when counting tasks [\#688](https://github.com/Yelp/paasta/pull/688) ([mattmb](https://github.com/mattmb))
- Move print statements in paasta\_maintenance [\#687](https://github.com/Yelp/paasta/pull/687) ([mattmb](https://github.com/mattmb))
- Added a verbose mode to autoscale\_all\_services [\#685](https://github.com/Yelp/paasta/pull/685) ([solarkennedy](https://github.com/solarkennedy))
- allow to pass --ulimit arguments to docker [\#673](https://github.com/Yelp/paasta/pull/673) ([gstarnberger](https://github.com/gstarnberger))

## [v0.40.0](https://github.com/Yelp/paasta/tree/v0.40.0) (2016-08-22)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.39.0...v0.40.0)

**Merged pull requests:**

- Revert "Upgrade mesos from 0.28.2 to 1.0.0 in itests" [\#684](https://github.com/Yelp/paasta/pull/684) ([Rob-Johnson](https://github.com/Rob-Johnson))
- Move paasta autoscaler config [\#683](https://github.com/Yelp/paasta/pull/683) ([mattmb](https://github.com/mattmb))

## [v0.39.0](https://github.com/Yelp/paasta/tree/v0.39.0) (2016-08-19)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.37.0...v0.39.0)

**Closed issues:**

- `paasta check` is broken when using command-\* steps [\#680](https://github.com/Yelp/paasta/issues/680)
- Log bounce exceptions as a single line [\#669](https://github.com/Yelp/paasta/issues/669)

**Merged pull requests:**

- remove mesos master state.json monkey patch [\#682](https://github.com/Yelp/paasta/pull/682) ([huadongliu](https://github.com/huadongliu))
- Add support for command- steps in check [\#681](https://github.com/Yelp/paasta/pull/681) ([tub](https://github.com/tub))
- check validity of clusters in start/stop/restart [\#679](https://github.com/Yelp/paasta/pull/679) ([Rob-Johnson](https://github.com/Rob-Johnson))
- Renaming instancetype to step [\#676](https://github.com/Yelp/paasta/pull/676) ([tub](https://github.com/tub))
- Remove paasta\_api itests to see if things get stabilized [\#674](https://github.com/Yelp/paasta/pull/674) ([huadongliu](https://github.com/huadongliu))
- Make paasta\_maintenance is\_host\_drained work for hosts not in mesos [\#672](https://github.com/Yelp/paasta/pull/672) ([nhandler](https://github.com/nhandler))
- Log exceptions as single events, to be formatted on the tailing side. Fixes \#669 [\#670](https://github.com/Yelp/paasta/pull/670) ([EvanKrall](https://github.com/EvanKrall))
- Autoscaler skip non deployed [\#668](https://github.com/Yelp/paasta/pull/668) ([solarkennedy](https://github.com/solarkennedy))

## [v0.37.0](https://github.com/Yelp/paasta/tree/v0.37.0) (2016-08-16)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.36.0...v0.37.0)

**Merged pull requests:**

- Move requests\_cache uninstall out of itest scenario cleanup [\#667](https://github.com/Yelp/paasta/pull/667) ([huadongliu](https://github.com/huadongliu))
- increase wait time to alleviate itest run\_until\_number\_tasks failures [\#666](https://github.com/Yelp/paasta/pull/666) ([huadongliu](https://github.com/huadongliu))
- Backwards compatible way to support multiple nerve namespaces [\#642](https://github.com/Yelp/paasta/pull/642) ([jolynch](https://github.com/jolynch))

## [v0.36.0](https://github.com/Yelp/paasta/tree/v0.36.0) (2016-08-12)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.35.0...v0.36.0)

**Merged pull requests:**

- Added pyramid\_swagger to paasta-api [\#657](https://github.com/Yelp/paasta/pull/657) ([huadongliu](https://github.com/huadongliu))

## [v0.35.0](https://github.com/Yelp/paasta/tree/v0.35.0) (2016-08-11)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.34.5...v0.35.0)

**Closed issues:**

- Cluster autoscaler drain/undrain [\#619](https://github.com/Yelp/paasta/issues/619)

**Merged pull requests:**

- Stop making local-run guess --pull behavior. Still default to --build… [\#663](https://github.com/Yelp/paasta/pull/663) ([ammaraskar](https://github.com/ammaraskar))
- Skip services with NoDeploymentsAvailable in check\_chronos\_jobs [\#662](https://github.com/Yelp/paasta/pull/662) ([ammaraskar](https://github.com/ammaraskar))
- Set user agent to PaaSTA Tools \<version\> when sending HTTP requests [\#661](https://github.com/Yelp/paasta/pull/661) ([ammaraskar](https://github.com/ammaraskar))
- Autoscaler maintenance cleanup windows [\#659](https://github.com/Yelp/paasta/pull/659) ([mattmb](https://github.com/mattmb))

## [v0.34.5](https://github.com/Yelp/paasta/tree/v0.34.5) (2016-08-10)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.34.4...v0.34.5)

**Merged pull requests:**

- Move version number to paasta\_tool's \_\_init\_\_ [\#658](https://github.com/Yelp/paasta/pull/658) ([ammaraskar](https://github.com/ammaraskar))
- Bump tron to pick up 'hour' support [\#656](https://github.com/Yelp/paasta/pull/656) ([solarkennedy](https://github.com/solarkennedy))
- Fix travis docker-engine [\#654](https://github.com/Yelp/paasta/pull/654) ([mattmb](https://github.com/mattmb))
- Fix itest rsyslog race [\#653](https://github.com/Yelp/paasta/pull/653) ([mattmb](https://github.com/mattmb))
- make check\_docker\_image respect DOCKER\_HOST, etc. [\#650](https://github.com/Yelp/paasta/pull/650) ([EvanKrall](https://github.com/EvanKrall))
- Introduce a None check for task's started\_at field in bounce\_lib [\#649](https://github.com/Yelp/paasta/pull/649) ([ammaraskar](https://github.com/ammaraskar))
- Fix itests orphaned containers bug. [\#648](https://github.com/Yelp/paasta/pull/648) ([mattmb](https://github.com/mattmb))
- Add support for maintenance mode to autoscaler [\#646](https://github.com/Yelp/paasta/pull/646) ([mattmb](https://github.com/mattmb))
- Change local-run to healthcheck via GET [\#644](https://github.com/Yelp/paasta/pull/644) ([ammaraskar](https://github.com/ammaraskar))
- Make paasta status print lines proprotional to how verbose they requested [\#641](https://github.com/Yelp/paasta/pull/641) ([solarkennedy](https://github.com/solarkennedy))
- Add docker-compose example cluster [\#635](https://github.com/Yelp/paasta/pull/635) ([mattmb](https://github.com/mattmb))

## [v0.34.4](https://github.com/Yelp/paasta/tree/v0.34.4) (2016-08-05)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.34.3...v0.34.4)

## [v0.34.3](https://github.com/Yelp/paasta/tree/v0.34.3) (2016-08-05)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.34.2...v0.34.3)

**Merged pull requests:**

- Add validation of HTTP drain method and fix bounce errors [\#645](https://github.com/Yelp/paasta/pull/645) ([keshavdv](https://github.com/keshavdv))

## [v0.34.2](https://github.com/Yelp/paasta/tree/v0.34.2) (2016-08-05)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.34.1...v0.34.2)

**Merged pull requests:**

- Mesos 1.0.0 [\#643](https://github.com/Yelp/paasta/pull/643) ([nhandler](https://github.com/nhandler))

## [v0.34.1](https://github.com/Yelp/paasta/tree/v0.34.1) (2016-08-03)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.34.0...v0.34.1)

**Merged pull requests:**

- Revert float change [\#640](https://github.com/Yelp/paasta/pull/640) ([mattmb](https://github.com/mattmb))

## [v0.34.0](https://github.com/Yelp/paasta/tree/v0.34.0) (2016-08-01)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.33.0...v0.34.0)

**Merged pull requests:**

- Ensure we treat instance weights as floats [\#634](https://github.com/Yelp/paasta/pull/634) ([mattmb](https://github.com/mattmb))
- Make setup\_marathon\_job aware of hosts marked as draining [\#521](https://github.com/Yelp/paasta/pull/521) ([nhandler](https://github.com/nhandler))

## [v0.33.0](https://github.com/Yelp/paasta/tree/v0.33.0) (2016-07-28)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.32.2...v0.33.0)

**Merged pull requests:**

- Make check\_chronos\_job only check the most recently run job [\#631](https://github.com/Yelp/paasta/pull/631) ([ammaraskar](https://github.com/ammaraskar))

## [v0.32.2](https://github.com/Yelp/paasta/tree/v0.32.2) (2016-07-27)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.32.1...v0.32.2)

**Merged pull requests:**

- Allow https healthcheck in ServiceNamespaceConfig, since hacheck now … [\#632](https://github.com/Yelp/paasta/pull/632) ([matthewbentley](https://github.com/matthewbentley))
- PAASTA-5190: Updating chronos to yelp release in itests [\#629](https://github.com/Yelp/paasta/pull/629) ([tub](https://github.com/tub))

## [v0.32.1](https://github.com/Yelp/paasta/tree/v0.32.1) (2016-07-26)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.32.0...v0.32.1)

**Merged pull requests:**

- Check spotfleet status before we gather metrics [\#628](https://github.com/Yelp/paasta/pull/628) ([mattmb](https://github.com/mattmb))

## [v0.32.0](https://github.com/Yelp/paasta/tree/v0.32.0) (2016-07-19)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.31.0...v0.32.0)

**Merged pull requests:**

- Make paasta stop/start/restart not print the same type of message rep… [\#625](https://github.com/Yelp/paasta/pull/625) ([ammaraskar](https://github.com/ammaraskar))
- Handle connection errors to chronos in check\_chronos\_jobs gracefully [\#624](https://github.com/Yelp/paasta/pull/624) ([ammaraskar](https://github.com/ammaraskar))
- paasta api changes to enable deployment changes in puppet [\#589](https://github.com/Yelp/paasta/pull/589) ([huadongliu](https://github.com/huadongliu))

## [v0.31.0](https://github.com/Yelp/paasta/tree/v0.31.0) (2016-07-18)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.30.4...v0.31.0)

**Closed issues:**

- More than 10 lines of Chronos job stdout/stderr in "paasta status" [\#463](https://github.com/Yelp/paasta/issues/463)

**Merged pull requests:**

- Use packaged ephemeral-port-reserve [\#626](https://github.com/Yelp/paasta/pull/626) ([asottile](https://github.com/asottile))
- Handle socket connection errors in metastatus gracefully [\#623](https://github.com/Yelp/paasta/pull/623) ([ammaraskar](https://github.com/ammaraskar))
- Stop hardcoding realert\_every, respect the setting for chronos jobs [\#622](https://github.com/Yelp/paasta/pull/622) ([ammaraskar](https://github.com/ammaraskar))
- Fix emergency-stop bug that doesn't allow it to infer service name fr… [\#621](https://github.com/Yelp/paasta/pull/621) ([ammaraskar](https://github.com/ammaraskar))

## [v0.30.4](https://github.com/Yelp/paasta/tree/v0.30.4) (2016-07-15)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.30.3...v0.30.4)

**Merged pull requests:**

- Reduce EC2 API calls from autoscaler [\#620](https://github.com/Yelp/paasta/pull/620) ([mattmb](https://github.com/mattmb))
- Autoscaler weights [\#617](https://github.com/Yelp/paasta/pull/617) ([mattmb](https://github.com/mattmb))

## [v0.30.3](https://github.com/Yelp/paasta/tree/v0.30.3) (2016-07-13)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.30.2...v0.30.3)

**Closed issues:**

- Config file merging [\#605](https://github.com/Yelp/paasta/issues/605)

**Merged pull requests:**

- Allow http drain method in configs [\#618](https://github.com/Yelp/paasta/pull/618) ([keshavdv](https://github.com/keshavdv))
- Fix sensu link markdown formatting [\#616](https://github.com/Yelp/paasta/pull/616) ([ealter](https://github.com/ealter))

## [v0.30.2](https://github.com/Yelp/paasta/tree/v0.30.2) (2016-07-08)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.30.1...v0.30.2)

**Merged pull requests:**

- Fix delayed and suspended status when no deployments are in progress [\#615](https://github.com/Yelp/paasta/pull/615) ([keshavdv](https://github.com/keshavdv))

## [v0.30.1](https://github.com/Yelp/paasta/tree/v0.30.1) (2016-07-07)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.30.0...v0.30.1)

**Merged pull requests:**

- Fix bug handling task counts in autoscaler [\#614](https://github.com/Yelp/paasta/pull/614) ([mattmb](https://github.com/mattmb))
- Change logging for autoscaler [\#613](https://github.com/Yelp/paasta/pull/613) ([mattmb](https://github.com/mattmb))
- Fix paasta\_metastatus: SystemPaastaConfig doesn't raise MarathonNotConfigured or ChronosNotConfigured if marathon\_config/chronos\_config don't exist [\#612](https://github.com/Yelp/paasta/pull/612) ([EvanKrall](https://github.com/EvanKrall))

## [v0.30.0](https://github.com/Yelp/paasta/tree/v0.30.0) (2016-07-07)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.29.1...v0.30.0)

**Merged pull requests:**

- Deep merge config files [\#611](https://github.com/Yelp/paasta/pull/611) ([mattmb](https://github.com/mattmb))
- Changes to cluster autoscaling [\#598](https://github.com/Yelp/paasta/pull/598) ([mattmb](https://github.com/mattmb))

## [v0.29.1](https://github.com/Yelp/paasta/tree/v0.29.1) (2016-07-06)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.29.0...v0.29.1)

**Closed issues:**

- Create HTTP drain method [\#514](https://github.com/Yelp/paasta/issues/514)

**Merged pull requests:**

- \[PAASTA-4955\] Add docs for new options [\#610](https://github.com/Yelp/paasta/pull/610) ([koikonom](https://github.com/koikonom))
- Implement HTTP drain method. [\#571](https://github.com/Yelp/paasta/pull/571) ([EvanKrall](https://github.com/EvanKrall))

## [v0.29.0](https://github.com/Yelp/paasta/tree/v0.29.0) (2016-07-05)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.28.0...v0.29.0)

**Closed issues:**

- paasta logs prints error messages to stdout instead of stderr [\#604](https://github.com/Yelp/paasta/issues/604)
- Paasta status for stopped instance says "Running" [\#587](https://github.com/Yelp/paasta/issues/587)

**Merged pull requests:**

- Log `paasta logs` errors to stderr, fixes \#604.  [\#608](https://github.com/Yelp/paasta/pull/608) ([ammaraskar](https://github.com/ammaraskar))
- \[PAASTA-4955\] Configurable app launch timeouts [\#606](https://github.com/Yelp/paasta/pull/606) ([koikonom](https://github.com/koikonom))

## [v0.28.0](https://github.com/Yelp/paasta/tree/v0.28.0) (2016-06-29)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.27.0...v0.28.0)

**Closed issues:**

- Use "real" configs for itests [\#576](https://github.com/Yelp/paasta/issues/576)

**Merged pull requests:**

- Various logging improvements: [\#603](https://github.com/Yelp/paasta/pull/603) ([ammaraskar](https://github.com/ammaraskar))
- Fix autoscaler metrics provider [\#602](https://github.com/Yelp/paasta/pull/602) ([mattmb](https://github.com/mattmb))
- Make itests use "real" configs by having them call setup\_service instead of deploy\_service [\#594](https://github.com/Yelp/paasta/pull/594) ([EvanKrall](https://github.com/EvanKrall))

## [v0.27.0](https://github.com/Yelp/paasta/tree/v0.27.0) (2016-06-24)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.26.0...v0.27.0)

## [v0.26.0](https://github.com/Yelp/paasta/tree/v0.26.0) (2016-06-24)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.25.0...v0.26.0)

**Closed issues:**

- paasta stop without `-i` tries to stop unrelated instance on different service [\#588](https://github.com/Yelp/paasta/issues/588)

**Merged pull requests:**

- Add delayed and waiting state to paasta status for marathon jobs [\#590](https://github.com/Yelp/paasta/pull/590) ([keshavdv](https://github.com/keshavdv))

## [v0.25.0](https://github.com/Yelp/paasta/tree/v0.25.0) (2016-06-23)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.24.0...v0.25.0)

**Merged pull requests:**

- PAASTA-4980 fix regression in -v option for metastatus [\#595](https://github.com/Yelp/paasta/pull/595) ([Rob-Johnson](https://github.com/Rob-Johnson))

## [v0.24.0](https://github.com/Yelp/paasta/tree/v0.24.0) (2016-06-23)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.23.0...v0.24.0)

**Closed issues:**

- Service autoscaling doesn't wait for healthy tasks [\#472](https://github.com/Yelp/paasta/issues/472)

**Merged pull requests:**

- Tolerate autoscaling for tasks with no healthcheck [\#592](https://github.com/Yelp/paasta/pull/592) ([mattmb](https://github.com/mattmb))
- add the -g and -H to the cli entrypoint for metastatus [\#591](https://github.com/Yelp/paasta/pull/591) ([Rob-Johnson](https://github.com/Rob-Johnson))
- ceil\(\) when using marging\_factor [\#586](https://github.com/Yelp/paasta/pull/586) ([oktopuz](https://github.com/oktopuz))
- Fix logging for cli comamnds by having a basicConfig by default.  [\#584](https://github.com/Yelp/paasta/pull/584) ([ammaraskar](https://github.com/ammaraskar))
- Make autoscaling wait for tasks to be healthy [\#580](https://github.com/Yelp/paasta/pull/580) ([mattmb](https://github.com/mattmb))

## [v0.23.0](https://github.com/Yelp/paasta/tree/v0.23.0) (2016-06-16)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.22.0...v0.23.0)

**Merged pull requests:**

- Fix typo in function name [\#582](https://github.com/Yelp/paasta/pull/582) ([EvanKrall](https://github.com/EvanKrall))

## [v0.22.0](https://github.com/Yelp/paasta/tree/v0.22.0) (2016-06-16)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.21.0...v0.22.0)

**Closed issues:**

- Tox isn't listed under requirements.txt [\#581](https://github.com/Yelp/paasta/issues/581)
- local-run has some issues with cmd quoting [\#555](https://github.com/Yelp/paasta/issues/555)
- paasta metastatus crashes if `dashboard\_links` isn't in /etc/paasta [\#530](https://github.com/Yelp/paasta/issues/530)
- Use docker containerizer in itests [\#373](https://github.com/Yelp/paasta/issues/373)

**Merged pull requests:**

- Use docker containerizer to run itests. [\#577](https://github.com/Yelp/paasta/pull/577) ([matthewbentley](https://github.com/matthewbentley))
- Correct healthcheck\_cmd docs [\#574](https://github.com/Yelp/paasta/pull/574) ([mattmb](https://github.com/mattmb))
- Handle missing dashboard\_links gracefully [\#570](https://github.com/Yelp/paasta/pull/570) ([koikonom](https://github.com/koikonom))
- Make paasta\_serviceinit exit more suitably when a command fails. [\#569](https://github.com/Yelp/paasta/pull/569) ([ammaraskar](https://github.com/ammaraskar))
- Fix command quoting issues [\#561](https://github.com/Yelp/paasta/pull/561) ([koikonom](https://github.com/koikonom))
- Add chronos status to paasta status output [\#560](https://github.com/Yelp/paasta/pull/560) ([keshavdv](https://github.com/keshavdv))

## [v0.21.0](https://github.com/Yelp/paasta/tree/v0.21.0) (2016-06-14)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.20.0...v0.21.0)

**Closed issues:**

- Make tests less verbose [\#503](https://github.com/Yelp/paasta/issues/503)

**Merged pull requests:**

- Paasta start/stop/restart multiple instances [\#578](https://github.com/Yelp/paasta/pull/578) ([mattmb](https://github.com/mattmb))
- Reduce logging verbosity during tests [\#575](https://github.com/Yelp/paasta/pull/575) ([keshavdv](https://github.com/keshavdv))
- Add kill threshold [\#573](https://github.com/Yelp/paasta/pull/573) ([mattmb](https://github.com/mattmb))
- Fix a bunch of doc warnings \(but not all, there are still some consis… [\#556](https://github.com/Yelp/paasta/pull/556) ([matthewbentley](https://github.com/matthewbentley))
- Allow crossover to make progress in the face of failures [\#534](https://github.com/Yelp/paasta/pull/534) ([oktopuz](https://github.com/oktopuz))
- \(Rebased\) Update emergency-start semantics for chronos jobs [\#370](https://github.com/Yelp/paasta/pull/370) ([nhandler](https://github.com/nhandler))

## [v0.20.0](https://github.com/Yelp/paasta/tree/v0.20.0) (2016-06-09)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.19.16...v0.20.0)

**Closed issues:**

- Get rid of "special" files in /etc/paasta [\#515](https://github.com/Yelp/paasta/issues/515)

**Merged pull requests:**

- upgrade service-configuration-lib to 0.10.1 [\#568](https://github.com/Yelp/paasta/pull/568) ([huadongliu](https://github.com/huadongliu))
- Get rid of "special" config files [\#551](https://github.com/Yelp/paasta/pull/551) ([matthewbentley](https://github.com/matthewbentley))

## [v0.19.16](https://github.com/Yelp/paasta/tree/v0.19.16) (2016-06-09)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.19.15...v0.19.16)

**Merged pull requests:**

- set utilization to 0 on zero division error [\#572](https://github.com/Yelp/paasta/pull/572) ([Rob-Johnson](https://github.com/Rob-Johnson))

## [v0.19.15](https://github.com/Yelp/paasta/tree/v0.19.15) (2016-06-09)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.19.14...v0.19.15)

**Closed issues:**

- paasta status exits zero on ^C [\#525](https://github.com/Yelp/paasta/issues/525)
- paasta status now shows 'connection closed' output [\#524](https://github.com/Yelp/paasta/issues/524)

## [v0.19.14](https://github.com/Yelp/paasta/tree/v0.19.14) (2016-06-08)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.19.13...v0.19.14)

## [v0.19.13](https://github.com/Yelp/paasta/tree/v0.19.13) (2016-06-08)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.19.12...v0.19.13)

**Merged pull requests:**

- Generalize cpu quota [\#567](https://github.com/Yelp/paasta/pull/567) ([matthewbentley](https://github.com/matthewbentley))
- disable service\_configuration\_lib yaml cache when setting up marathon jobs [\#562](https://github.com/Yelp/paasta/pull/562) ([huadongliu](https://github.com/huadongliu))

## [v0.19.12](https://github.com/Yelp/paasta/tree/v0.19.12) (2016-06-07)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.19.11...v0.19.12)

## [v0.19.11](https://github.com/Yelp/paasta/tree/v0.19.11) (2016-06-07)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.19.10...v0.19.11)

**Merged pull requests:**

- Marathon expects docker parameters to be strings, not integers. [\#565](https://github.com/Yelp/paasta/pull/565) ([matthewbentley](https://github.com/matthewbentley))

## [v0.19.10](https://github.com/Yelp/paasta/tree/v0.19.10) (2016-06-07)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.19.9...v0.19.10)

**Closed issues:**

- Support cfs\_quota\_us / cfs\_period\_us [\#504](https://github.com/Yelp/paasta/issues/504)

**Merged pull requests:**

- Add cpu-period and cpu-quota to marathon logic [\#564](https://github.com/Yelp/paasta/pull/564) ([matthewbentley](https://github.com/matthewbentley))
- Upgrade to mesos 0.28.2 [\#563](https://github.com/Yelp/paasta/pull/563) ([keshavdv](https://github.com/keshavdv))
- Add support in `paasta logs` for non tailing modes [\#559](https://github.com/Yelp/paasta/pull/559) ([ammaraskar](https://github.com/ammaraskar))
- Use sphinxcontrib-programoutput instead of sphinxcontrib-autorun [\#558](https://github.com/Yelp/paasta/pull/558) ([nhandler](https://github.com/nhandler))
- Upgrade to mesos 0.28.1 [\#557](https://github.com/Yelp/paasta/pull/557) ([nhandler](https://github.com/nhandler))
- Print stdout / stderr when container exits with a code.  Resolves \#552 [\#554](https://github.com/Yelp/paasta/pull/554) ([asottile](https://github.com/asottile))
- PAASTA-4188 per attribute alerting [\#543](https://github.com/Yelp/paasta/pull/543) ([Rob-Johnson](https://github.com/Rob-Johnson))

## [v0.19.9](https://github.com/Yelp/paasta/tree/v0.19.9) (2016-06-01)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.19.8...v0.19.9)

**Closed issues:**

- paasta local-run --healthcheck-only does not print output on failure [\#552](https://github.com/Yelp/paasta/issues/552)

**Merged pull requests:**

- remove leading slash from docker container name [\#550](https://github.com/Yelp/paasta/pull/550) ([huadongliu](https://github.com/huadongliu))
- Support passing posargs through to paasta\_itests [\#549](https://github.com/Yelp/paasta/pull/549) ([nhandler](https://github.com/nhandler))
- Document config blacklist automatically [\#548](https://github.com/Yelp/paasta/pull/548) ([nhandler](https://github.com/nhandler))
- Remove misleading \* [\#546](https://github.com/Yelp/paasta/pull/546) ([asottile](https://github.com/asottile))
- Improve --dry-run help text [\#545](https://github.com/Yelp/paasta/pull/545) ([asottile](https://github.com/asottile))
- Use the force=True flag to kill marathon tasks [\#542](https://github.com/Yelp/paasta/pull/542) ([huadongliu](https://github.com/huadongliu))
- update draining and setup\_marathon\_job docs for \#529 [\#538](https://github.com/Yelp/paasta/pull/538) ([huadongliu](https://github.com/huadongliu))

## [v0.19.8](https://github.com/Yelp/paasta/tree/v0.19.8) (2016-05-25)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.19.7...v0.19.8)

**Merged pull requests:**

- disabling requests\_cache when creating/deleting a marathon app [\#540](https://github.com/Yelp/paasta/pull/540) ([huadongliu](https://github.com/huadongliu))
- Document the system paasta config options [\#539](https://github.com/Yelp/paasta/pull/539) ([EvanKrall](https://github.com/EvanKrall))
- Enabling checking of disabled chronos jobs [\#537](https://github.com/Yelp/paasta/pull/537) ([solarkennedy](https://github.com/solarkennedy))
- Make chronos respect the force bounce key [\#536](https://github.com/Yelp/paasta/pull/536) ([solarkennedy](https://github.com/solarkennedy))
- continue serviceinit when an exception occurs on one service.instance [\#535](https://github.com/Yelp/paasta/pull/535) ([huadongliu](https://github.com/huadongliu))
- Specify schema/port in paasta\_maintenance [\#532](https://github.com/Yelp/paasta/pull/532) ([nhandler](https://github.com/nhandler))
- Fixed some nits. [\#531](https://github.com/Yelp/paasta/pull/531) ([philipmulcahy](https://github.com/philipmulcahy))

## [v0.19.7](https://github.com/Yelp/paasta/tree/v0.19.7) (2016-05-17)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.19.6...v0.19.7)

**Closed issues:**

- Deb packages don't list python2.7 dependency [\#477](https://github.com/Yelp/paasta/issues/477)

**Merged pull requests:**

- install requests\_cache for setup\_marathon\_job, restore fast bouncing [\#528](https://github.com/Yelp/paasta/pull/528) ([huadongliu](https://github.com/huadongliu))
- Use named tuples in paasta metastatus [\#527](https://github.com/Yelp/paasta/pull/527) ([Rob-Johnson](https://github.com/Rob-Johnson))
- Hardcode a python2.7 dep to be compatible with all ubuntu [\#526](https://github.com/Yelp/paasta/pull/526) ([solarkennedy](https://github.com/solarkennedy))

## [v0.19.6](https://github.com/Yelp/paasta/tree/v0.19.6) (2016-05-17)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.19.5...v0.19.6)

**Merged pull requests:**

- Dont try to do mesos leader detection in any script [\#522](https://github.com/Yelp/paasta/pull/522) ([solarkennedy](https://github.com/solarkennedy))
- Migrate chronos scripts to use console\_scripts [\#520](https://github.com/Yelp/paasta/pull/520) ([solarkennedy](https://github.com/solarkennedy))

## [v0.19.5](https://github.com/Yelp/paasta/tree/v0.19.5) (2016-05-13)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.19.4...v0.19.5)

**Merged pull requests:**

- make the command the last arg to serviceinit [\#523](https://github.com/Yelp/paasta/pull/523) ([Rob-Johnson](https://github.com/Rob-Johnson))
- just pass on non-paasta rather than skipping in graceful\_container\_drain.py [\#518](https://github.com/Yelp/paasta/pull/518) ([Rob-Johnson](https://github.com/Rob-Johnson))
- Move http request cache install up to paasta serviceinit [\#517](https://github.com/Yelp/paasta/pull/517) ([huadongliu](https://github.com/huadongliu))
- Make paasta\_autoscale\_cluster and use console\_scripts for it [\#510](https://github.com/Yelp/paasta/pull/510) ([solarkennedy](https://github.com/solarkennedy))
- First pass at 'paasta maintenance' command [\#438](https://github.com/Yelp/paasta/pull/438) ([nhandler](https://github.com/nhandler))

## [v0.19.4](https://github.com/Yelp/paasta/tree/v0.19.4) (2016-05-12)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.19.3...v0.19.4)

## [v0.19.3](https://github.com/Yelp/paasta/tree/v0.19.3) (2016-05-12)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.19.2...v0.19.3)

**Merged pull requests:**

- Fix ssh stream option and increase timeout for paasta status [\#516](https://github.com/Yelp/paasta/pull/516) ([huadongliu](https://github.com/huadongliu))

## [v0.19.2](https://github.com/Yelp/paasta/tree/v0.19.2) (2016-05-11)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.19.1...v0.19.2)

**Closed issues:**

- support extra\_constraints in chronos configs [\#456](https://github.com/Yelp/paasta/issues/456)

**Merged pull requests:**

- Make setup\_marathon\_job capable of handling a list of service.instance [\#513](https://github.com/Yelp/paasta/pull/513) ([huadongliu](https://github.com/huadongliu))
- Load paasta configs from /etc/paasta recursively [\#511](https://github.com/Yelp/paasta/pull/511) ([solarkennedy](https://github.com/solarkennedy))
- Make marathon and chronos constraint logic a bit more consistent [\#499](https://github.com/Yelp/paasta/pull/499) ([solarkennedy](https://github.com/solarkennedy))

## [v0.19.1](https://github.com/Yelp/paasta/tree/v0.19.1) (2016-05-11)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.19.0...v0.19.1)

**Closed issues:**

- Make get\_mesos\_leader use mesos.cli [\#498](https://github.com/Yelp/paasta/issues/498)
- Blacklist autoscaling params from the marathon config sha [\#474](https://github.com/Yelp/paasta/issues/474)
- make paasta status be more explicit about 'rerun jobs' [\#457](https://github.com/Yelp/paasta/issues/457)

**Merged pull requests:**

- Update copyright for 2016 [\#512](https://github.com/Yelp/paasta/pull/512) ([nhandler](https://github.com/nhandler))
- Log lots more things when doing cluster autoscaling [\#509](https://github.com/Yelp/paasta/pull/509) ([solarkennedy](https://github.com/solarkennedy))
- Make paasta status use the multi-instance endpoint [\#506](https://github.com/Yelp/paasta/pull/506) ([huadongliu](https://github.com/huadongliu))
- Use mesos.cli for get\_mesos\_leader [\#505](https://github.com/Yelp/paasta/pull/505) ([nhandler](https://github.com/nhandler))

## [v0.19.0](https://github.com/Yelp/paasta/tree/v0.19.0) (2016-05-10)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.18.38...v0.19.0)

**Merged pull requests:**

- fix execute\_paasta\_serviceinit\_call for emergency-start [\#507](https://github.com/Yelp/paasta/pull/507) ([Rob-Johnson](https://github.com/Rob-Johnson))
- Validate that env settings are always strings [\#502](https://github.com/Yelp/paasta/pull/502) ([solarkennedy](https://github.com/solarkennedy))
- Show temporary status chronos jobs [\#495](https://github.com/Yelp/paasta/pull/495) ([Rob-Johnson](https://github.com/Rob-Johnson))
- Remove the 'driver' keyword from log\_writer options before passing it as kwargs to the log writer class, in case the log writer class doesn't take driver or \*\*kwargs [\#485](https://github.com/Yelp/paasta/pull/485) ([EvanKrall](https://github.com/EvanKrall))

## [v0.18.38](https://github.com/Yelp/paasta/tree/v0.18.38) (2016-05-05)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.18.37...v0.18.38)

**Closed issues:**

- Unify logging behavior [\#490](https://github.com/Yelp/paasta/issues/490)
- Typos in "paasta stop" docs [\#469](https://github.com/Yelp/paasta/issues/469)
- improve help text in check\_chronos\_jobs [\#461](https://github.com/Yelp/paasta/issues/461)

**Merged pull requests:**

- paasta local-run --dry-run [\#497](https://github.com/Yelp/paasta/pull/497) ([kentwills](https://github.com/kentwills))
- fix the bad command shown in check\_chronos\_jobs output [\#496](https://github.com/Yelp/paasta/pull/496) ([Rob-Johnson](https://github.com/Rob-Johnson))
- Fix bug in autoscaling humanize\_error [\#494](https://github.com/Yelp/paasta/pull/494) ([solarkennedy](https://github.com/solarkennedy))
- Delete get\_scribe\_map, since this functionality is now an option on the scribe log\_reader driver. [\#492](https://github.com/Yelp/paasta/pull/492) ([EvanKrall](https://github.com/EvanKrall))
- Be more consistent about how we log to stderr [\#491](https://github.com/Yelp/paasta/pull/491) ([solarkennedy](https://github.com/solarkennedy))
- PAASTA-4412 monkey patch MesosMaster.state to increase caching TTL [\#489](https://github.com/Yelp/paasta/pull/489) ([huadongliu](https://github.com/huadongliu))
- Added a cli entrypoint to autoscale\_cluster [\#482](https://github.com/Yelp/paasta/pull/482) ([solarkennedy](https://github.com/solarkennedy))
- Default to using the max instance count when autoscaling without decision data [\#481](https://github.com/Yelp/paasta/pull/481) ([solarkennedy](https://github.com/solarkennedy))

## [v0.18.37](https://github.com/Yelp/paasta/tree/v0.18.37) (2016-05-04)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.18.36...v0.18.37)

**Closed issues:**

- Expose CLI as console\_scripts [\#85](https://github.com/Yelp/paasta/issues/85)

**Merged pull requests:**

- expose CLI as console\_scripts \#85 [\#488](https://github.com/Yelp/paasta/pull/488) ([kentwills](https://github.com/kentwills))
- Greatly improve the output of check\_marathon\_services replication [\#478](https://github.com/Yelp/paasta/pull/478) ([solarkennedy](https://github.com/solarkennedy))
- Use cookiecutter instead of home-grown templates [\#471](https://github.com/Yelp/paasta/pull/471) ([solarkennedy](https://github.com/solarkennedy))

## [v0.18.36](https://github.com/Yelp/paasta/tree/v0.18.36) (2016-05-03)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.18.35...v0.18.36)

**Closed issues:**

- paasta metastatus is broken [\#486](https://github.com/Yelp/paasta/issues/486)

**Merged pull requests:**

- add missing system\_paasta\_config arg in metastatus [\#487](https://github.com/Yelp/paasta/pull/487) ([Rob-Johnson](https://github.com/Rob-Johnson))

## [v0.18.35](https://github.com/Yelp/paasta/tree/v0.18.35) (2016-05-03)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.18.34...v0.18.35)

## [v0.18.34](https://github.com/Yelp/paasta/tree/v0.18.34) (2016-05-03)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.18.33...v0.18.34)

**Merged pull requests:**

- Make the FQDN format configurable; previously hard-coded as paasta-%s.yelp [\#483](https://github.com/Yelp/paasta/pull/483) ([EvanKrall](https://github.com/EvanKrall))

## [v0.18.33](https://github.com/Yelp/paasta/tree/v0.18.33) (2016-05-02)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.18.32...v0.18.33)

**Merged pull requests:**

- PAASTA-4182 enable paasta\_serviceinit status with multiple service instances [\#480](https://github.com/Yelp/paasta/pull/480) ([huadongliu](https://github.com/huadongliu))

## [v0.18.32](https://github.com/Yelp/paasta/tree/v0.18.32) (2016-04-29)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.18.31...v0.18.32)

**Closed issues:**

- Make haproxy-synapse status url configurable [\#434](https://github.com/Yelp/paasta/issues/434)

**Merged pull requests:**

- Revert "Document which config entries invoke a bounce" [\#479](https://github.com/Yelp/paasta/pull/479) ([solarkennedy](https://github.com/solarkennedy))
- Document which config entries invoke a bounce [\#470](https://github.com/Yelp/paasta/pull/470) ([solarkennedy](https://github.com/solarkennedy))
- improve failure message sent by check\_chronos\_jobs [\#468](https://github.com/Yelp/paasta/pull/468) ([Rob-Johnson](https://github.com/Rob-Johnson))

## [v0.18.31](https://github.com/Yelp/paasta/tree/v0.18.31) (2016-04-29)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.18.30...v0.18.31)

## [v0.18.30](https://github.com/Yelp/paasta/tree/v0.18.30) (2016-04-29)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.18.29...v0.18.30)

## [v0.18.29](https://github.com/Yelp/paasta/tree/v0.18.29) (2016-04-28)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.18.28...v0.18.29)

**Merged pull requests:**

- Make synapse port configurable via SystemPaastaConfig [\#454](https://github.com/Yelp/paasta/pull/454) ([EvanKrall](https://github.com/EvanKrall))

## [v0.18.28](https://github.com/Yelp/paasta/tree/v0.18.28) (2016-04-28)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.18.27...v0.18.28)

**Merged pull requests:**

- Paasta cluster autoscaling [\#466](https://github.com/Yelp/paasta/pull/466) ([mjksmith](https://github.com/mjksmith))

## [v0.18.27](https://github.com/Yelp/paasta/tree/v0.18.27) (2016-04-28)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.18.26...v0.18.27)

**Merged pull requests:**

- Switch from readthedocs.org-\>readthedocs.io [\#467](https://github.com/Yelp/paasta/pull/467) ([nhandler](https://github.com/nhandler))

## [v0.18.26](https://github.com/Yelp/paasta/tree/v0.18.26) (2016-04-27)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.18.25...v0.18.26)

**Closed issues:**

- write some documentation on how resource isolation works [\#459](https://github.com/Yelp/paasta/issues/459)
- Create a 'paasta rerun' command [\#216](https://github.com/Yelp/paasta/issues/216)

**Merged pull requests:**

- fix some formatting in docs [\#464](https://github.com/Yelp/paasta/pull/464) ([Rob-Johnson](https://github.com/Rob-Johnson))
- Made autoscale able to scale down below 9 [\#462](https://github.com/Yelp/paasta/pull/462) ([mjksmith](https://github.com/mjksmith))
- add a first pass at some docs explaining isolation. [\#460](https://github.com/Yelp/paasta/pull/460) ([Rob-Johnson](https://github.com/Rob-Johnson))
- improve the documentation on paasta batches [\#458](https://github.com/Yelp/paasta/pull/458) ([Rob-Johnson](https://github.com/Rob-Johnson))
- removed service\_configuration\_lib.DEFAULT\_SOA\_DIR [\#455](https://github.com/Yelp/paasta/pull/455) ([mjksmith](https://github.com/mjksmith))

## [v0.18.25](https://github.com/Yelp/paasta/tree/v0.18.25) (2016-04-25)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.18.24...v0.18.25)

**Merged pull requests:**

- made the default metrics provider mesos\_cpu [\#453](https://github.com/Yelp/paasta/pull/453) ([mjksmith](https://github.com/mjksmith))
- interpolate datestrings when using local-run with a chronos job [\#451](https://github.com/Yelp/paasta/pull/451) ([Rob-Johnson](https://github.com/Rob-Johnson))

## [v0.18.24](https://github.com/Yelp/paasta/tree/v0.18.24) (2016-04-21)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.18.23...v0.18.24)

**Merged pull requests:**

- Use marathon 1.1.1 in itest and upgrade marathon-python [\#433](https://github.com/Yelp/paasta/pull/433) ([nhandler](https://github.com/nhandler))

## [v0.18.23](https://github.com/Yelp/paasta/tree/v0.18.23) (2016-04-21)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.18.22...v0.18.23)

**Merged pull requests:**

- fixed error running paasta metastatus [\#449](https://github.com/Yelp/paasta/pull/449) ([mjksmith](https://github.com/mjksmith))
- fix "paasta rerun" crashes \(PAASTA-4152\) [\#448](https://github.com/Yelp/paasta/pull/448) ([giuliano108](https://github.com/giuliano108))
- Don't alert teams if a service hasn't been deployed yet [\#435](https://github.com/Yelp/paasta/pull/435) ([solarkennedy](https://github.com/solarkennedy))

## [v0.18.22](https://github.com/Yelp/paasta/tree/v0.18.22) (2016-04-20)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.18.21...v0.18.22)

**Closed issues:**

- paasta local-run --healthcheck-only exits 0 even when the healthcheck never passes [\#445](https://github.com/Yelp/paasta/issues/445)

**Merged pull requests:**

- made local\_run --healthcheck-only return 1 when healthchecks fail [\#446](https://github.com/Yelp/paasta/pull/446) ([mjksmith](https://github.com/mjksmith))
- Made almost every cli tool take a soa\_dir [\#444](https://github.com/Yelp/paasta/pull/444) ([mjksmith](https://github.com/mjksmith))

## [v0.18.21](https://github.com/Yelp/paasta/tree/v0.18.21) (2016-04-20)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.18.20...v0.18.21)

**Closed issues:**

- paasta check for DOCKER\_TAG is too strict [\#439](https://github.com/Yelp/paasta/issues/439)
- Update paasta local run to include memory-swap parameter [\#428](https://github.com/Yelp/paasta/issues/428)
- only require a date to chronos-rerun if there is something to be interpolated [\#420](https://github.com/Yelp/paasta/issues/420)

**Merged pull requests:**

- s/PST/America\/Los\_Angeles/ [\#441](https://github.com/Yelp/paasta/pull/441) ([nhandler](https://github.com/nhandler))
- Make DOCKER\_TAG check less strict [\#440](https://github.com/Yelp/paasta/pull/440) ([EvanKrall](https://github.com/EvanKrall))
- always round --memory-swap param up to closest mb [\#437](https://github.com/Yelp/paasta/pull/437) ([Rob-Johnson](https://github.com/Rob-Johnson))
- Unknown slaves in "paasta status" workaround \(PAASTA-4119\) [\#436](https://github.com/Yelp/paasta/pull/436) ([giuliano108](https://github.com/giuliano108))
- It's dockercfg, not dockerfile. [\#432](https://github.com/Yelp/paasta/pull/432) ([EvanKrall](https://github.com/EvanKrall))
- Local run memory swappiness [\#430](https://github.com/Yelp/paasta/pull/430) ([Rob-Johnson](https://github.com/Rob-Johnson))

## [v0.18.20](https://github.com/Yelp/paasta/tree/v0.18.20) (2016-04-15)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.18.19...v0.18.20)

**Merged pull requests:**

- removed portMapping from non-bridge nets [\#431](https://github.com/Yelp/paasta/pull/431) ([mjksmith](https://github.com/mjksmith))

## [v0.18.19](https://github.com/Yelp/paasta/tree/v0.18.19) (2016-04-15)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.18.18...v0.18.19)

## [v0.18.18](https://github.com/Yelp/paasta/tree/v0.18.18) (2016-04-15)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.18.17...v0.18.18)

**Merged pull requests:**

- made bespoke autoscaling short circuit earlier [\#429](https://github.com/Yelp/paasta/pull/429) ([mjksmith](https://github.com/mjksmith))

## [v0.18.17](https://github.com/Yelp/paasta/tree/v0.18.17) (2016-04-14)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.18.16...v0.18.17)

**Merged pull requests:**

- Make the argparser print the full help whenever an argparse issue is encountered [\#422](https://github.com/Yelp/paasta/pull/422) ([solarkennedy](https://github.com/solarkennedy))

## [v0.18.16](https://github.com/Yelp/paasta/tree/v0.18.16) (2016-04-13)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.18.15...v0.18.16)

**Closed issues:**

- create a frontend paasta rerun command [\#323](https://github.com/Yelp/paasta/issues/323)

**Merged pull requests:**

- create a frontend paasta rerun command [\#419](https://github.com/Yelp/paasta/pull/419) ([giuliano108](https://github.com/giuliano108))

## [v0.18.15](https://github.com/Yelp/paasta/tree/v0.18.15) (2016-04-13)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.18.14...v0.18.15)

**Closed issues:**

- temporary jobs created by chronos-rerun trigger a sensu alert for check\_chronos\_jobs [\#421](https://github.com/Yelp/paasta/issues/421)
- set the "memory-swap" parameter in docker containers. [\#410](https://github.com/Yelp/paasta/issues/410)

**Merged pull requests:**

- Add memory swap param [\#411](https://github.com/Yelp/paasta/pull/411) ([Rob-Johnson](https://github.com/Rob-Johnson))
- Clean up orphaned docker container script [\#352](https://github.com/Yelp/paasta/pull/352) ([nosmo](https://github.com/nosmo))

## [v0.18.14](https://github.com/Yelp/paasta/tree/v0.18.14) (2016-04-11)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.18.13...v0.18.14)

**Merged pull requests:**

- ignore temporary jobs when checking chronos jobs [\#425](https://github.com/Yelp/paasta/pull/425) ([Rob-Johnson](https://github.com/Rob-Johnson))
- added healthcheck output to marathon status [\#423](https://github.com/Yelp/paasta/pull/423) ([mjksmith](https://github.com/mjksmith))
- Revert "Merge branch 'dont-require-start-time'" [\#418](https://github.com/Yelp/paasta/pull/418) ([Rob-Johnson](https://github.com/Rob-Johnson))
- Upgrade marathon to 0.15.3-1.0.463.ubuntu1404 in itests [\#415](https://github.com/Yelp/paasta/pull/415) ([nhandler](https://github.com/nhandler))

## [v0.18.13](https://github.com/Yelp/paasta/tree/v0.18.13) (2016-04-07)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.18.12...v0.18.13)

**Merged pull requests:**

- fix repo fingerprints [\#417](https://github.com/Yelp/paasta/pull/417) ([ysyoung](https://github.com/ysyoung))
- Bump mesos.interface to 0.28.0 [\#416](https://github.com/Yelp/paasta/pull/416) ([nhandler](https://github.com/nhandler))
- Use json for mesos authentication [\#413](https://github.com/Yelp/paasta/pull/413) ([nhandler](https://github.com/nhandler))
- Removed variable delay and other params from autoscaling [\#394](https://github.com/Yelp/paasta/pull/394) ([mjksmith](https://github.com/mjksmith))
- made local run create a fresh deployments.json [\#368](https://github.com/Yelp/paasta/pull/368) ([mjksmith](https://github.com/mjksmith))

## [v0.18.12](https://github.com/Yelp/paasta/tree/v0.18.12) (2016-04-07)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.18.11...v0.18.12)

**Closed issues:**

- Odd formatting in pasta fsm --yelpsoa-config-root [\#396](https://github.com/Yelp/paasta/issues/396)

**Merged pull requests:**

- Graceful container drain [\#501](https://github.com/Yelp/paasta/pull/501) ([Rob-Johnson](https://github.com/Rob-Johnson))
- fixed net host marathon error [\#412](https://github.com/Yelp/paasta/pull/412) ([mjksmith](https://github.com/mjksmith))
- fixed formatting in fsm [\#407](https://github.com/Yelp/paasta/pull/407) ([mjksmith](https://github.com/mjksmith))
- Upgrade tests to use marathon 0.14.2 [\#403](https://github.com/Yelp/paasta/pull/403) ([nhandler](https://github.com/nhandler))

## [v0.18.11](https://github.com/Yelp/paasta/tree/v0.18.11) (2016-04-07)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.18.10...v0.18.11)

**Closed issues:**

- chronos jobs require start times [\#383](https://github.com/Yelp/paasta/issues/383)
- Add deploy\_whitelist [\#251](https://github.com/Yelp/paasta/issues/251)
- location of dockercfg file is hardcoded [\#6](https://github.com/Yelp/paasta/issues/6)

**Merged pull requests:**

- Upgrade mesos to 0.28.0-2.0.16.ubuntu1404 in itests [\#408](https://github.com/Yelp/paasta/pull/408) ([nhandler](https://github.com/nhandler))
- added soa\_dir argument to paasta itest [\#406](https://github.com/Yelp/paasta/pull/406) ([mjksmith](https://github.com/mjksmith))
- Upgrade mesos to 0.27.2-2.0.15.ubuntu1404 in the itests [\#405](https://github.com/Yelp/paasta/pull/405) ([nhandler](https://github.com/nhandler))
- added host networking [\#404](https://github.com/Yelp/paasta/pull/404) ([mjksmith](https://github.com/mjksmith))
- Add support for whitelisting. [\#402](https://github.com/Yelp/paasta/pull/402) ([ysyoung](https://github.com/ysyoung))
- Make is\_app\_id\_running tolerate leading slashes [\#401](https://github.com/Yelp/paasta/pull/401) ([solarkennedy](https://github.com/solarkennedy))
- Make the marathon upgrade docs match current practice [\#400](https://github.com/Yelp/paasta/pull/400) ([nhandler](https://github.com/nhandler))
- remove the requirement for a start time in chronos schedules [\#398](https://github.com/Yelp/paasta/pull/398) ([Rob-Johnson](https://github.com/Rob-Johnson))
- make docker config location configurable [\#395](https://github.com/Yelp/paasta/pull/395) ([ysyoung](https://github.com/ysyoung))

## [v0.18.10](https://github.com/Yelp/paasta/tree/v0.18.10) (2016-04-04)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.18.9...v0.18.10)

**Closed issues:**

- test\_filter\_expired\_tmp\_jobs is failing on master [\#392](https://github.com/Yelp/paasta/issues/392)
- cleanup\_chronos\_jobs failing on tmp jobs [\#388](https://github.com/Yelp/paasta/issues/388)
- test\_format\_parents\_verbose fails due to hardcoded relative date [\#387](https://github.com/Yelp/paasta/issues/387)
- chronos\_rerun -h blows up [\#385](https://github.com/Yelp/paasta/issues/385)

**Merged pull requests:**

- fix test missing tzinfo [\#393](https://github.com/Yelp/paasta/pull/393) ([Rob-Johnson](https://github.com/Rob-Johnson))
- Upgrade to marathon 0.13.1 in itests [\#375](https://github.com/Yelp/paasta/pull/375) ([nhandler](https://github.com/nhandler))

## [v0.18.9](https://github.com/Yelp/paasta/tree/v0.18.9) (2016-04-04)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.18.8...v0.18.9)

**Merged pull requests:**

- fix escaping in chronos\_rerun help string [\#391](https://github.com/Yelp/paasta/pull/391) ([Rob-Johnson](https://github.com/Rob-Johnson))
- fix test dependent on time [\#390](https://github.com/Yelp/paasta/pull/390) ([Rob-Johnson](https://github.com/Rob-Johnson))
- parse the datetime string from lastruntime in chronos jobs [\#389](https://github.com/Yelp/paasta/pull/389) ([Rob-Johnson](https://github.com/Rob-Johnson))
- Autoscaling improvements [\#386](https://github.com/Yelp/paasta/pull/386) ([mjksmith](https://github.com/mjksmith))
- Make the marathon cleanup script resolve all related sensu events. [\#379](https://github.com/Yelp/paasta/pull/379) ([solarkennedy](https://github.com/solarkennedy))

## [v0.18.8](https://github.com/Yelp/paasta/tree/v0.18.8) (2016-04-01)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.18.7...v0.18.8)

**Merged pull requests:**

- added a command to autoscale all services [\#382](https://github.com/Yelp/paasta/pull/382) ([mjksmith](https://github.com/mjksmith))

## [v0.18.7](https://github.com/Yelp/paasta/tree/v0.18.7) (2016-04-01)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.18.6...v0.18.7)

**Closed issues:**

- make `cleanup\_chronos\_jobs` aware of temporary jobs created by `paasta rerun` [\#324](https://github.com/Yelp/paasta/issues/324)
- create a backend 'paasta rerun' command [\#322](https://github.com/Yelp/paasta/issues/322)

**Merged pull requests:**

- ensure chronos rerun is distributed properly [\#384](https://github.com/Yelp/paasta/pull/384) ([Rob-Johnson](https://github.com/Rob-Johnson))

## [v0.18.6](https://github.com/Yelp/paasta/tree/v0.18.6) (2016-04-01)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.18.5...v0.18.6)

**Merged pull requests:**

- Ensure cleanup jobs understands tmp jobs [\#354](https://github.com/Yelp/paasta/pull/354) ([Rob-Johnson](https://github.com/Rob-Johnson))
- Create a ``chronos\_rerun`` script for rerunning past jobs [\#330](https://github.com/Yelp/paasta/pull/330) ([Rob-Johnson](https://github.com/Rob-Johnson))

## [v0.18.5](https://github.com/Yelp/paasta/tree/v0.18.5) (2016-04-01)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.18.4...v0.18.5)

**Closed issues:**

- Fix Upgrading Marathon Docs now that we use upstream marathon-python [\#378](https://github.com/Yelp/paasta/issues/378)

**Merged pull requests:**

- Set real disabled value [\#381](https://github.com/Yelp/paasta/pull/381) ([Rob-Johnson](https://github.com/Rob-Johnson))
- Remove docs about adding marathon-python to internal pypi [\#380](https://github.com/Yelp/paasta/pull/380) ([nhandler](https://github.com/nhandler))
- Bump mesos.interface to 0.25.0 [\#377](https://github.com/Yelp/paasta/pull/377) ([nhandler](https://github.com/nhandler))
- Only deploy to pypi on py27 build [\#372](https://github.com/Yelp/paasta/pull/372) ([EvanKrall](https://github.com/EvanKrall))

## [v0.18.4](https://github.com/Yelp/paasta/tree/v0.18.4) (2016-03-31)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.18.3...v0.18.4)

**Closed issues:**

- getting started docs are incorrect outside of Yelp [\#82](https://github.com/Yelp/paasta/issues/82)
- wrong version of mesos.interface being installed [\#81](https://github.com/Yelp/paasta/issues/81)

**Merged pull requests:**

- stopped alerting for disabled jobs on chronos [\#361](https://github.com/Yelp/paasta/pull/361) ([mjksmith](https://github.com/mjksmith))
- Added a default autoscaling method [\#303](https://github.com/Yelp/paasta/pull/303) ([mjksmith](https://github.com/mjksmith))

## [v0.18.3](https://github.com/Yelp/paasta/tree/v0.18.3) (2016-03-30)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.18.2...v0.18.3)

**Closed issues:**

- Typo in "paasta local-run -h" [\#369](https://github.com/Yelp/paasta/issues/369)
- Switching from branches to tags made it harder to get currently deployed git sha [\#359](https://github.com/Yelp/paasta/issues/359)

**Merged pull requests:**

- Use behave-pytest for pytest-style asserts [\#371](https://github.com/Yelp/paasta/pull/371) ([nhandler](https://github.com/nhandler))
- added a command to get a deploy group's latest deploy sha [\#357](https://github.com/Yelp/paasta/pull/357) ([mjksmith](https://github.com/mjksmith))
- Upgrade to mesos 0.25.0 [\#319](https://github.com/Yelp/paasta/pull/319) ([nhandler](https://github.com/nhandler))

## [v0.18.2](https://github.com/Yelp/paasta/tree/v0.18.2) (2016-03-25)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.18.1...v0.18.2)

**Merged pull requests:**

- made args for deep\_merge\_dictionaries more descriptive [\#363](https://github.com/Yelp/paasta/pull/363) ([mjksmith](https://github.com/mjksmith))

## [v0.18.1](https://github.com/Yelp/paasta/tree/v0.18.1) (2016-03-25)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.18.0...v0.18.1)

**Closed issues:**

- Add Bintray badge to readme [\#364](https://github.com/Yelp/paasta/issues/364)
- Make packages available publicly [\#318](https://github.com/Yelp/paasta/issues/318)

**Merged pull requests:**

- Only have travis build on master \(and pull requests\). [\#366](https://github.com/Yelp/paasta/pull/366) ([EvanKrall](https://github.com/EvanKrall))
- Add Bintray Badge to README [\#365](https://github.com/Yelp/paasta/pull/365) ([nhandler](https://github.com/nhandler))
- Fix bintray upload [\#362](https://github.com/Yelp/paasta/pull/362) ([EvanKrall](https://github.com/EvanKrall))
- fixed a bug where smartstack.yaml was overriding marathon.yaml [\#360](https://github.com/Yelp/paasta/pull/360) ([mjksmith](https://github.com/mjksmith))

## [v0.18.0](https://github.com/Yelp/paasta/tree/v0.18.0) (2016-03-25)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.17.20...v0.18.0)

**Closed issues:**

- Marathon doesn't like non-string values in constraints [\#333](https://github.com/Yelp/paasta/issues/333)
- write docs on how to disable a chronos job [\#294](https://github.com/Yelp/paasta/issues/294)

**Merged pull requests:**

- stringify any constraint vals for marathon [\#356](https://github.com/Yelp/paasta/pull/356) ([Rob-Johnson](https://github.com/Rob-Johnson))
- Add mass-deploy-tag.sh to contrib/ [\#347](https://github.com/Yelp/paasta/pull/347) ([nhandler](https://github.com/nhandler))
- Move configuration of replication monitoring to yelpsoa config [\#345](https://github.com/Yelp/paasta/pull/345) ([nosmo](https://github.com/nosmo))
- Public package build [\#337](https://github.com/Yelp/paasta/pull/337) ([EvanKrall](https://github.com/EvanKrall))

## [v0.17.20](https://github.com/Yelp/paasta/tree/v0.17.20) (2016-03-23)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.17.19...v0.17.20)

**Closed issues:**

- cleanup\_marathon\_jobs crashing because marathon.yaml doesn't exist [\#350](https://github.com/Yelp/paasta/issues/350)
- metastatus ZeroDivisionError [\#348](https://github.com/Yelp/paasta/issues/348)
- Include schedule\_time\_zone in schema [\#338](https://github.com/Yelp/paasta/issues/338)
- UnboundLocalError when Popen fails [\#60](https://github.com/Yelp/paasta/issues/60)

**Merged pull requests:**

- made cleanup\_marathon\_job not load soa configs for a deleted service [\#351](https://github.com/Yelp/paasta/pull/351) ([mjksmith](https://github.com/mjksmith))
- made metastatus print sane errors if mesos states it has 0 resources [\#349](https://github.com/Yelp/paasta/pull/349) ([mjksmith](https://github.com/mjksmith))
- fixed \_run throwing an error when popen failed with a timeout [\#346](https://github.com/Yelp/paasta/pull/346) ([mjksmith](https://github.com/mjksmith))
- added schedule\_time\_zone to chronos schema [\#344](https://github.com/Yelp/paasta/pull/344) ([mjksmith](https://github.com/mjksmith))
- Make -vv output consistent with -vvv output, humanize figures [\#340](https://github.com/Yelp/paasta/pull/340) ([nosmo](https://github.com/nosmo))
- stopped per-instance configs from nuking global defaults [\#331](https://github.com/Yelp/paasta/pull/331) ([mjksmith](https://github.com/mjksmith))
- Use tags rather than branches for deployments [\#326](https://github.com/Yelp/paasta/pull/326) ([nhandler](https://github.com/nhandler))
- add some docs for paasta start/stop behaviour [\#314](https://github.com/Yelp/paasta/pull/314) ([Rob-Johnson](https://github.com/Rob-Johnson))

## [v0.17.19](https://github.com/Yelp/paasta/tree/v0.17.19) (2016-03-18)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.17.18...v0.17.19)

**Closed issues:**

- Broken links in docs [\#336](https://github.com/Yelp/paasta/issues/336)

**Merged pull requests:**

- Fix broken links to drain\_lib / bounce\_lib docs. Fixes \#336 [\#342](https://github.com/Yelp/paasta/pull/342) ([EvanKrall](https://github.com/EvanKrall))
- fixed paasta fsm creating invalid deploy.yaml [\#339](https://github.com/Yelp/paasta/pull/339) ([mjksmith](https://github.com/mjksmith))
- Catch failed responses during http healthcheck [\#332](https://github.com/Yelp/paasta/pull/332) ([koikonom](https://github.com/koikonom))

## [v0.17.18](https://github.com/Yelp/paasta/tree/v0.17.18) (2016-03-16)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.17.17...v0.17.18)

## [v0.17.17](https://github.com/Yelp/paasta/tree/v0.17.17) (2016-03-16)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.17.16...v0.17.17)

**Closed issues:**

- create alternative backend for logging [\#64](https://github.com/Yelp/paasta/issues/64)

**Merged pull requests:**

- Upgrade mesos.interface to 0.24.1 [\#329](https://github.com/Yelp/paasta/pull/329) ([nhandler](https://github.com/nhandler))
- Allow 'paasta re.start -c' to use a list of clusters [\#328](https://github.com/Yelp/paasta/pull/328) ([nosmo](https://github.com/nosmo))

## [v0.17.16](https://github.com/Yelp/paasta/tree/v0.17.16) (2016-03-14)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.17.15...v0.17.16)

**Merged pull requests:**

- Fix `paasta status -vv` [\#325](https://github.com/Yelp/paasta/pull/325) ([giuliano108](https://github.com/giuliano108))
- Display more data when doing the healthcheck during local-run [\#321](https://github.com/Yelp/paasta/pull/321) ([solarkennedy](https://github.com/solarkennedy))

## [v0.17.15](https://github.com/Yelp/paasta/tree/v0.17.15) (2016-03-11)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.17.14...v0.17.15)

**Closed issues:**

- setting 'disable: true' in a chronos job is a noop unless you also 'paasta stop' the job [\#283](https://github.com/Yelp/paasta/issues/283)

**Merged pull requests:**

- Mesos 0.24.1 Passes Tests! [\#317](https://github.com/Yelp/paasta/pull/317) ([nhandler](https://github.com/nhandler))
- Use authentication with mesos in itests [\#316](https://github.com/Yelp/paasta/pull/316) ([nhandler](https://github.com/nhandler))
- Print metastatus free resources alongside total resource capacity [\#315](https://github.com/Yelp/paasta/pull/315) ([nosmo](https://github.com/nosmo))
- Make FileLogWriter, which writes json-formatted log lines to a specified file. [\#313](https://github.com/Yelp/paasta/pull/313) ([EvanKrall](https://github.com/EvanKrall))
- Do the mesos upgrade again [\#312](https://github.com/Yelp/paasta/pull/312) ([nhandler](https://github.com/nhandler))
- added clean\_up\_zookeeper\_autoscaling to environment.py [\#310](https://github.com/Yelp/paasta/pull/310) ([mjksmith](https://github.com/mjksmith))
- increased test coverage for marathon\_tools [\#309](https://github.com/Yelp/paasta/pull/309) ([mjksmith](https://github.com/mjksmith))
- fixed the default autoscaling config args [\#308](https://github.com/Yelp/paasta/pull/308) ([mjksmith](https://github.com/mjksmith))
- fixed a log line in generate\_deployments\_for\_service [\#307](https://github.com/Yelp/paasta/pull/307) ([mjksmith](https://github.com/mjksmith))
- ensure disabled flag is considered when computing config hash for chr… [\#306](https://github.com/Yelp/paasta/pull/306) ([Rob-Johnson](https://github.com/Rob-Johnson))
- added an autocomplete to rollback git shas [\#305](https://github.com/Yelp/paasta/pull/305) ([mjksmith](https://github.com/mjksmith))
- Added more explicit start/stop/restart messages [\#302](https://github.com/Yelp/paasta/pull/302) ([solarkennedy](https://github.com/solarkennedy))
- Add stdout/err tail snippet to paasta status -vv \(PAASTA-3268\) [\#301](https://github.com/Yelp/paasta/pull/301) ([giuliano108](https://github.com/giuliano108))
- Order tasks in verbose paasta status [\#295](https://github.com/Yelp/paasta/pull/295) ([drolando](https://github.com/drolando))
- Added an autoscaling\_method field [\#290](https://github.com/Yelp/paasta/pull/290) ([mjksmith](https://github.com/mjksmith))
- Update docs to note Marathon env\_var\_prefix [\#286](https://github.com/Yelp/paasta/pull/286) ([nhandler](https://github.com/nhandler))
- Pluggable log systems [\#281](https://github.com/Yelp/paasta/pull/281) ([EvanKrall](https://github.com/EvanKrall))

## [v0.17.14](https://github.com/Yelp/paasta/tree/v0.17.14) (2016-03-07)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.17.13...v0.17.14)

**Closed issues:**

- Can't run paasta itests when using docker http api [\#287](https://github.com/Yelp/paasta/issues/287)
- ``check\_chronos\_job`` alerts when a job is disabled [\#284](https://github.com/Yelp/paasta/issues/284)

**Merged pull requests:**

- Redo mesos.cli upgrade [\#304](https://github.com/Yelp/paasta/pull/304) ([nhandler](https://github.com/nhandler))
- Allow paasta to use docker http api [\#300](https://github.com/Yelp/paasta/pull/300) ([keshavdv](https://github.com/keshavdv))
- refactored create\_complete\_config [\#298](https://github.com/Yelp/paasta/pull/298) ([mjksmith](https://github.com/mjksmith))
- Remove disabled field from chronos soa-configs [\#293](https://github.com/Yelp/paasta/pull/293) ([Rob-Johnson](https://github.com/Rob-Johnson))
- send OK event when a chronos job has been disabled [\#292](https://github.com/Yelp/paasta/pull/292) ([Rob-Johnson](https://github.com/Rob-Johnson))

## [v0.17.13](https://github.com/Yelp/paasta/tree/v0.17.13) (2016-03-04)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.17.12...v0.17.13)

**Merged pull requests:**

- Revert "Upgrade mesos.cli for mesos 0.24.1 upgrade" [\#297](https://github.com/Yelp/paasta/pull/297) ([nhandler](https://github.com/nhandler))
- added autoscaling docs [\#296](https://github.com/Yelp/paasta/pull/296) ([mjksmith](https://github.com/mjksmith))
- respect the alert\_after param in check\_chronos\_jobs [\#291](https://github.com/Yelp/paasta/pull/291) ([Rob-Johnson](https://github.com/Rob-Johnson))
- Mesos 0.24.1 [\#289](https://github.com/Yelp/paasta/pull/289) ([nhandler](https://github.com/nhandler))

## [v0.17.12](https://github.com/Yelp/paasta/tree/v0.17.12) (2016-03-04)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.17.11...v0.17.12)

**Merged pull requests:**

- Set MARATHON\_PORT env variable in local-run [\#285](https://github.com/Yelp/paasta/pull/285) ([nhandler](https://github.com/nhandler))
- Upgrade mesos in itests to 0.24.1 [\#233](https://github.com/Yelp/paasta/pull/233) ([nhandler](https://github.com/nhandler))

## [v0.17.11](https://github.com/Yelp/paasta/tree/v0.17.11) (2016-03-03)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.17.10n...v0.17.11)

**Closed issues:**

- paasta metastatus should show units in output [\#277](https://github.com/Yelp/paasta/issues/277)
- paasta fsm generating bad config [\#270](https://github.com/Yelp/paasta/issues/270)
- paasta start/stop/restart should provide helpful hints if it can't talk to git [\#269](https://github.com/Yelp/paasta/issues/269)
- error running metastatus -vv [\#257](https://github.com/Yelp/paasta/issues/257)
- Rename /state.json endpoint in conjunction with mesos 0.24.x to 0.25.x upgrade [\#232](https://github.com/Yelp/paasta/issues/232)

**Merged pull requests:**

- Fix an output bug in paasta check [\#282](https://github.com/Yelp/paasta/pull/282) ([mjksmith](https://github.com/mjksmith))
- add a note about the parents field in yelpsoa-configs [\#280](https://github.com/Yelp/paasta/pull/280) ([Rob-Johnson](https://github.com/Rob-Johnson))
- added docs for deploy groups string interpolation [\#279](https://github.com/Yelp/paasta/pull/279) ([mjksmith](https://github.com/mjksmith))
- fix some dead links in the docs [\#278](https://github.com/Yelp/paasta/pull/278) ([Rob-Johnson](https://github.com/Rob-Johnson))
- Updated /state endpoint for mesos 0.25.x upgrade [\#276](https://github.com/Yelp/paasta/pull/276) ([ameya-pandilwar](https://github.com/ameya-pandilwar))
- Allow marathon instance counts to be read from zookeeper [\#275](https://github.com/Yelp/paasta/pull/275) ([mjksmith](https://github.com/mjksmith))
- Added a more friendly error message when git isn't reachable. [\#274](https://github.com/Yelp/paasta/pull/274) ([solarkennedy](https://github.com/solarkennedy))
- Use returncode instead of sys.exit in the paasta cli [\#273](https://github.com/Yelp/paasta/pull/273) ([solarkennedy](https://github.com/solarkennedy))
- Add extra\_constraints and pool to schemas for paasta validate [\#272](https://github.com/Yelp/paasta/pull/272) ([nhandler](https://github.com/nhandler))
- Added missing pool docs [\#271](https://github.com/Yelp/paasta/pull/271) ([solarkennedy](https://github.com/solarkennedy))
- Fix metastatus -vv [\#262](https://github.com/Yelp/paasta/pull/262) ([nhandler](https://github.com/nhandler))

## [v0.17.10n](https://github.com/Yelp/paasta/tree/v0.17.10n) (2016-02-25)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.17.10...v0.17.10n)

## [v0.17.10](https://github.com/Yelp/paasta/tree/v0.17.10) (2016-02-25)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.17.9n...v0.17.10)

**Merged pull requests:**

- changed do\_bounce to kill task w/the batch endpoint [\#267](https://github.com/Yelp/paasta/pull/267) ([mjksmith](https://github.com/mjksmith))

## [v0.17.9n](https://github.com/Yelp/paasta/tree/v0.17.9n) (2016-02-25)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.17.9...v0.17.9n)

## [v0.17.9](https://github.com/Yelp/paasta/tree/v0.17.9) (2016-02-25)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.17.8...v0.17.9)

**Closed issues:**

- PORT exported by paasta makes it difficult to do dockerfiles according to best practices [\#152](https://github.com/Yelp/paasta/issues/152)

**Merged pull requests:**

- Pass --env\_vars\_prefix to marathon and remove local\_run workaround [\#266](https://github.com/Yelp/paasta/pull/266) ([nhandler](https://github.com/nhandler))

## [v0.17.8](https://github.com/Yelp/paasta/tree/v0.17.8) (2016-02-24)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.17.7...v0.17.8)

## [v0.17.7](https://github.com/Yelp/paasta/tree/v0.17.7) (2016-02-24)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.17.6...v0.17.7)

## [v0.17.6](https://github.com/Yelp/paasta/tree/v0.17.6) (2016-02-24)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.17.5...v0.17.6)

## [v0.17.5](https://github.com/Yelp/paasta/tree/v0.17.5) (2016-02-24)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.17.4...v0.17.5)

## [v0.17.4](https://github.com/Yelp/paasta/tree/v0.17.4) (2016-02-24)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.17.3...v0.17.4)

## [v0.17.3](https://github.com/Yelp/paasta/tree/v0.17.3) (2016-02-23)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.17.2...v0.17.3)

## [v0.17.2](https://github.com/Yelp/paasta/tree/v0.17.2) (2016-02-23)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.17.1...v0.17.2)

**Merged pull requests:**

- enabled scaling without bounces when editing soa\_configs [\#255](https://github.com/Yelp/paasta/pull/255) ([mjksmith](https://github.com/mjksmith))

## [v0.17.1](https://github.com/Yelp/paasta/tree/v0.17.1) (2016-02-23)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.17.0...v0.17.1)

**Closed issues:**

- Rename `--deploy\_group` to `--deploy-group` [\#243](https://github.com/Yelp/paasta/issues/243)
- we should validate that a given service.instance parent exists before trying to deploy [\#148](https://github.com/Yelp/paasta/issues/148)
- add validation for chronos job parents [\#97](https://github.com/Yelp/paasta/issues/97)

**Merged pull requests:**

- Renamed --deploy\_group to --deploy-group [\#264](https://github.com/Yelp/paasta/pull/264) ([ameya-pandilwar](https://github.com/ameya-pandilwar))
- made setup\_chronos\_jobs handle non-existant jobs [\#263](https://github.com/Yelp/paasta/pull/263) ([mjksmith](https://github.com/mjksmith))
- added string interpolation to deploy groups [\#259](https://github.com/Yelp/paasta/pull/259) ([mjksmith](https://github.com/mjksmith))
- validate dependent jobs' parents - issue 148 [\#245](https://github.com/Yelp/paasta/pull/245) ([giuliano108](https://github.com/giuliano108))
- re-ordered default deploy order in fsm [\#242](https://github.com/Yelp/paasta/pull/242) ([mjksmith](https://github.com/mjksmith))

## [v0.17.0](https://github.com/Yelp/paasta/tree/v0.17.0) (2016-02-17)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.16.34...v0.17.0)

**Closed issues:**

- error in check\_marathon\_services\_replication when a task hasn't started [\#258](https://github.com/Yelp/paasta/issues/258)

**Merged pull requests:**

- marathon tasks with started\_at of None are now unhealthy [\#261](https://github.com/Yelp/paasta/pull/261) ([mjksmith](https://github.com/mjksmith))
- No longer use paasta\_execute\_in\_container for command healthchecks. [\#260](https://github.com/Yelp/paasta/pull/260) ([solarkennedy](https://github.com/solarkennedy))
- made metastatus catch when no dashboard in configured [\#253](https://github.com/Yelp/paasta/pull/253) ([mjksmith](https://github.com/mjksmith))

## [v0.16.34](https://github.com/Yelp/paasta/tree/v0.16.34) (2016-02-11)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.16.33...v0.16.34)

**Merged pull requests:**

- Update paasta check sensu\_check and smartstack\_check to pass soa\_dir [\#254](https://github.com/Yelp/paasta/pull/254) ([nhandler](https://github.com/nhandler))
- Revert "Revert "Mark for deployment tags take2"" [\#252](https://github.com/Yelp/paasta/pull/252) ([nhandler](https://github.com/nhandler))

## [v0.16.33](https://github.com/Yelp/paasta/tree/v0.16.33) (2016-02-11)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.16.32...v0.16.33)

**Closed issues:**

- Control tags besides start/stop are all interpreted as stop. [\#223](https://github.com/Yelp/paasta/issues/223)

**Merged pull requests:**

- Revert "Mark for deployment tags take2" [\#250](https://github.com/Yelp/paasta/pull/250) ([nhandler](https://github.com/nhandler))
- Added experimental script to kill old marathon deployments [\#249](https://github.com/Yelp/paasta/pull/249) ([solarkennedy](https://github.com/solarkennedy))
- made metastatus read from /etc/paasta to get dashboards [\#248](https://github.com/Yelp/paasta/pull/248) ([mjksmith](https://github.com/mjksmith))
- fix typo in docs [\#247](https://github.com/Yelp/paasta/pull/247) ([Rob-Johnson](https://github.com/Rob-Johnson))
- Fixed typo in docs/source/deploy\_groups.rst [\#246](https://github.com/Yelp/paasta/pull/246) ([drolando](https://github.com/drolando))
- Use verbose flags instead of debug in paasta logs [\#244](https://github.com/Yelp/paasta/pull/244) ([solarkennedy](https://github.com/solarkennedy))
- Added metastatus -vv [\#234](https://github.com/Yelp/paasta/pull/234) ([mjksmith](https://github.com/mjksmith))
- Mark for deployment tags take2 [\#231](https://github.com/Yelp/paasta/pull/231) ([nhandler](https://github.com/nhandler))

## [v0.16.32](https://github.com/Yelp/paasta/tree/v0.16.32) (2016-02-09)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.16.31...v0.16.32)

**Merged pull requests:**

- Add support for disk resource isolation [\#241](https://github.com/Yelp/paasta/pull/241) ([nhandler](https://github.com/nhandler))
- removed unnecessary arg from generate\_deployments.get\_desired\_state [\#240](https://github.com/Yelp/paasta/pull/240) ([mjksmith](https://github.com/mjksmith))
- reduced kill\_orphaned\_docker\_containers runtime [\#239](https://github.com/Yelp/paasta/pull/239) ([mjksmith](https://github.com/mjksmith))
- made paasta metastatus display total cpu and mem on critical status [\#237](https://github.com/Yelp/paasta/pull/237) ([mjksmith](https://github.com/mjksmith))
- paasta status - show the value of schedule\_time\_zone \(PAASTA-2389\) [\#235](https://github.com/Yelp/paasta/pull/235) ([giuliano108](https://github.com/giuliano108))

## [v0.16.31](https://github.com/Yelp/paasta/tree/v0.16.31) (2016-02-05)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.16.30...v0.16.31)

**Closed issues:**

- update chronos\_schema.json to support dependent jobs [\#217](https://github.com/Yelp/paasta/issues/217)
- If container dies, fail healthcheck \(local-run\) [\#213](https://github.com/Yelp/paasta/issues/213)
- allow a string or array value for the 'parents' key of a dependent job [\#149](https://github.com/Yelp/paasta/issues/149)
- Show the status of a job's parents in paasta status [\#96](https://github.com/Yelp/paasta/issues/96)

**Merged pull requests:**

- Added experimental manual mesos task reconciliation script \(orphan killer\) [\#238](https://github.com/Yelp/paasta/pull/238) ([solarkennedy](https://github.com/solarkennedy))
- Add extra\_constraints, which doesn't override default constraints. [\#236](https://github.com/Yelp/paasta/pull/236) ([EvanKrall](https://github.com/EvanKrall))
- Upgrade mesos.interface to 0.23.1 [\#230](https://github.com/Yelp/paasta/pull/230) ([nhandler](https://github.com/nhandler))
- Fail fast when contained dies in local-run.  Resolves \#213 [\#229](https://github.com/Yelp/paasta/pull/229) ([asottile](https://github.com/asottile))
- Add jsonschema to setup.py [\#228](https://github.com/Yelp/paasta/pull/228) ([asottile](https://github.com/asottile))
- Add a new itest to assert new services have a desired\_state of start [\#227](https://github.com/Yelp/paasta/pull/227) ([nhandler](https://github.com/nhandler))
- Fix typo [\#226](https://github.com/Yelp/paasta/pull/226) ([asottile](https://github.com/asottile))
- Update chronos json schema \#217 [\#224](https://github.com/Yelp/paasta/pull/224) ([giuliano108](https://github.com/giuliano108))
- allow a string or array value for the 'parents' key of a dependent job [\#219](https://github.com/Yelp/paasta/pull/219) ([giuliano108](https://github.com/giuliano108))
- Added documentation for deploy groups [\#218](https://github.com/Yelp/paasta/pull/218) ([mjksmith](https://github.com/mjksmith))
- fsm now generates configs that use deploy groups [\#208](https://github.com/Yelp/paasta/pull/208) ([mjksmith](https://github.com/mjksmith))
- Show details of parents in output of ``paasta status`` [\#200](https://github.com/Yelp/paasta/pull/200) ([Rob-Johnson](https://github.com/Rob-Johnson))

## [v0.16.30](https://github.com/Yelp/paasta/tree/v0.16.30) (2016-02-03)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.16.29...v0.16.30)

## [v0.16.29](https://github.com/Yelp/paasta/tree/v0.16.29) (2016-02-03)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.16.28...v0.16.29)

**Merged pull requests:**

- Revert "Merge pull request \#190 from Yelp/mark\_for\_deployment-tags" [\#222](https://github.com/Yelp/paasta/pull/222) ([nhandler](https://github.com/nhandler))

## [v0.16.28](https://github.com/Yelp/paasta/tree/v0.16.28) (2016-02-02)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.16.27...v0.16.28)

**Merged pull requests:**

- Use json.dumps to get more consistent hash keys [\#221](https://github.com/Yelp/paasta/pull/221) ([solarkennedy](https://github.com/solarkennedy))

## [v0.16.27](https://github.com/Yelp/paasta/tree/v0.16.27) (2016-02-02)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.16.26...v0.16.27)

**Closed issues:**

- Stop Using Short GPG Key IDs [\#212](https://github.com/Yelp/paasta/issues/212)
- Cannot fetch index base URL http://pypi.yelpcorp.com/ [\#77](https://github.com/Yelp/paasta/issues/77)

**Merged pull requests:**

- Use full GPG fingerprints [\#215](https://github.com/Yelp/paasta/pull/215) ([nhandler](https://github.com/nhandler))
- Upgrade mesos to 0.23.1-0.2.61.ubuntu1404 [\#214](https://github.com/Yelp/paasta/pull/214) ([nhandler](https://github.com/nhandler))
- Catch the race between inspecting a marathon app that isn't running during a bounce. PAASTA-1721 [\#210](https://github.com/Yelp/paasta/pull/210) ([solarkennedy](https://github.com/solarkennedy))
- Gracefully handle killing a marathon task that is already dead. PAASTA-1219 [\#209](https://github.com/Yelp/paasta/pull/209) ([solarkennedy](https://github.com/solarkennedy))
- made paasta start/stop deploy-group aware [\#206](https://github.com/Yelp/paasta/pull/206) ([mjksmith](https://github.com/mjksmith))
- Inject PAASTA\_\* variables into every task [\#196](https://github.com/Yelp/paasta/pull/196) ([solarkennedy](https://github.com/solarkennedy))
- Add healthcheck\_mode to service config [\#194](https://github.com/Yelp/paasta/pull/194) ([fede1024](https://github.com/fede1024))
- Mark for deployment tags [\#190](https://github.com/Yelp/paasta/pull/190) ([nhandler](https://github.com/nhandler))

## [v0.16.26](https://github.com/Yelp/paasta/tree/v0.16.26) (2016-01-30)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.16.25...v0.16.26)

**Merged pull requests:**

- Fix regression from https://github.com/Yelp/paasta/pull/179 where we … [\#211](https://github.com/Yelp/paasta/pull/211) ([EvanKrall](https://github.com/EvanKrall))
- added deploy\_group to valid schemas for paasta check [\#207](https://github.com/Yelp/paasta/pull/207) ([mjksmith](https://github.com/mjksmith))

## [v0.16.25](https://github.com/Yelp/paasta/tree/v0.16.25) (2016-01-29)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.16.24...v0.16.25)

**Closed issues:**

- Figure out how to launch both a 'scheduled' job and 'dependent' job at the same time [\#147](https://github.com/Yelp/paasta/issues/147)

**Merged pull requests:**

- Have crossover bounce prefer killing unhealthy tasks. [\#179](https://github.com/Yelp/paasta/pull/179) ([EvanKrall](https://github.com/EvanKrall))

## [v0.16.24](https://github.com/Yelp/paasta/tree/v0.16.24) (2016-01-29)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.16.23...v0.16.24)

**Merged pull requests:**

- added support for old bugged refs/tags [\#205](https://github.com/Yelp/paasta/pull/205) ([mjksmith](https://github.com/mjksmith))
- make release no longer breaks PEP8 [\#204](https://github.com/Yelp/paasta/pull/204) ([mjksmith](https://github.com/mjksmith))
- Use acutal running tasks for marathon\_services\_running\_here\_works itest [\#203](https://github.com/Yelp/paasta/pull/203) ([solarkennedy](https://github.com/solarkennedy))

## [v0.16.23](https://github.com/Yelp/paasta/tree/v0.16.23) (2016-01-28)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.16.22...v0.16.23)

**Merged pull requests:**

- metastatus now prints an error when marathon is configured & unreachable [\#202](https://github.com/Yelp/paasta/pull/202) ([mjksmith](https://github.com/mjksmith))
- made cleanup\_chronos\_jobs send OK on sensu [\#191](https://github.com/Yelp/paasta/pull/191) ([mjksmith](https://github.com/mjksmith))
- Enabled deploy\_groups [\#177](https://github.com/Yelp/paasta/pull/177) ([mjksmith](https://github.com/mjksmith))

## [v0.16.22](https://github.com/Yelp/paasta/tree/v0.16.22) (2016-01-28)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.16.21...v0.16.22)

**Merged pull requests:**

- made cleanup\_chronos\_jobs remove all non-paasta jobs [\#201](https://github.com/Yelp/paasta/pull/201) ([mjksmith](https://github.com/mjksmith))
- skip dependent jobs if they are created before their parent [\#199](https://github.com/Yelp/paasta/pull/199) ([giuliano108](https://github.com/giuliano108))
- made cleanup\_marathon\_jobs send sensu OK events [\#198](https://github.com/Yelp/paasta/pull/198) ([mjksmith](https://github.com/mjksmith))

## [v0.16.21](https://github.com/Yelp/paasta/tree/v0.16.21) (2016-01-28)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.16.20...v0.16.21)

**Merged pull requests:**

- Actually install schemas [\#197](https://github.com/Yelp/paasta/pull/197) ([nhandler](https://github.com/nhandler))

## [v0.16.20](https://github.com/Yelp/paasta/tree/v0.16.20) (2016-01-27)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.16.19...v0.16.20)

**Merged pull requests:**

- Converts marathon's `started\_at` to the local tz [\#195](https://github.com/Yelp/paasta/pull/195) ([mjksmith](https://github.com/mjksmith))
- Remove tags chronos jobs [\#193](https://github.com/Yelp/paasta/pull/193) ([Rob-Johnson](https://github.com/Rob-Johnson))

## [v0.16.19](https://github.com/Yelp/paasta/tree/v0.16.19) (2016-01-27)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.16.18...v0.16.19)

**Closed issues:**

- Misleading error message with local-run -p [\#192](https://github.com/Yelp/paasta/issues/192)

## [v0.16.18](https://github.com/Yelp/paasta/tree/v0.16.18) (2016-01-27)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.16.17...v0.16.18)

**Closed issues:**

- Make 'paasta check' call 'paasta validate' [\#128](https://github.com/Yelp/paasta/issues/128)

**Merged pull requests:**

- Pick a smartstack port randomly, instead of always picking highest existing + 1. [\#189](https://github.com/Yelp/paasta/pull/189) ([EvanKrall](https://github.com/EvanKrall))
- Fixed bugs introduced in the generate deployments rebase [\#188](https://github.com/Yelp/paasta/pull/188) ([mjksmith](https://github.com/mjksmith))
- WIP: Implement 'pool': An attribute & constraint that lets you split a cluster [\#187](https://github.com/Yelp/paasta/pull/187) ([EvanKrall](https://github.com/EvanKrall))
- Make tox faster [\#186](https://github.com/Yelp/paasta/pull/186) ([EvanKrall](https://github.com/EvanKrall))
- added a HOST variable to docker containers [\#185](https://github.com/Yelp/paasta/pull/185) ([mjksmith](https://github.com/mjksmith))
- Make paasta check call validate \(Closes \#128\) [\#161](https://github.com/Yelp/paasta/pull/161) ([nhandler](https://github.com/nhandler))

## [v0.16.17](https://github.com/Yelp/paasta/tree/v0.16.17) (2016-01-25)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.16.16...v0.16.17)

**Closed issues:**

- Marathon 'bean is not valid' errors during crossover bounce cause itest failures [\#172](https://github.com/Yelp/paasta/issues/172)
- remove 'versioned' chronos jobs [\#158](https://github.com/Yelp/paasta/issues/158)

**Merged pull requests:**

- Install deps during manpages [\#183](https://github.com/Yelp/paasta/pull/183) ([asottile](https://github.com/asottile))
- chronos now gives sensu the full job id of a failed task [\#181](https://github.com/Yelp/paasta/pull/181) ([mjksmith](https://github.com/mjksmith))
- Catch the 'bean not found' error. Fixes \#172 [\#180](https://github.com/Yelp/paasta/pull/180) ([solarkennedy](https://github.com/solarkennedy))
- Add http://pre-commit.com hooks [\#175](https://github.com/Yelp/paasta/pull/175) ([asottile](https://github.com/asottile))

## [v0.16.16](https://github.com/Yelp/paasta/tree/v0.16.16) (2016-01-22)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.16.15...v0.16.16)

**Merged pull requests:**

- Fix the broken link [\#178](https://github.com/Yelp/paasta/pull/178) ([ronin13](https://github.com/ronin13))

## [v0.16.15](https://github.com/Yelp/paasta/tree/v0.16.15) (2016-01-22)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.16.14...v0.16.15)

**Closed issues:**

- deploy\_chronos\_services isn't shuffling [\#154](https://github.com/Yelp/paasta/issues/154)
- `paasta check` doesn't consider chronos-\*.yaml for deploy.yaml checks [\#110](https://github.com/Yelp/paasta/issues/110)
- Update paasta help [\#3](https://github.com/Yelp/paasta/issues/3)

**Merged pull requests:**

- Force python2.7 in other tox environments [\#176](https://github.com/Yelp/paasta/pull/176) ([asottile](https://github.com/asottile))
- fixed paasta\_metastatus -vv when there are no mesos slaves [\#174](https://github.com/Yelp/paasta/pull/174) ([mjksmith](https://github.com/mjksmith))
- Fixed formatting for very verbose paasta metastatus [\#173](https://github.com/Yelp/paasta/pull/173) ([mjksmith](https://github.com/mjksmith))
- Remove tags chronos jobs [\#171](https://github.com/Yelp/paasta/pull/171) ([Rob-Johnson](https://github.com/Rob-Johnson))
- Remove PYTHONPATH hax when running tests [\#170](https://github.com/Yelp/paasta/pull/170) ([asottile](https://github.com/asottile))
- Pick an ephemeral port better [\#169](https://github.com/Yelp/paasta/pull/169) ([asottile](https://github.com/asottile))
- made InstanceConfig take service, cluster and instance as args [\#167](https://github.com/Yelp/paasta/pull/167) ([mjksmith](https://github.com/mjksmith))
- Added contrib location and purge\_chronos\_jobs [\#166](https://github.com/Yelp/paasta/pull/166) ([solarkennedy](https://github.com/solarkennedy))
- Add --yelpsoa-config-root argument to 'paasta check' [\#165](https://github.com/Yelp/paasta/pull/165) ([nhandler](https://github.com/nhandler))
- Allow users to set accepted\_resource\_roles in marathon yamls. [\#164](https://github.com/Yelp/paasta/pull/164) ([EvanKrall](https://github.com/EvanKrall))
- add docs for schedule time zone field [\#163](https://github.com/Yelp/paasta/pull/163) ([Rob-Johnson](https://github.com/Rob-Johnson))
- Consider chronos\*.yaml for deploy checks \(Closes: \#110\) [\#162](https://github.com/Yelp/paasta/pull/162) ([nhandler](https://github.com/nhandler))
- Make deploy\_chronos\_services shuffle [\#160](https://github.com/Yelp/paasta/pull/160) ([nhandler](https://github.com/nhandler))
- Fixed a bug where non-smartstack services would fail to send alerts when they start failing [\#145](https://github.com/Yelp/paasta/pull/145) ([mjksmith](https://github.com/mjksmith))
- Added a script to drain and kill a marathon app as gracefully as possible [\#143](https://github.com/Yelp/paasta/pull/143) ([mjksmith](https://github.com/mjksmith))

## [v0.16.14](https://github.com/Yelp/paasta/tree/v0.16.14) (2016-01-14)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.16.13...v0.16.14)

**Closed issues:**

- setup\_chronos\_job does not clean up old jobs [\#146](https://github.com/Yelp/paasta/issues/146)
- write integration tests for launching a dependent job [\#141](https://github.com/Yelp/paasta/issues/141)
- lookup the latest full job for a 'service.instance' parent [\#140](https://github.com/Yelp/paasta/issues/140)
- theres a bogus help message in `check\_chronos\_jobs -h` [\#139](https://github.com/Yelp/paasta/issues/139)
- Update 'paasta validate' schemas to be more complete/accurate [\#129](https://github.com/Yelp/paasta/issues/129)
- paasta local-run doesn't handle chronos jobs correctly [\#124](https://github.com/Yelp/paasta/issues/124)

**Merged pull requests:**

- Run command inside sh c local run [\#159](https://github.com/Yelp/paasta/pull/159) ([nhandler](https://github.com/nhandler))
- Update itest to use chronos 2.4.0-0.1.20151007110204.ubuntu1404 [\#157](https://github.com/Yelp/paasta/pull/157) ([nhandler](https://github.com/nhandler))
- Extend schemas [\#156](https://github.com/Yelp/paasta/pull/156) ([nhandler](https://github.com/nhandler))
- remove assumptions from paasta validate tests [\#153](https://github.com/Yelp/paasta/pull/153) ([Rob-Johnson](https://github.com/Rob-Johnson))
- update the description of check\_chronos\_jobs [\#151](https://github.com/Yelp/paasta/pull/151) ([Rob-Johnson](https://github.com/Rob-Johnson))
- First support for creating dependent jobs - Fixes \#140, \#141, \#146 [\#150](https://github.com/Yelp/paasta/pull/150) ([Rob-Johnson](https://github.com/Rob-Johnson))
- fixed a testcase for utils.guess\_instance [\#144](https://github.com/Yelp/paasta/pull/144) ([mjksmith](https://github.com/mjksmith))
- Improve the human readable state of services depending on their type [\#133](https://github.com/Yelp/paasta/pull/133) ([solarkennedy](https://github.com/solarkennedy))

## [v0.16.13](https://github.com/Yelp/paasta/tree/v0.16.13) (2016-01-07)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.16.12...v0.16.13)

**Closed issues:**

- Make 'paasta validate' validate chronos files [\#127](https://github.com/Yelp/paasta/issues/127)
- Add 'paasta validate' itests [\#126](https://github.com/Yelp/paasta/issues/126)

**Merged pull requests:**

- Consolidate/tighten up filtering of marathon apps so status and bounc… [\#142](https://github.com/Yelp/paasta/pull/142) ([EvanKrall](https://github.com/EvanKrall))
- Fix local-run healthchecks when using custom soadir argument [\#138](https://github.com/Yelp/paasta/pull/138) ([keshavdv](https://github.com/keshavdv))
- Paasta validate itests [\#136](https://github.com/Yelp/paasta/pull/136) ([nhandler](https://github.com/nhandler))
- Validate chronos [\#134](https://github.com/Yelp/paasta/pull/134) ([nhandler](https://github.com/nhandler))

## [v0.16.12](https://github.com/Yelp/paasta/tree/v0.16.12) (2016-01-05)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.16.11...v0.16.12)

**Closed issues:**

- support a 'parents' field in Chronos job configuration. [\#94](https://github.com/Yelp/paasta/issues/94)
- If multiple marathon apps exist \(e.g `paasta restart` during a bounce\), crossover bounce might kill your only registered instances [\#20](https://github.com/Yelp/paasta/issues/20)

**Merged pull requests:**

- Added 0 principle [\#137](https://github.com/Yelp/paasta/pull/137) ([solarkennedy](https://github.com/solarkennedy))
- Added a --service argument to paasta check. Closes \#130 [\#132](https://github.com/Yelp/paasta/pull/132) ([solarkennedy](https://github.com/solarkennedy))
- Begin testing against marathon 11 [\#120](https://github.com/Yelp/paasta/pull/120) ([solarkennedy](https://github.com/solarkennedy))

## [v0.16.11](https://github.com/Yelp/paasta/tree/v0.16.11) (2015-12-14)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.16.10...v0.16.11)

**Closed issues:**

- paasta local-run --pull fails without --interactive [\#131](https://github.com/Yelp/paasta/issues/131)
- paasta check doesn't allow overriding of service name [\#130](https://github.com/Yelp/paasta/issues/130)

**Merged pull requests:**

- Lots of updates to help sections and therefore man pages [\#125](https://github.com/Yelp/paasta/pull/125) ([solarkennedy](https://github.com/solarkennedy))
- Fix some sys.stderr.writes that don't have their newlines [\#123](https://github.com/Yelp/paasta/pull/123) ([asottile](https://github.com/asottile))
- Add 'paasta validate' command [\#107](https://github.com/Yelp/paasta/pull/107) ([nhandler](https://github.com/nhandler))

## [v0.16.10](https://github.com/Yelp/paasta/tree/v0.16.10) (2015-12-10)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.16.9...v0.16.10)

**Merged pull requests:**

- Dependent jobs field [\#122](https://github.com/Yelp/paasta/pull/122) ([Rob-Johnson](https://github.com/Rob-Johnson))
- Nix paasta\_cli =\> to just cli [\#121](https://github.com/Yelp/paasta/pull/121) ([solarkennedy](https://github.com/solarkennedy))

## [v0.16.9](https://github.com/Yelp/paasta/tree/v0.16.9) (2015-12-10)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.16.8...v0.16.9)

**Merged pull requests:**

- Remove extra context from replication check output for performance reasons [\#117](https://github.com/Yelp/paasta/pull/117) ([solarkennedy](https://github.com/solarkennedy))

## [v0.16.8](https://github.com/Yelp/paasta/tree/v0.16.8) (2015-12-09)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.16.7...v0.16.8)

**Merged pull requests:**

- Fix InvalidJobNameError in marathon\_services\_running\_here [\#119](https://github.com/Yelp/paasta/pull/119) ([EvanKrall](https://github.com/EvanKrall))
- Use https internal pypi [\#118](https://github.com/Yelp/paasta/pull/118) ([asottile](https://github.com/asottile))
- Autodetect the instance in local-run [\#105](https://github.com/Yelp/paasta/pull/105) ([solarkennedy](https://github.com/solarkennedy))
- Make local-run stream the docker-pull with /dev/null as stdin [\#104](https://github.com/Yelp/paasta/pull/104) ([solarkennedy](https://github.com/solarkennedy))

## [v0.16.7](https://github.com/Yelp/paasta/tree/v0.16.7) (2015-12-08)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.16.6...v0.16.7)

## [v0.16.6](https://github.com/Yelp/paasta/tree/v0.16.6) (2015-12-08)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.16.5...v0.16.6)

**Merged pull requests:**

- Bump max\_consecutive\_failures to 30 \(5 minutes\) [\#101](https://github.com/Yelp/paasta/pull/101) ([solarkennedy](https://github.com/solarkennedy))

## [v0.16.5](https://github.com/Yelp/paasta/tree/v0.16.5) (2015-12-08)
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.16.4...v0.16.5)

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
[Full Changelog](https://github.com/Yelp/paasta/compare/v0.15.5...v0.15.6)

**Merged pull requests:**

- fail gracefully if you try and run paasta logs without scribe existing [\#66](https://github.com/Yelp/paasta/pull/66) ([Rob-Johnson](https://github.com/Rob-Johnson))
- Some refactoring related to the argument 'clusterinstance' [\#65](https://github.com/Yelp/paasta/pull/65) ([zeldinha](https://github.com/zeldinha))

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



\* *This Change Log was automatically generated by [github_changelog_generator](https://github.com/skywinder/Github-Changelog-Generator)*
