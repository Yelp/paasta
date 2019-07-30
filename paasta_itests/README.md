Iterating on itests
===

paasta_itests can take a while to run (up to 25 minutes!), so here are steps to
make the process faster.

1. `cd paasta_itests; docker-compose build; docker-compose up -d; docker-compose scale mesosslave=3`

Re-building the test environment for each run takes a while; let's just keep it up!

2. `docker-compose run paastatools`

Rather than letting tox run the itest container, run it yourself.

3. `tox -e paasta_itests_inside_container`

Once in the `paastatools` container, run the tests. The first run will take a while
(as it has to install the tox env), but if you don't exit the contaienr between
runs it will be faster in the future.

4. `tox -e paasta_itests_inside_container -- -i paasta_api`

Run only, for example, the `paasta_api` itests. Just be sure to run the full tests
once before pushing, or travis might be sad.

5. `tox -e paasta_itests_inside_container -- -n '"instance GET shows the marathon status of service.instance"'`

Run only, for example, the `instance GET shows the marathon status of service.instance`
scenario. Just be sure to run the full tests once before pushing, or travis
might be sad.
