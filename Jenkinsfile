@Library('jenkinsfile_stdlib') _

yproperties() // Sets releng approved global properties (SCM polling, build log rotation, etc)


CHANNELS = ['paasta']
GIT_SERVER = 'git@github.com'
PACKAGE_NAME = 'mirrors/Yelp/paasta'
DIST = ['xenial', 'bionic', 'jammy']

commit = ''

ircMsgResult(CHANNELS) {
    ystage('Test') {
        node {
            ensureCleanWorkspace {
                commit = clone(
                    PACKAGE_NAME,
                )['GIT_COMMIT']
                sh 'make itest'
            }
        }
    }

    // Runs `make itest_${version}` and attempts to upload to apt server if not an automatically timed run
    // This will automatically break all the steps into stages for you
    debItestUpload(
        repo: PACKAGE_NAME,
        versions: DIST,
        committish: commit,
    )

    ystage('Upload to PyPi') {
        node {
            promoteToPypi(
                "git@git.yelpcorp.com:mirrors/Yelp/paasta.git",
                commit,
            )
        }
    }
}
