@Library('jenkinsfile_stdlib') _

yproperties() // Sets releng approved global properties (SCM polling, build log rotation, etc)


CHANNELS = ['paasta']
GIT_SERVER = 'git@github.com'
PACKAGE_NAME = 'Yelp/paasta'
DIST = ['trusty', 'xenial', 'bionic']

ircMsgResult(CHANNELS) {
    ystage('Test') {
        node {
            clone(
                [
                    gitServer: GIT_SERVER,
                ],
                PACKAGE_NAME,
            )
            sh 'make itest'
        }
    }

    // Runs `make itest_${version}` and attempts to upload to apt server if not an automatically timed run
    // This will automatically break all the steps into stages for you
    debItestUpload(PACKAGE_NAME, DIST, gitServer=GIT_SERVER)

    ystage('Upload to PyPi') {
        node {
            promoteToPypi(
                "https://github.com/Yelp/paasta.git",
                commit,
            )
        }
    }
}
