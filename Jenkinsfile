@Library('jenkinsfile_stdlib') _

env.PAASTA_ENV = 'YELP'

CHANNELS = ['paasta']
GIT_SERVER = 'git@github.com'
PACKAGE_NAME = 'mirrors/Yelp/paasta'
DIST = ['xenial', 'bionic', 'jammy']
BRANCH_REGEX = 'refs/tags/v[0-9.]+'

commit = ''

yproperties(
    branchRegex: BRANCH_REGEX
) // Sets releng approved global properties (SCM polling, build log rotation, etc)

ircMsgResult(CHANNELS) {
    ystage('Test') {
        def head_tag = sh(script: 'git describe --abbrev=0 --tags', returnStdout: true).trim()
        node {
            ensureCleanWorkspace {
                commit = clone(
                    PACKAGE_NAME,
                    repoBranch: head_tag
                )['GIT_COMMIT']
                sh 'make itest'
            }
        }
    }

    // // Runs `make itest_${version}` and attempts to upload to apt server if not an automatically timed run
    // // This will automatically break all the steps into stages for you
    // debItestUpload(
    //     repo: PACKAGE_NAME,
    //     versions: DIST,
    //     committish: commit,
    // )

    // ystage('Upload to PyPi') {
    //     node {
    //         promoteToPypi(
    //             "git@git.yelpcorp.com:mirrors/Yelp/paasta.git",
    //             commit,
    //         )
    //     }
    // }
}
