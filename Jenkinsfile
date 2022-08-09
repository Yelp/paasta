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
        node {
            ensureCleanWorkspace {
                commit = clone(
                    repo: PACKAGE_NAME,
                    fetchTags: true
                )['GIT_COMMIT']
                def head_tag = sh(script: 'git describe --abbrev=0 --tags', returnStdout: true).trim()
                def commit_sha = sh(script: 'git rev-list -n 1 ${head_tag}', returnStdout: true).trim()
                sh "git checkout ${head_tag}"
                sh 'make itest'
            }
        }
    }

    // // Runs `make itest_${version}` and attempts to upload to apt server if not an automatically timed run
    // // This will automatically break all the steps into stages for you
    // debItestUpload(
    //     repo: PACKAGE_NAME,
    //     versions: DIST,
    //     committish: commit_sha,
    // )

    // ystage('Upload to PyPi') {
    //     node {
    //         promoteToPypi(
    //             "git@git.yelpcorp.com:mirrors/Yelp/paasta.git",
    //             commit_sha,
    //         )
    //     }
    // }
}
