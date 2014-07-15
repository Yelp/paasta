#!/bin/bash
#
# Promote a python package to the pypi repository if a version was tagged
#
# Suggested build parameters for the packages jenkins build:
#
# TARGET_GIT_REPO=$GIT_URL
# TARGET_GIT_SHA=$GIT_COMMIT
# TARGET_BUILD_TAG=$BUIlD_TAG
# PYPI_URL="git@git.yelpcorp.com:pypi_packages"
#

set -e


LOG=promote.log

SEP="==============================================================="

function gitCheckout() {
    local working_dir="working_repo"
    {
        git clone "$TARGET_GIT_REPO" "$working_dir" --no-checkout
        pushd "$working_dir"
        git checkout "$TARGET_GIT_SHA"
        popd "$working_dir"
    } >> $LOG
    # KWA: Our source is in the src dir
    echo "$working_dir/src/"
}

function verifyVersionTag() {
    local expected="$1"
    local version="$2"

    local remote_sha=$(git rev-parse refs/tags/${TAG_PREFIX}${version})
    if [ "$remote_sha" != "$expected" ]; then
        echo $SEP
        echo "Promote FAILED!"
        echo "Remote SHA for version \"$version\" is: $remote_sha"
        echo "Expected: $expected"
        echo $SEP
        exit 1
    fi
}

function verifyNewPackageFile() {
    local package="$1"

    if [ -f "$package" ]; then
        echo $SEP
        echo "File $package already exists."
        echo $SEP
        exit 0
    fi
}

function promote() {
# Build and promote this package to pypi.
    local name="$(python setup.py --name)"
    local version="$(python setup.py --version)"
    local package="$name-$version.tar.gz"
    local wheel="${name//-/_}-$version-py26-none-any.whl"

    activateVirtualEnv

    verifyVersionTag "$TARGET_GIT_SHA" "$version"
    git clone "$PYPI_URL" pypi_repo


    # We want both a source dist and a wheel for the time being
    python setup.py sdist
    promoteSingleDist "$TARGET_BUILD_TAG" "$package"
    python setup.py bdist_wheel
    promoteSingleDist "$TARGET_BUILD_TAG" "$wheel"


    deactivate
}

function activateVirtualEnv() {
# Prepare and activate a virtualenv with the necessary packages for generating wheels.
    virtualenv venv --system-site-packages
    source venv/bin/activate
    pip install "pip>=1.4" "setuptools>=0.9" "wheel>=0.21" -i http://pypi.yelpcorp.com/simple/ --use-wheel
}

function promoteSingleDist() {
# Take a sdist or wheel and add/commit/push it to the pypi repository.
    local build_tag="$1"
    local package="$2"

    verifyNewPackageFile "pypi_repo/$package"
    cp "dist/$package" pypi_repo/

    pushd pypi_repo > /dev/null
    git add "$package"
    git commit -m "Adding $package from $build_tag"
    git push origin master
    popd > /dev/null
}

function run() {
    if [ -z "$TARGET_GIT_REPO" ];  then echo "\$TARGET_GIT_REPO missing";  exit 2; fi
    if [ -z "$TARGET_GIT_SHA" ];   then echo "\$TARGET_GIT_SHA missing";   exit 3; fi
    if [ -z "$TARGET_BUILD_TAG" ]; then echo "\$TARGET_BUILD_TAG missing"; exit 4; fi
    if [ -z "$PYPI_URL" ];         then echo "\$PYPI_URL missing";         exit 5; fi

    if [ -z ${TAG_PREFIX+x} ];     then TAG_PREFIX="v"; fi

    # For jenkins because it can't pass in empty strings #59737
    if [ -n "$NO_TAG_PREFIX" ];    then TAG_PREFIX=""; fi

    local working_dir="$(gitCheckout)"
    pushd "$working_dir" > /dev/null

    promote

    popd "$working_dir" > /dev/null
}


# if __name__ == "__main__"
if [[ ${BASH_SOURCE[0]} == $0 ]]; then
    run
fi
