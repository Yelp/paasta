#!/bin/bash
set -e

KIND_VERSION="${1:?Usage: install-kind.sh <version>}"

mkdir -p .tmp
cd .tmp
git -c advice.detachedHead=false clone -q --branch "$KIND_VERSION" --depth 1 https://github.com/kubernetes-sigs/kind.git 2>/dev/null
cd kind
make build --no-print-directory > /dev/null 2>&1
cp bin/kind ../../
cd ../..
rm -rf .tmp
echo "kind $KIND_VERSION built successfully"
