mkdir .tmp
cd .tmp
echo "Cloning kind"
git clone git@github.com:keymone/kind.git
echo "Finished cloning kind"
cd kind
make build
cp bin/kind ../../
rm -rf .tmp
