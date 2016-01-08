cd /opt/libhdfs3

mkdir build
pushd build
../bootstrap --prefix=/usr/local
make
make install
popd
