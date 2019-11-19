#!/bin/bash

MYPATH=`pwd`;

if [ ! -d ../lib ] ; then
    mkdir -p ../lib
fi

if [ ! -d apache-log4cxx-0.10.0 ] ; then
    unzip apache-log4cxx-0.10.0.zip
    patch -p0 < apache-log4cxx-0.10.0.patch
fi

cd apache-log4cxx-0.10.0
if [ ! -f configure ] ; then 
    ./autogen.sh
fi
if [ ! -f Makefile ] ; then
    ./configure --with-apr=/usr --with-apr-util=/usr --disable-html-docs --disable-dot --disable-doxygen --disable-wchar_t LDFLAGS="-L$MYPATH/../lib"
fi
make

g++ -shared -o ./src/main/cpp/.libs/liblog4cxx.dll \
	-Wl,--out-implib=./src/main/cpp/.libs/liblog4cxx.dll.a \
	-Wl,--export-all-symbols \
	-Wl,--enable-auto-import \
	-Wl,--whole-archive ./src/main/cpp/.libs/liblog4cxx.a \
	-Wl,--no-whole-archive '-L/usr/lib -lxml2 -lz -lpthread -liconv -lm' \
	-lxml2 -lz -lpthread -liconv -lm -lapr-1 -laprutil-1

cp -P src/main/cpp/.libs/*.dll ../../lib
rm -rf ../include/log4cxx
cp -r src/main/include/log4cxx ../../include

cd ..
