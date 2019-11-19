#!/bin/sh

MYPATH=`pwd`;


try() {
	"$@" || exit 1
}

LOCAL_LIB=""
if [ -e /usr/local/lib ] && [ -e /usr/local/include ] ; then
	LOCAL_LIB="-L/usr/local/lib"
	LLDFLAGS=LDFLAGS="$LOCAL_LIB"

	LOCAL_INC="-I/usr/local/include"
	LCPPFLAGS=CPPFLAGS="$LOCAL_INC"

	LICONV=--with-iconv=/usr/local
fi

set -x
if [ ! -d ../lib ] ; then
    mkdir -p ../lib
fi

if [ ! -d apr-1.2.12 ] ; then
    try tar -xzf apr-1.2.12.tar.gz
fi

cd apr-1.2.12
if [ ! -f configure ] ; then
    try ./buildconf
fi
if [ ! -f Makefile ] ; then
    try ./configure "$LLDFLAGS" "$LCPPFLAGS"
fi
try make
try cp -P ./.libs/*.dylib* ../../lib
cd ..

if [ ! -d apr-util-1.2.12 ] ; then
    try tar -xzf apr-util-1.2.12.tar.gz
fi

cd apr-util-1.2.12
if [ ! -f configure ] ; then
    try ./buildconf --with-apr=../apr-1.2.12
fi
if [ ! -f Makefile ] ; then
    try ./configure --with-apr=../apr-1.2.12 $LICONV
fi
try make
try cp -P ./.libs/*.dylib* ../../lib
cd ..

if [ ! -d apache-log4cxx-0.10.0 ] ; then
    try unzip apache-log4cxx-0.10.0.zip
    try patch -p0 < apache-log4cxx-0.10.0.patch
    try patch -p0 < apache-log4cxx-0.10.0-gcc-6.2.1.patch 
fi

try cd apache-log4cxx-0.10.0
if [ ! -f configure ] ; then 
    try ./autogen.sh
fi
if [ ! -f Makefile ] ; then
    try ./configure --with-apr=../apr-1.2.12 --with-apr-util=../apr-util-1.2.12 --disable-html-docs --disable-dot --disable-doxygen LDFLAGS="$LOCAL_LIB -L$MYPATH/../lib"
fi
try make

try cp -P src/main/cpp/.libs/*.dylib* ../../lib
rm -rf ../include/log4cxx
try cp -r src/main/include/log4cxx ../../include

cd ..

