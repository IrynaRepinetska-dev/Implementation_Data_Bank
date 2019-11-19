#!/bin/echo Please run this command instead: source 
MYPATH=`pwd`

LD_LIBRARY_PATH=$MYPATH/lib:$LD_LIBRARY_PATH
# Diese Variable wird beim Mac benutzt, um library-Verzeichnisse anzugeben,
# die von den Standardverzeichnissen abweichen
DYLD_FALLBACK_LIBRARY_PATH=$MYPATH/lib:$DYLD_FALLBACK_LIBRARY_PATH
PATH=$MYPATH:$MYPATH/bin:$MYPATH/lib:$PATH
export LD_LIBRARY_PATH PATH DYLD_FALLBACK_LIBRARY_PATH
