#
# export_vars.sh
#
# This shell script is used to export vars to the application using the
# APRUTIL library. This script should be "sourced" to ensure the variable
# values are set within the calling script's context. For example:
#
#   $ . path/to/apr-util/export_vars.sh
#

APRUTIL_EXPORT_INCLUDES="-I/home/iryna/Documents/WS19_20/ImplimentierungDatenBanken/HubDB_v1/HubDB_v1/tools/apr-util-1.2.12/xml/expat/lib -I/usr/local/include"
APRUTIL_EXPORT_LIBS="/home/iryna/Documents/WS19_20/ImplimentierungDatenBanken/HubDB_v1/HubDB_v1/tools/apr-util-1.2.12/xml/expat/lib/libexpat.la"
APRUTIL_LDFLAGS="-L/usr/local/lib"
