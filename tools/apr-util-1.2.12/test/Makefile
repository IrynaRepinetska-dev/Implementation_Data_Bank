srcdir = .


# PROGRAMS includes all test programs built on this platform.
# STDTEST_PORTABLE
#   test programs invoked via standard user interface, run on all platforms
# ALL_TESTS
#   test modules invoked through the abts suite (./testall)
# OTHER_PROGRAMS
#   programs such as sockperf, that have to be invoked in a special sequence
#   or with special parameters

INCLUDES = -I/home/iryna/Documents/WS19_20/ImplimentierungDatenBanken/HubDB_v1/HubDB_v1/tools/apr-util-1.2.12/include -I/home/iryna/Documents/WS19_20/ImplimentierungDatenBanken/HubDB_v1/HubDB_v1/tools/apr-util-1.2.12/include/private  -I/home/iryna/Documents/WS19_20/ImplimentierungDatenBanken/HubDB_v1/HubDB_v1/tools/apr-1.2.12/include  -I/home/iryna/Documents/WS19_20/ImplimentierungDatenBanken/HubDB_v1/HubDB_v1/tools/apr-util-1.2.12/xml/expat/lib -I/usr/local/include

STDTEST_PORTABLE = testall dbd

TESTS = teststrmatch.lo testuri.lo testuuid.lo testbuckets.lo testpass.lo \
	testmd4.lo testmd5.lo testldap.lo testdate.lo testdbm.lo testdbd.lo \
	testxml.lo testrmm.lo testreslist.lo testqueue.lo testxlate.lo

OTHER_PROGRAMS = 

PROGRAMS = $(STDTEST_PORTABLE) $(OTHER_PROGRAMS)

TARGETS = $(PROGRAMS)

LOCAL_LIBS = ../libaprutil-${APRUTIL_MAJOR_VERSION}1.la

CLEAN_TARGETS = manyfile.bin testfile.txt data/sqlite*.db

# bring in rules.mk for standard functionality
include /home/iryna/Documents/WS19_20/ImplimentierungDatenBanken/HubDB_v1/HubDB_v1/tools/apr-util-1.2.12/build/rules.mk
APRUTIL_LIBS = /home/iryna/Documents/WS19_20/ImplimentierungDatenBanken/HubDB_v1/HubDB_v1/tools/apr-util-1.2.12/xml/expat/lib/libexpat.la /home/iryna/Documents/WS19_20/ImplimentierungDatenBanken/HubDB_v1/HubDB_v1/tools/apr-1.2.12/libapr-1.la -lrt -lcrypt -lpthread -ldl
APRUTIL_LDFLAGS = $(ALL_LDFLAGS) -no-install -L/usr/local/lib

# link programs using -no-install to get real executables not
# libtool wrapper scripts which link an executable when first run.
LINK_PROG = $(LIBTOOL) $(LTFLAGS) --mode=link $(LT_LDFLAGS) $(COMPILE) \
	    $(APRUTIL_LDFLAGS) -o $@

# STDTEST_PORTABLE;

abts.lo: $(srcdir)/abts.c $(srcdir)/abts.h $(srcdir)/abts_tests.h \
	 $(srcdir)/testutil.h

testutil.lo: $(srcdir)/abts.c $(srcdir)/abts.h $(srcdir)/abts_tests.h \
	     $(srcdir)/testutil.h

OBJECTS_testall = abts.lo testutil.lo $(TESTS) $(LOCAL_LIBS)
testall: $(OBJECTS_testall)
	$(LINK_PROG) $(OBJECTS_testall) $(APRUTIL_LIBS)
# For VPATH builds; where we have no ./data, copy us some data
# if we wait until 'make check', then 'make; ./testall' fails;
	if test ! -d "./data"; then cp -r $(srcdir)/data data; fi

OBJECTS_dbd = dbd.lo $(LOCAL_LIBS)
dbd: $(OBJECTS_dbd)
	$(LINK_PROG) $(OBJECTS_dbd) $(APRUTIL_LIBS)

check: $(TESTALL_COMPONENTS) $(STDTEST_PORTABLE) $(STDTEST_NONPORTABLE)
	for prog in $(STDTEST_PORTABLE) $(STDTEST_NONPORTABLE); do \
	        if test "$$prog" = 'dbd'; then \
			for driver in sqlite2 sqlite3; do \
				./$$prog $$driver; \
				if test $$? = 255; then \
					echo "$$prog $$driver failed"; \
					break; \
				fi; \
			done; \
	        else \
			./$$prog; \
			if test $$? = 255; then \
				echo "$$prog failed"; \
				break; \
			fi; \
		fi; \
	done

# DO NOT REMOVE
