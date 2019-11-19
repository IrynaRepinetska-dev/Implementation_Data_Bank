CLEAN_TARGETS = DBLexer.cpp DBParser.cpp $(INCLUDEDIR)/hubDB/DBParser.h $(INCLUDEDIR)/hubDB/DBLexer.h

LIB_TARGETS    = $(LIBDIR)/libhubDB.$(SHLIBSUF)

libhubDB_SRCS = DBTypes.cpp DBException.cpp DBMutex.cpp DBLock.cpp \
DBManager.cpp DBMonitorMgr.cpp \
DBFile.cpp DBFileBlock.cpp DBFileMgr.cpp \
DBBCB.cpp DBBACB.cpp DBBufferMgr.cpp DBRandomBufferMgr.cpp \
DBTable.cpp \
DBIndex.cpp DBSeqIndex.cpp \
DBSocket.cpp DBClientSocket.cpp DBServerSocket.cpp \
DBClient.cpp DBServer.cpp \
DBQueryMgr.cpp DBSimpleQueryMgr.cpp DBSysCatMgr.cpp \
DBLexer.cpp DBParser.cpp

include ../Makefile.common

DBLexer.cpp: DBLexer.lex DBParser.cpp
	@echo "Create DBLexer.cpp DBLexer.h from DBLexer.lex"
	$(FLEX) -R -o $*.cpp --header-file=$(INCLUDEDIR)/hubDB/$*.h $*.lex

DBParser.cpp: DBParser.y
	@echo "Create DBParser.cpp, hubDB/DBParser.h from DBParser.y"
	$(BISON) -d --file-prefix=$* $< && mv DBParser.tab.h $(INCLUDEDIR)/hubDB/DBParser.h && mv DBParser.tab.c DBParser.cpp
