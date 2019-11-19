cmake_minimum_required(VERSION 3.3.2)
project(HubDB)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")

include_directories( include/ tools/apr-1.2.12/include tools/apr-util-1.2.12/include )
link_directories( lib )

set(SOURCE_FILES
    DBLib/DBBACB.cpp
    DBLib/DBBCB.cpp
    DBLib/DBBufferMgr.cpp
    DBLib/DBClient.cpp
    DBLib/DBClientSocket.cpp
    DBLib/DBException.cpp
    DBLib/DBFile.cpp
    DBLib/DBFileBlock.cpp
    DBLib/DBFileMgr.cpp
    DBLib/DBIndex.cpp
    DBLib/DBLock.cpp
    DBLib/DBManager.cpp
    DBLib/DBMonitorMgr.cpp
    DBLib/DBMutex.cpp
    DBLib/DBQueryMgr.cpp
    DBLib/DBRandomBufferMgr.cpp
    DBLib/DBSeqIndex.cpp
    DBLib/DBServer.cpp
    DBLib/DBServerSocket.cpp
    DBLib/DBSimpleQueryMgr.cpp
    DBLib/DBSocket.cpp
    DBLib/DBSysCatMgr.cpp
    DBLib/DBTable.cpp
    DBLib/DBTypes.cpp
    include/hubDB/DBBACB.h
    include/hubDB/DBBCB.h
    include/hubDB/DBBufferMgr.h
    include/hubDB/DBClient.h
    include/hubDB/DBClientSocket.h
    include/hubDB/DBException.h
    include/hubDB/DBFile.h
    include/hubDB/DBFileBlock.h
    include/hubDB/DBFileMgr.h
    include/hubDB/DBIndex.h
    include/hubDB/DBLock.h
    include/hubDB/DBManager.h
    include/hubDB/DBMonitorMgr.h
    include/hubDB/DBMutex.h
    include/hubDB/DBParserDefs.h
    include/hubDB/DBQueryMgr.h
    include/hubDB/DBRandomBufferMgr.h
    include/hubDB/DBSeqIndex.h
    include/hubDB/DBServer.h
    include/hubDB/DBServerSocket.h
    include/hubDB/DBSimpleQueryMgr.h
    include/hubDB/DBSocket.h
    include/hubDB/DBSysCatMgr.h
    include/hubDB/DBTable.h
    include/hubDB/DBTypes.h

    DBLib/DBMyBufferMgr.cpp
    include/hubDB/DBMyBufferMgr.h
    DBLib/DBMyIndex.cpp
    include/hubDB/DBMyIndex.h
    DBLib/DBMyQueryManager.cpp
    include/hubDB/DBMyQueryManager.h)



################################################################################
# Flex and Bison
FIND_PACKAGE(FLEX REQUIRED)
SET(FlexOutput ${CMAKE_SOURCE_DIR}/DBLib/DBLexer.cpp)
SET(FlexHeader ${CMAKE_SOURCE_DIR}/include/hubDB/DBLexer.h)
IF(FLEX_FOUND)
    ADD_CUSTOM_COMMAND(
            OUTPUT ${FlexOutput}
            DEPENDS ${CMAKE_SOURCE_DIR}/DBLib/DBLexer.lex
            COMMAND ${FLEX_EXECUTABLE}
            --outfile=${FlexOutput}
            --header-file=${FlexHeader}
            ${CMAKE_SOURCE_DIR}/DBLib/DBLexer.lex
            COMMENT "Generating DBLexer.cpp"
    )
ENDIF()
FIND_PACKAGE(BISON REQUIRED)
SET( ParserSrc ${CMAKE_SOURCE_DIR}/DBLib/DBParser.cpp )
SET( ParserHeader ${CMAKE_SOURCE_DIR}/include/hubDB/DBParser.h )
IF(BISON_FOUND)
    message("-- bison found")
    ADD_CUSTOM_COMMAND(
            OUTPUT ${ParserSrc} ${ParserHeader}
            DEPENDS ${CMAKE_SOURCE_DIR}/DBLib/DBParser.y
            COMMAND ${BISON_EXECUTABLE}
            -g
            --defines=${ParserHeader}
            --output=${ParserSrc}
            ${CMAKE_SOURCE_DIR}/DBLib/DBParser.y
            COMMENT "Generating DBParser.cpp"
    )
ENDIF()

list( APPEND SOURCE_FILES ${FlexOutput} ${ParserSrc} ${ParserHeader} )

add_library(hubDB SHARED ${SOURCE_FILES})
target_link_libraries( hubDB apr-1 aprutil-1 log4cxx )

add_executable(hubDBClient DBApp/hubDBClient.cpp)
target_link_libraries(hubDBClient hubDB)

add_executable(hubDBServer DBApp/hubDBServer.cpp)
target_link_libraries(hubDBServer hubDB)

#add_executable(hubDBTestBufMgr DBApp/hubDBTestBufMgr.cpp)
#target_link_libraries(hubDBTestBufMgr hubDB)

#add_executable(hubDBTestIndex DBApp/hubDBTestIndex.cpp)
#target_link_libraries(hubDBTestIndex hubDB)

