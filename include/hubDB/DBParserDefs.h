#ifndef DBPARSERDEFS_H_
#define DBPARSERDEFS_H_

#include <hubDB/DBQueryMgr.h>
#include <hubDB/DBException.h>

using namespace HubDB::Manager;

typedef void* yyscan_t;

#define yyerror(X,Y,Z) extyyerror(Z,queryMgr)
#define YY_DECL int yylex(YYSTYPE * myyylval,yyscan_t yyscanner)
#define YY_EXTRA_TYPE DBQueryMgr *

void extyyerror(const char *,DBQueryMgr & queryMgr);

namespace HubDB{
    namespace Exception{
class DBParserException : public DBException{
public:
    DBParserException(const std::string& msg);
    DBParserException(const DBParserException&);
    DBParserException& operator=(const DBParserException&);
}; // class DBParserException

class DBParserSyntaxException : public DBParserException{
public:
    DBParserSyntaxException(const std::string& msg);
    DBParserSyntaxException(const DBParserSyntaxException&);
    DBParserSyntaxException& operator=(const DBParserSyntaxException&);
}; // class DBSocketWriteException

class DBParserQuitException : public DBParserException{
public:
    DBParserQuitException(const std::string& msg);
    DBParserQuitException(const DBParserQuitException&);
    DBParserQuitException& operator=(const DBParserQuitException&);
}; // class DBSocketException

    }
}
#endif /*DBPARSERDEFS_H_*/
