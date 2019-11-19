
%define api.pure full
%parse-param {yyscan_t scanner} {DBQueryMgr& queryMgr}
%lex-param {yyscan_t scanner}

%{
#include <hubDB/DBParserDefs.h>
#include <hubDB/DBParser.h>
#include <hubDB/DBLexer.h>
#include <hubDB/DBTypes.h>
#include <hubDB/DBException.h>
#include <hubDB/DBServer.h>

#include <hubDB/DBServerSocket.h>
using HubDB::Socket::DBServerSocket;
using namespace HubDB::Types;
using namespace HubDB::Exception;

static LoggerPtr loggerParser(Logger::getLogger("HubDB.Parser"));

YY_DECL ;

%}

%union{
  char * str;
  QualifiedName qName;
  int num;
  double realnum;
  bool flag;
  AttrDefStruct attrDef;
  RelDefStruct relDef;
  DBAttrType * valT;
  DBTuple * tupleT;
  DBListQualifiedName * listString;
  DBListPredicate * listPredicate;
  DBJoin * join;
};

%token CREATE DROP TABLE INSERT INTO VALUES SELECT DELETE FROM WHERE CONNECT
%token DISCONNECT EQUALS STAR PARA_OPEN PARA_CLOSE COMMATA SEMICOLON TO LIST
%token TABLES IMPORT EXPORT QUIT APOSTROPHE INDEX TYPE GET SCHEMA FOR DATABASE
%token INTT DOUBLET VARCHAR AS IN DOT UNIQUE ON AND JOIN PRINT STATISTICS CLASSES
%token HELP
%token <num>  NUM
%token <realnum> REALNUM
%token <str> DBNAME STR_VALUE

%type <str> strValue itype dbName
%type <qName> qName
%type <flag> unique
%type <attrDef> type
%type <relDef> attrDefs
%type <valT> vtype
%type <tupleT> value
%type <listString> attrList projection
%type <listPredicate> predicates where_clause
%type <join> join
%%

start: commands QUIT { YYACCEPT; }
     | QUIT { YYACCEPT; }
;

commands: commands full_command
        | full_command
        | error SEMICOLON {yyerrok; queryMgr.getSocket()->getWriteStream()<<endl; queryMgr.getSocket()->writeToSocket();}
;

full_command: command
	{
		queryMgr.getSocket()->getWriteStream()<<endl;
		queryMgr.getSocket()->writeToSocket();
		HubDB::Server::DBServer::checkAbortStatus();
	}
;

command: connect
     | disconnect
     | create_db
     | drop_db
     | list_tables
     | get_schema
     | create_table
     | drop_table
     | create_index
     | drop_index
     | insert
     | delete
     | select
     | import
     | export
     | statistics
     | classes
     | help
;

dbName: DBNAME
		{
			if(strlen($1)>MAX_STR_LEN){
				queryMgr.getSocket()->getWriteStream() << "string is too large" <<endl;
				YYERROR;
			}
            $$=$1;
            uint i=0;
            while($$[i]!='\0'){
                if(isalpha($$[i]))
                    $$[i] = toupper($$[i]);
                ++i;
            }
		}
;

qName: dbName DOT dbName
		{
			strcpy($$.relationName,$1);
			free($1);
			strcpy($$.attributeName,$3);
			free($3);
		}
;

strValue: APOSTROPHE STR_VALUE APOSTROPHE
		{
			$$=$2;
			if(strlen($$)>MAX_STR_LEN){
				queryMgr.getSocket()->getWriteStream() << "string is too large" <<endl;
				YYERROR;
			}
		}
;

create_db: CREATE DATABASE dbName SEMICOLON
	{
		LOG4CXX_DEBUG(loggerParser,(string)"create database " + $3 );
		queryMgr.createDB($3);
        free($3);
	}
;

drop_db: DROP DATABASE dbName SEMICOLON
	{
		LOG4CXX_DEBUG(loggerParser,(string)"drop database " + $3 );
		queryMgr.dropDB($3);
        free($3);
	}
;

connect: CONNECT TO dbName SEMICOLON
	{
		LOG4CXX_DEBUG(loggerParser,(string)"connect to " + $3 );
		queryMgr.connectTo($3);
        free($3);
	}
;

disconnect: DISCONNECT SEMICOLON
	{
		LOG4CXX_DEBUG(loggerParser,"disconnect");
		queryMgr.disconnect();
	}
;

list_tables: LIST TABLES SEMICOLON
    {
        LOG4CXX_DEBUG(loggerParser,"list tables " );
        queryMgr.listTables();
    }
;

get_schema: GET SCHEMA FOR TABLE dbName SEMICOLON
    {
        LOG4CXX_DEBUG(loggerParser,(string)"get schema for tables " + $5 );
        queryMgr.getSchemaForTable($5);
        free($5);
    }
;

create_table: CREATE TABLE dbName AS PARA_OPEN attrDefs PARA_CLOSE SEMICOLON
    {
	    strcpy($6.relationName,$3);
        LOG4CXX_DEBUG(loggerParser,(string)"create table\n"+TO_STR(&$6)); 
        queryMgr.createTable($6);
        free($3);
    }
;

attrDefs: attrDefs COMMATA dbName type unique
    {
        $$ = $1;
    	if($$.attrCnt == MAX_ATTR_PER_REL){
			queryMgr.getSocket()->getWriteStream() << "too many attributes" <<endl;
			YYERROR;
    	}
        $4.isUnique = $5;
        $4.isIndexed = false;
        $4.attrPos = $$.attrCnt;
		$4.indexType[0] = '\0';
        strcpy($4.attrName,$3);
        free($3);
        $$.attrList[$$.attrCnt++] = $4;
        $$.tupleSize += $4.attrLen;
    }
        | dbName type unique
    {
        $$.attrCnt = 0;
        $$.tupleSize = $2.attrLen;
        $2.isUnique = $3;
        $2.isIndexed = false;
        $2.attrPos = $$.attrCnt;
		$2.indexType[0] = '\0';
		strcpy($2.attrName,$1);
        free($1);
        $$.attrList[$$.attrCnt++] = $2;
    }
;

unique: { $$=false; }
	  | UNIQUE { $$=true;}
;

type: INTT { $$.attrType = INT; $$.attrLen = DBIntType::getSize(); }
	| DOUBLET { $$.attrType = DOUBLE; $$.attrLen = DBDoubleType::getSize();}
	| VARCHAR {  $$.attrType = VCHAR; $$.attrLen = DBVCharType::getSize(); }
;

drop_table: DROP TABLE dbName SEMICOLON
    {
        LOG4CXX_DEBUG(loggerParser,(string)"drop table " + $3);
        queryMgr.dropTable($3);
        free($3);
    }
;

create_index: CREATE INDEX qName itype SEMICOLON
    {
        LOG4CXX_DEBUG(loggerParser,(string)"create index " + $3.toString() + " TYPE " + $4 );
        queryMgr.createIndex($3,$4);
        free($4);
    }
;

itype: { $$=strdup(DEFAULT_IDXTYPE.c_str());}
     | TYPE strValue {$$=$2;}
;

drop_index: DROP INDEX qName SEMICOLON
    {
        LOG4CXX_DEBUG(loggerParser,(string)"drop index " + $3.toString());
        queryMgr.dropIndex($3);
    }
;

insert: INSERT INTO dbName VALUES PARA_OPEN value PARA_CLOSE SEMICOLON
    {
       LOG4CXX_DEBUG(loggerParser,(string)"insert into " + $3 + " value(" + $6->toString() + ")");
       queryMgr.insertInto($3,$6);
       free($3);
       delete $6;
    }
;

value: value COMMATA vtype
    {
        $$ = $1;
        $$->appendAttrVal($3);
    }
     | vtype
    {
        $$ = new DBTuple();
        $$->appendAttrVal($1);
    }
;

vtype: NUM
    {
        $$ = new DBIntType($1);
    }
     | REALNUM
    {
        $$ = new DBDoubleType($1);
    }
     | strValue
    {
        $$ = new DBVCharType($1);
        free($1);
    }
;

delete: DELETE FROM dbName where_clause SEMICOLON
    {
        LOG4CXX_DEBUG(loggerParser,(string)"delete from " + $3 + ($4!=NULL ? " where " + TO_STR(*$4) : ""));
        queryMgr.deleteFromTable($3,$4);
        free($3);
        if($4!=NULL)
            delete $4;
    }
;

where_clause:
    {
    	LOG4CXX_DEBUG(loggerParser,"where is null");
        $$ = NULL;
    }
			| WHERE predicates
    {
    	LOG4CXX_DEBUG(loggerParser,"where: " + TO_STR($2));    
    	$$ = $2;
    }
                
;

predicates: predicates AND qName EQUALS vtype
    {
        $$ = $1;
        DBPredicate p($5,$3);
        LOG4CXX_DEBUG(loggerParser,"Create Predicate: "+ p.toString())
        $$->push_back(p);
        LOG4CXX_DEBUG(loggerParser,"PredicateList is"+ TO_STR(*$$))
    }
		  | qName EQUALS vtype
    {
        $$ = new DBListPredicate();
        DBPredicate p($3,$1);
        LOG4CXX_DEBUG(loggerParser,"Create Predicate: "+ p.toString())
        $$->push_back(p);
        LOG4CXX_DEBUG(loggerParser,"PredicateList is "+ TO_STR($$)+ " "+ TO_STR(*$$))
    }
;

select: SELECT projection FROM dbName join where_clause SEMICOLON
    {
        LOG4CXX_DEBUG(loggerParser,"select " + ($2!=NULL ? TO_STR($2) : "*") + " from " + $4 + " " + ($5!=NULL ? $5->toString() : "") + " " + ($6!=NULL ? "where " + TO_STR(*$6) : ""));
        queryMgr.select($2,$4,$5,$6);
        free($4);
        if($2!=NULL)
            delete $2;
        if($5!=NULL)
            delete $5;
        if($6!=NULL)
            delete $6; 
    }
;

projection: STAR
    {
        $$ = NULL;
    }
		  | attrList
    {
        $$ = $1;
    }
;

attrList: attrList COMMATA qName
    {
        $$ = $1;
        $$->push_back($3);
    }
		| qName
    {
        $$ = new DBListQualifiedName;
        $$->push_back($1);
    }
;

join:
    {
        $$ = NULL;
    }
	| JOIN dbName ON qName EQUALS qName
    {
        $$ = new DBJoin;
		strcpy($$->relationName,$2);
        free($2);
        $$->attrNames[0] = $4;
        $$->attrNames[1] = $6;        
    }
;

import: IMPORT FROM strValue INTO dbName SEMICOLON
    {
        LOG4CXX_DEBUG(loggerParser,(string)"import from " + $3 + " into " + $5);
        queryMgr.importTab($3,$5);
        free($3);
        free($5);
    }
;

export: EXPORT dbName TO strValue SEMICOLON
    {
        LOG4CXX_DEBUG(loggerParser,(string)"export " + $2 + " to " + $4);
        queryMgr.exportTab($2,$4);
        free($2);
        free($4);
    }
;

statistics:	PRINT STATISTICS SEMICOLON
	{
		LOG4CXX_DEBUG(loggerParser,(string)"print statistics");
		if(DBMonitorMgr::getMonitorPtr() != NULL){
			DBMonitorMgr::getMonitorPtr()->printStatistic(queryMgr.getSocket()->getWriteStream());
		}else{
			queryMgr.getSocket()->getWriteStream() << "Monitoring is not enabled"<<endl;
		}
	}
;

classes: PRINT CLASSES SEMICOLON
    {
        LOG4CXX_DEBUG(loggerParser,(string)"print classes");
        queryMgr.getSocket()->getWriteStream() << "----------KnownClasses----------" <<endl;
        list<string> l = getKnownClassNames();
        uint cnt = l.size();
        while(l.empty()==false){
            queryMgr.getSocket()->getWriteStream() << l.front()  <<endl;
            l.pop_front();
        }
        queryMgr.getSocket()->getWriteStream() << "   " << cnt << " row(s) selected."<<endl;
    }
    ;
help: HELP SEMICOLON
    {
        LOG4CXX_DEBUG(loggerParser,(string)"help");
        queryMgr.getSocket()->getWriteStream() << "HELP:" <<endl;
        queryMgr.getSocket()->getWriteStream() << "\tCREATE DATABASE <dbname>" <<endl;
        queryMgr.getSocket()->getWriteStream() << "\tDROP DATABASE <dbname>" <<endl;
        queryMgr.getSocket()->getWriteStream() << "\tCONNECT TO <dbname>" <<endl;
        queryMgr.getSocket()->getWriteStream() << "\tDISCONNECT" <<endl;
        queryMgr.getSocket()->getWriteStream() << "\tLIST TABLES" <<endl;
        queryMgr.getSocket()->getWriteStream() << "\tGET SCHEMA FOR TABLE <tabname>" <<endl;
        queryMgr.getSocket()->getWriteStream() << "\tCREATE TABLE <tabname> AS ( <attrname> {INTEGER|DOUBLE|VARCHAR} [UNIQUE] {,<attrname> {INTEGER|DOUBLE|VARCHAR} [UNIQUE]}*)" <<endl;
        queryMgr.getSocket()->getWriteStream() << "\tDROP TABLE <tabname>" <<endl;
        queryMgr.getSocket()->getWriteStream() << "\tCREATE INDEX <tabname>.<attrname> [TYPE '<idxclassname>']" <<endl;
        queryMgr.getSocket()->getWriteStream() << "\tDROP INDEX <tabname>.<attrname>" <<endl;
        queryMgr.getSocket()->getWriteStream() << "\tINSERT INTO <tabname> VALUES (x,y,z,..)" <<endl;
        queryMgr.getSocket()->getWriteStream() << "\tDELETE FROM <tabname> [WHERE <tabname>.<attrname> = value {AND <tabname>.<attrname> = value}*]" <<endl;
        queryMgr.getSocket()->getWriteStream() << "\tSELECT {*|<tabname>.<attrname>{,<tabname>.<attrname>}*} FROM <tabname> [JOIN <tabname> ON <tabname>.<attrname> = <tabname>.<attrname>] [WHERE <tabname>.<attrname> = value {AND <tabname>.<attrname> = value}*]" <<endl;
        queryMgr.getSocket()->getWriteStream() << "\tIMPORT FROM '<filename>' INTO <tabname>" <<endl;
        queryMgr.getSocket()->getWriteStream() << "\tEXPORT <tabname> TO '<filename>'" <<endl;
        queryMgr.getSocket()->getWriteStream() << "\tPRINT STATISTICS" <<endl;
        queryMgr.getSocket()->getWriteStream() << "\tPRINT CLASSES" <<endl;
        queryMgr.getSocket()->getWriteStream() << "\tHELP" <<endl;
    }
;


%%

void extyyerror(const char *str,DBQueryMgr & queryMgr)
{
    LOG4CXX_INFO(loggerParser,str);
	queryMgr.getSocket()->getWriteStream() << str;
}

int yywrap(yyscan_t yyscanner)
{
	DBQueryMgr * queryMgr = (DBQueryMgr *)yyget_extra(yyscanner);
	DBServerSocket * socket = queryMgr->getSocket();
	if(socket->readFromSocket()>0){
		yy_scan_string(socket->getReadString().c_str(),yyscanner);
		return 0;
   	}
	return 1;
}

void parse(DBQueryMgr & queryMgr){
  	yyscan_t scanner;
	yylex_init ( &scanner );
	yyset_extra(&queryMgr,scanner);
	yy_scan_string("",scanner);
	yyparse(scanner,queryMgr);
	yylex_destroy( scanner );
}

DBParserException::DBParserException(const std::string& msg1)
     : DBException(msg1) {
}

DBParserException::DBParserException(const DBParserException& src)
      : DBException(src) {
}

DBParserException& DBParserException::operator=(const DBParserException& src) {
      DBException::operator=(src);
      return *this;
}

DBParserSyntaxException::DBParserSyntaxException(const std::string& msg1)
     : DBParserException(msg1) {
}

DBParserSyntaxException::DBParserSyntaxException(const DBParserSyntaxException& src)
      : DBParserException(src) {
}

DBParserSyntaxException& DBParserSyntaxException::operator=(const DBParserSyntaxException& src) {
      DBException::operator=(src);
      return *this;
}

DBParserQuitException::DBParserQuitException(const std::string& msg1)
     : DBParserException(msg1) {
}

DBParserQuitException::DBParserQuitException(const DBParserQuitException& src)
      : DBParserException(src) {
}

DBParserQuitException& DBParserQuitException::operator=(const DBParserQuitException& src) {
      DBException::operator=(src);
      return *this;
}

