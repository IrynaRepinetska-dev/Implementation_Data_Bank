#ifndef DBSYSCATMGR_H_
#define DBSYSCATMGR_H_

#include <hubDB/DBManager.h>
#include <hubDB/DBMonitorMgr.h>
#include <hubDB/DBBufferMgr.h>
#include <hubDB/DBTypes.h>
#include <hubDB/DBIndex.h>
#include <hubDB/DBTable.h>

using namespace HubDB::Types;
using namespace HubDB::Manager;
using namespace HubDB::Index;
using namespace HubDB::Table;

namespace HubDB{
    namespace Manager{
        class DBSysCatMgr : public DBManager
        {
            struct sysCatInfoPage{
                bool isValid;
                uint blockSize;
            };

        public:
            DBSysCatMgr (bool doMonitor,string bufferMgrName,bool doThreading);
            ~DBSysCatMgr ( );
            string toString(string linePrefix="") const;

            void createDB(string dbName);
            void dropDB(string dbName);
            void disconnect(DBBACB * hdl);
            DBBACB * connectTo(string dbName);
            void listTables(string dbName,list<string> & tableNames);
            DBRelDef getSchemaForTable (const string dbName,string relationName );

            void createTable(const string dbName,const RelDefStruct & def);
            void dropTable(const string dbName,const string relName);
            DBTable * openTable(const string dbName,const string relName,ModType mode);

            void createIndex(string dbName,const QualifiedName & qname,string indexType);
            void dropIndex(string dbName,const QualifiedName & qname);
            DBIndex * openIndex(const string dbName,const QualifiedName & qname,ModType mode);

            bool doMonitor(){ return DBMonitorMgr::getMonitorPtr()!= NULL ? true : false;};

        protected:

            void initialize(DBFile & file);

            static LoggerPtr logger;
            static const BlockNo rootBlockNo;
            DBBufferMgr * bufMgr;
        };
    }
    namespace Exception{
        class DBSysCatMgrException : public DBRuntimeException
        {
        public:
            DBSysCatMgrException(const std::string& msg);
            DBSysCatMgrException(const DBSysCatMgrException&);
            DBSysCatMgrException& operator=(const DBSysCatMgrException&);
        };
    }
}

#endif // DBSYSCATMGR_H_
