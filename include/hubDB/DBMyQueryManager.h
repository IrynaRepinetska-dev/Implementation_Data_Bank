#ifndef HUBDB_DBMYQUERYMANAGER_H
#define HUBDB_DBMYQUERYMANAGER_H

#include <hubDB/DBQueryMgr.h>

namespace HubDB {
    namespace Manager {
        class DBMyQueryManager : public DBQueryMgr {
        public:
            DBMyQueryManager(DBServerSocket & socket,DBSysCatMgr & sysCatMgr);
            ~DBMyQueryManager();
            string toString(string linePrefix="") const;

            void selectTuple(DBTable * table,DBListPredicate & where,DBListTuple & tuple);
            void selectJoinTuple(DBTable * table[2],uint attrJoinPos[2],DBListPredicate where[2],DBListJoinTuple & tuples);

            static int registerClass();

        private:
            static LoggerPtr logger;
        };
    }
}

#endif //HUBDB_DBMYQUERYMANAGER_H
