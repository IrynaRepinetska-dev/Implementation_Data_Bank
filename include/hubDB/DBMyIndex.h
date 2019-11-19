#ifndef HUBDB_DBMYINDEX_H
#define HUBDB_DBMYINDEX_H

#include <hubDB/DBIndex.h>

namespace HubDB {
    namespace Index {
        class DBMyIndex : public DBIndex {

        public:
            DBMyIndex(DBBufferMgr &bufferMgr,
                      DBFile &file,
                      enum AttrTypeEnum attrType,
                      ModType mode,
                      bool unique);

            ~DBMyIndex();

            string toString(string linePrefix = "") const;

            void initializeIndex();

            void find(const DBAttrType &val, DBListTID &tids);

            void insert(const DBAttrType &val, const TID &tid);

            void remove(const DBAttrType &val, const DBListTID &tid);

            static int registerClass();


        private:
            static LoggerPtr logger;
        };

    }
}


#endif //HUBDB_DBMYINDEX_H
