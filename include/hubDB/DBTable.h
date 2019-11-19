#ifndef DBTABLE_H_
#define DBTABLE_H_

#include <hubDB/DBBufferMgr.h>
#include <hubDB/DBFile.h>

using namespace HubDB::File;
using namespace HubDB::Manager;

namespace HubDB{
    namespace Table{

        class DBTable
        {
            struct tablePageLayout{
                BlockNo nextFreeBlock;
                uint slotUsedMask[1];
            };

        public:
            DBTable(DBBufferMgr & bufMgr,DBFile & file,const DBRelDef & rel,ModType mode);
            ~DBTable();
            string toString(string linePrefix="");

            static void initializeTable(DBBufferMgr & bufMgr,DBFile & file);

            void insert(DBTuple & tuple);

            void remove(const DBListTID & tidList);

            void readTIDs(const DBListTID & tids,DBListTuple & tupleList);

            TID readSeqFromTID(TID tid,
                               uint numOfTuples,
                               DBListTuple & tupleList);

            uint tuplesPerPage();
            uint getPageCnt() const { return bufMgr.getBlockCount(file); }

            void undo();

            const DBRelDef & getRelDef(){return rel;};

        protected:
            uint findFreeSlot(uint * slotMask,uint max);
            uint slotMaskCnt;
            size_t _sizeOfPage;
            void unfixBACBs(bool dirty);
            void assertUnique(DBTuple & tuple);
            void setSlot(uint * slotMask,uint pos);
            void unsetSlot(uint * slotMask,uint pos);
            bool isSlot(uint * slotMask,uint pos);
            size_t sizeOfPage(){ return _sizeOfPage;};

            static const BlockNo rootBlockNo;
            static LoggerPtr logger;
            DBBufferMgr & bufMgr;
            DBFile & file;
            const DBRelDef rel;
            stack<DBBACB> bacbStack;
            const ModType mode;
        };
    }
    namespace Exception{
        class DBTableException : public DBRuntimeException
        {
        public:
            DBTableException(const std::string& msg);
            DBTableException(const DBTableException&);
            DBTableException& operator=(const DBTableException&);
        };
    }
}

#endif // DBTABLE_H_
