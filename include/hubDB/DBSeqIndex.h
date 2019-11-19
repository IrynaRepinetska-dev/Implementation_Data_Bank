#ifndef DBSEQINDEX_H_
#define DBSEQINDEX_H_

#include <hubDB/DBIndex.h>
#include <hubDB/DBTypes.h>

#include <memory>

namespace HubDB{
    namespace Index{

        struct IndexEntries {
            std::shared_ptr<DBAttrType> attr;   // raw Attribute-Data
            std::vector<TID> tidList; // Array of tids

            IndexEntries( std::shared_ptr<DBAttrType> attr, std::vector<TID> tids) :
              attr(attr), tidList(tids) {
            }

            IndexEntries( std::shared_ptr<DBAttrType> attr, TID tids) :
                    attr(attr) {
              tidList.push_back(tids);
            }
        };

        struct SequentialIndex {
            uint countEntries;      // number of entries / position of first free entry
            std::vector<IndexEntries> entries; // Array of Entries

            // File Layout:
            // Pointer auf erste freie Stelle | Value (DBAttrType) | TID-Liste (#tidsPerEntry)

            char* write(char* ptr, const AttrTypeEnum& attrType, uint tidsPerEntry) const {
              uint *count = (uint *) ptr;
              *count = countEntries;

              // Pointer, um eine Position vorrücken
              ptr += sizeof(uint);

              for (auto e : entries) {
                // Zuerst der Wert
                const char *ptrNext = 0;
                ptr = e.attr->write(ptr);

                // Die TID-Liste
                TID *tidList = (TID *) ptr;
                uint id = 0;
                for (auto tid : e.tidList) {
                  tidList[id] = tid;
                  id++;
                }
                // restlichen Positionen mit invalid füllen
                for (id; id < tidsPerEntry; id++) {
                  tidList[id] = invalidTID;
                }

                ptr += (sizeof(TID) * tidsPerEntry);
              }
            }

            static std::shared_ptr<SequentialIndex> read(const char* ptr, AttrTypeEnum& attrType, uint tidsPerEntry) {
              std::shared_ptr<SequentialIndex> s(new SequentialIndex());

              s->countEntries = *(uint*) ptr;
              ptr += sizeof(uint);

              for (uint i = 0; i < s->countEntries; i++) {
                std::shared_ptr<DBAttrType> attr(DBAttrType::read(ptr, attrType, &ptr)); // deleted by shared pointer

                TID *tidList = (TID *) ptr;
                std::vector<TID> tidVector;
                for (uint id = 0; id < tidsPerEntry; id++) {
                  if (tidList[id] == invalidTID) {
                    break;
                  }
                  else {
                    tidVector.push_back(tidList[id]);
                  }
                }

                s->entries.push_back(IndexEntries(attr, tidVector)); // moves temporary into vector
                ptr += sizeof(TID) * tidsPerEntry;
              }

              return s;
            }


        };

        class DBSeqIndex : public DBIndex{

        public:
            DBSeqIndex(DBBufferMgr & bufferMgr,
                       DBFile & file,
                       enum AttrTypeEnum attrType,
                       ModType mode,
                       bool unique);
            ~DBSeqIndex();
            string toString(string linePrefix="") const;

            void initializeIndex();
            void find(const DBAttrType & val,DBListTID & tids);
            void insert(const DBAttrType & val,const TID & tid);
            void remove(const DBAttrType & valToRemove, const DBListTID & tidsToRemove);

            static int registerClass();

        private:
            static const uint MAX_TID_PER_ENTRY;

            // Einträge (TIDs) pro Schlüsseleintrag
            // (maximal MAX_TID_PER_ENTRY or 1 when unique): tidsPerEntry
            const uint tidsPerEntry;
            bool findFirstPage(const DBAttrType & val,BlockNo & blockNo);
            uint entriesPerPage()const;

            bool isEmpty(const char * ptr);
            void insertEmptyPage(BlockNo blockNo);
            void removeEmptyPage(BlockNo blockNoToRemove);
            void insertInPage(const DBAttrType & valToInsert, const TID & tidToInsert, BlockNo blockNo);
            void splitPage(BlockNo &blockNo, uint &posToInsert, bool &unfix);
            void findFromPage(const DBAttrType & val,BlockNo pos,list<TID> & tidsFound);
            void removeFromPage(const DBAttrType & valToRemove, list<TID> tidsToRemove, BlockNo startBlockNo);
            void unfixBACBs(bool dirty);

            DBBACB& fixNonRootBlock(const BlockNo & blockNo, const DBBCBLockMode & mode);
            void unfixNonRootBlock(const DBBACB & dbbacb);

            void checkBacbStackInvariant();
            size_t getEntrySize() const;

            static LoggerPtr logger;
            static const BlockNo rootBlockNo;

            stack<DBBACB> bacbStack; // Der bacbStack speichert immer den Root-Block der Datei


        };
    }
}


#endif /*DBSEQINDEX_H_*/
