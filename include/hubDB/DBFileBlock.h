#ifndef DBFILEBLOCK_H_
#define DBFILEBLOCK_H_

#include <hubDB/DBFile.h>

namespace HubDB{
    namespace Manager{
        class DBFileMgr;
    }
    namespace File {
        class DBFileBlock
        {
            friend class HubDB::Manager::DBFileMgr;
        public:
            DBFileBlock(DBFile & file,const BlockNo blockNum);
            string toString(string linePrefix="") const;
            BlockNo getBlockNo() const { return blockNum;};
            const char * getDataPtr()const { return data;};
            char * getDataPtr() { return data;};
            bool operator==(const DBFile&cfile)const { return file==cfile;};
            static uint getBlockSize(){ return blockSize; };
            FileNo getFileNum()const { return file.getFileNum();};
        protected:
            void readFileBlock();
            void writeFileBlock();
        private:
            DBFile & file;
            const BlockNo blockNum;
            char data[STD_BLOCKSIZE];
            static LoggerPtr logger;
            static const uint blockSize;
        };
    }
}

#endif // DBFILEBLOCK_H_

