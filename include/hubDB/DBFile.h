#ifndef DBFILE_H_
#define DBFILE_H_

#include <hubDB/DBTypes.h>
#include <hubDB/DBException.h>

using namespace HubDB::Types;

#define FILE_OPEN_FLAGS O_RDWR
//|O_DIRECT|O_SYNC
#define FILE_CREAT_FLAGS S_IWUSR|S_IRUSR
#define DIR_CREAT_FLAGS S_IXUSR|S_IWUSR|S_IRUSR

namespace HubDB{
    namespace Manager{
        class DBFileMgr;
    }
    namespace File{
        class DBFileBlock;
        class DBFile
        {
            friend class HubDB::Manager::DBFileMgr;
            friend class HubDB::File::DBFileBlock;
        public:
            DBFile();
            ~DBFile();

            string toString(string linePrefix="")const;

        protected:
            DBFile(const string & fileName);

            bool operator==(const DBFile & ref)const
            {return (fileNum == ref.fileNum) ? true : false;}

            bool operator==(const string & fName)const
            {return (fileName == fName) ? true : false;}

            FileNo getFileNum()const { return fileNum;};
            uint getBlockCnt()const { return blockCnt;};

            void setBlockCnt(uint blockCnt);

            static void create(const string & name,bool isDir);
            static void drop(const string & name,bool isDir);

        private:
            static LoggerPtr logger;
            string fileName;
            FileNo fileNum;
            uint blockCnt;
        };
    }
    namespace Exception{
        class DBFileException : public DBSystemException{
        public:
            DBFileException(const int num);
            DBFileException(const DBFileException&);
            DBFileException& operator=(const DBFileException&);
        };
    }
}

#endif // DBFILE_H_
