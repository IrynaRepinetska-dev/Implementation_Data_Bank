#ifndef DBFILEMGR_H_
#define DBFILEMGR_H_

#include <hubDB/DBTypes.h>
#include <hubDB/DBException.h>
#include <hubDB/DBManager.h>
#include <hubDB/DBFile.h>
#include <hubDB/DBFileBlock.h>

using namespace HubDB::File;

namespace HubDB{
	namespace Manager{
		class DBFileMgr : public DBManager
		{
		public:
			DBFileMgr(bool doThreading):
				DBManager(doThreading){ if(logger!=NULL) LOG4CXX_INFO(logger,"DBFileMgr()");};
            ~DBFileMgr();
			string toString(string linePrefix="") const __attribute__ ((pure));

			void createFile(const string & name);
			void createDirectory(const string & name){DBFile::create(name,true);};

			void dropFile(const string & name);
			void dropDirectory(const string & name){DBFile::drop(name.c_str(),true);};
			
			DBFile & openFile(const string & name);
			DBFile * getOpenFile(const string & name);
			void closeFile(DBFile & file);

			void readFileBlock(DBFileBlock & block);
			void writeFileBlock(DBFileBlock & block);
			
			uint getBlockCnt(DBFile & file);
			void setBlockCnt(DBFile & file,uint cnt);

		private:  			
			list<DBFile *>::iterator find(const string & fileName);

		private:
  			static LoggerPtr logger;
  			list<DBFile *> fileList;
		};
    }
    namespace Exception{        
        class DBFileMgrException : public DBException{
        public:
            DBFileMgrException(const std::string& msg);
            DBFileMgrException(const DBFileMgrException&);
            DBFileMgrException& operator=(const DBFileMgrException&);
        }; 
	}
}
#endif // DBFILEMGR_H_
