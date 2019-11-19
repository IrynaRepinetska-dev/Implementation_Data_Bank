#ifndef DBSERVER_H
#define DBSERVER_H

#include <hubDB/DBServerSocket.h>
#include <hubDB/DBMutex.h>
#include <hubDB/DBSysCatMgr.h>

using namespace HubDB::Manager;
using namespace HubDB::Socket;

/**
  * class DBServer
  */

namespace HubDB
{
	namespace Server{
		class DBServer
		{
			struct ThreadArg{
				DBSysCatMgr * sysCat;
				DBServerSocket * socket;
				string queryExec;
			};
		public:
			DBServer();
			~DBServer();
			string toString(string linePrefix="") const;
			static int run(const int argc,char * const argv []);
			static void catch_int(int sig_num);
			static bool getAbortStatus(){return abortRecived;};
			static void checkAbortStatus();
			
		protected:
			int execute(const int argc,char * const argv[]);
			void parseCommandArgs(const int argc,char * const argv []);
  	
  			void startListen();
  			static void * DBThreadFunc(void *arg);
  			static void * DBThreadFuncClose(void *arg);
  			static void startUserThreadProcessing(ThreadArg * arg);

		private:
  			static LoggerPtr logger;
  			static uint actThreadCnt;
  			static uint maxCntOfThreads;
  			static pthread_cond_t count_threshold_cv;
  			static DBMutex threadCntMutex;
            static bool abortRecived;
			static int port;
            DBServerSocket * socket;
            DBSysCatMgr * sysCatMgr;
			string bufferMgrName;
			string queryExecName;
			bool monitor;
			bool threading;
		};
	}
}

#endif // DBSERVER_H
