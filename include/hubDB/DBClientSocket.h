#ifndef DBCLIENTSOCKET_H_
#define DBCLIENTSOCKET_H_

#include <hubDB/DBSocket.h>

namespace HubDB{
    namespace Socket{
		class DBClientSocket : public DBSocket{
		public:
	    	DBClientSocket(const int socketFD);
	    	string toString(string linePrefix="") const;
			static DBClientSocket * createConnection(int port,string hostName);
		private:
	    	static LoggerPtr logger;
		};
	}
}

#endif /*DBCLIENTSOCKET_H_*/
