#ifndef DBSERVERSOCKET_H_
#define DBSERVERSOCKET_H_

#include <hubDB/DBSocket.h>

namespace HubDB{
	namespace Socket{
		class DBServerSocket : public DBSocket{
		public:
			DBServerSocket(const int socketFD);	
		    string toString(string linePrefix="") const;
			static DBServerSocket * createConnection(int port);
			DBServerSocket * acceptConnection();
		protected:

		private:
			static LoggerPtr logger;
		};
	}
}

#endif /*DBSERVERSOCKET_H_*/
