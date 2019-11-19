#ifndef DBCLIENT_H_
#define DBCLIENT_H_

#include <hubDB/DBTypes.h>
#include <hubDB/DBClientSocket.h>

using namespace HubDB::Types;
using namespace HubDB::Socket;

namespace HubDB
{
	namespace Client{
		class DBClient{
		public:
			DBClient();
			~DBClient();
			static int run(const int argc,char * const argv[]);
			string toString(string linePrefix="") const;
		protected:
  			int execute(const int argc,char * const argv[]);
			void parseCommandArgs(const int argc,char * const argv[]);
		private:
			DBClientSocket * socket;
			int port;
			string serverName;
  			static LoggerPtr logger;
		};
}}

#endif // DBCLIENT_H_
