#ifndef DBSOCKET_H_
#define DBSOCKET_H_

#include <hubDB/DBTypes.h>
#include <hubDB/DBException.h>

namespace HubDB{
	namespace Socket{
		class DBSocket
		{
		protected:
			DBSocket(int socketFD);
			DBSocket(const DBSocket & socket);
		public:
			DBSocket();
			virtual ~DBSocket();
		    virtual string toString(string linePrefix="") const;
			int getSocketFD() const;
			stringstream & getWriteStream();
			string getReadString() const ;
			int readFromSocket();
			void writeToSocket();
			
		private:
  			int socketFD;
  			static LoggerPtr logger;
			stringstream readStream;
			stringstream writeStream;
		};
		inline int DBSocket::getSocketFD() const{ return socketFD;};
		inline stringstream & DBSocket::getWriteStream(){return writeStream;};
		inline string DBSocket::getReadString()const {return readStream.str();};
    }
    namespace Exception{
        class DBSocketException : public DBException{
        public:
            DBSocketException(const std::string& msg);
            DBSocketException(const DBSocketException&);
            DBSocketException& operator=(const DBSocketException&);
        };

        class DBSocketWriteException : public DBSocketException{
        public:
            DBSocketWriteException(const std::string& msg);
            DBSocketWriteException(const DBSocketWriteException&);
            DBSocketWriteException& operator=(const DBSocketWriteException&);
        };

        class DBSocketReadException : public DBSocketException{
        public:
            DBSocketReadException(const std::string& msg);
            DBSocketReadException(const DBSocketReadException&);
            DBSocketReadException& operator=(const DBSocketReadException&);
        };
	}
}

#endif /*DBSOCKET_H_*/
