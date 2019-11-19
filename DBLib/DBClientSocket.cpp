#include <fcntl.h>
#include <netdb.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <sys/socket.h>
#include <netinet/in.h>

#include <hubDB/DBClientSocket.h>
#include <hubDB/DBException.h>

using namespace HubDB::Socket;

LoggerPtr DBClientSocket::logger(Logger::getLogger("HubDB.Socket.DBClientSocket"));

DBClientSocket::DBClientSocket(const int socketFD) :
        DBSocket(socketFD) {
  if (logger != NULL) {
    LOG4CXX_INFO(logger, "DBClientSocket()");
    LOG4CXX_DEBUG(logger, "socketFD: " + TO_STR(socketFD));
  }
}

string DBClientSocket::toString(string linePrefix) const {
  stringstream ss;
  ss << linePrefix << "[DBClientSocket]" << endl;
  ss << DBSocket::toString(linePrefix + "\t");
  ss << linePrefix << "----------------" << endl;
  return ss.str();
}

DBClientSocket *DBClientSocket::createConnection(int port, string hostName) {
  LOG4CXX_INFO(logger, "createConnection()");
  LOG4CXX_DEBUG(logger, "port: " + TO_STR(port));
  LOG4CXX_DEBUG(logger, "hostName: " + hostName);
  struct sockaddr_in serv_addr;
  struct hostent *host;

  memset((char *) &serv_addr, 0, sizeof(serv_addr));
  serv_addr.sin_family = AF_INET;      //Internet-addr

  LOG4CXX_INFO(logger, "Client-Connect to " + hostName + " on port " + TO_STR(port));
  serv_addr.sin_port = htons(port);    //Port

  if ((host = (struct hostent *) gethostbyname(hostName.c_str())) == NULL) {
    LOG4CXX_ERROR(logger, strerror(errno));
    throw HubDB::Exception::DBSystemException(errno);
  }

  //umwandlung von network byte addr string in network byte addr long int
  unsigned long *lptr = (unsigned long *) &host->h_addr_list[0][0];
  serv_addr.sin_addr.s_addr = *lptr;

  int socketFD = -1;
  if ((socketFD = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
    LOG4CXX_ERROR(logger, strerror(errno));
    throw HubDB::Exception::DBSystemException(errno);
  }

  if (connect(socketFD, (struct sockaddr *) &serv_addr, sizeof(serv_addr)) < 0) {
    LOG4CXX_ERROR(logger, strerror(errno));
    throw HubDB::Exception::DBSystemException(errno);
  }
  return new DBClientSocket(socketFD);
}
