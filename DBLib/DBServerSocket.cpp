#include <fcntl.h>
#include <netdb.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <sys/socket.h>
#include <netinet/in.h>

#include <hubDB/DBServerSocket.h>
#include <hubDB/DBException.h>

using namespace HubDB::Socket;

LoggerPtr DBServerSocket::logger(Logger::getLogger("HubDB.Socket.DBServerSocket"));

DBServerSocket::DBServerSocket(const int socketFD) :
        DBSocket(socketFD) {
  if (logger != NULL) {
    LOG4CXX_INFO(logger, "DBServerSocket()");
    LOG4CXX_DEBUG(logger, "socketFD: " + TO_STR(socketFD));
  }
}

string DBServerSocket::toString(string linePrefix) const {
  stringstream ss;
  ss << linePrefix << "[DBServerSocket]" << endl;
  ss << DBSocket::toString(linePrefix + "\t");
  ss << linePrefix << "----------------" << endl;
  return ss.str();
}

DBServerSocket *DBServerSocket::createConnection(int port) {
  LOG4CXX_INFO(logger, "createConnection()");
  LOG4CXX_DEBUG(logger, "port: " + TO_STR(port));
  struct sockaddr_in serv_addr;
  int socketFD = -1;

  if ((socketFD = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
    LOG4CXX_ERROR(logger, strerror(errno));
    throw HubDB::Exception::DBSystemException(errno);
  }

  memset((char *) &serv_addr, 0, sizeof(serv_addr));
  serv_addr.sin_family = AF_INET;      //Internet-addr

  serv_addr.sin_port = htons(port);    //Port
  serv_addr.sin_addr.s_addr = htonl(INADDR_ANY);  // alle annehmen
  LOG4CXX_INFO(logger, "SERVER-Listen on port " + TO_STR(port));

  int val = 1;
  setsockopt(socketFD, SOL_SOCKET, SO_REUSEADDR, &val, sizeof(int));

  if (bind(socketFD, (struct sockaddr *) &serv_addr, sizeof(serv_addr)) < 0) {
    LOG4CXX_ERROR(logger, strerror(errno));
    throw HubDB::Exception::DBSystemException(errno);
  }

  if (listen(socketFD, STD_LISTEN_CNT) < 0) {
    LOG4CXX_ERROR(logger, strerror(errno));
    throw HubDB::Exception::DBSystemException(errno);
  }
  return new DBServerSocket(socketFD);
}

DBServerSocket *DBServerSocket::acceptConnection() {
  LOG4CXX_INFO(logger, "acceptConnection()");
  struct sockaddr cli_addr;
  socklen_t clilen = sizeof(cli_addr);
  LOG4CXX_DEBUG(logger, "wait for new connection");
  int newsockfd = accept(getSocketFD(), (struct sockaddr *) &cli_addr, &clilen);
  if (newsockfd < 0 && errno == 4) {
    LOG4CXX_INFO(logger, "Retry after signal");
    newsockfd = accept(getSocketFD(), (struct sockaddr *) &cli_addr, &clilen);
  }
  if (newsockfd < 0) {
    LOG4CXX_ERROR(logger, strerror(errno));
    throw HubDB::Exception::DBSystemException(errno);
  }
  LOG4CXX_DEBUG(logger, "accepted connection: " + TO_STR(newsockfd));
  return new DBServerSocket(newsockfd);
}
