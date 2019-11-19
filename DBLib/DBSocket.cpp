#include <errno.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <hubDB/DBSocket.h>
#include <hubDB/DBException.h>

/* Cygwin doesn't define the MSG_DONTWAIT flag.
 * Fixing this by using a null-flag instead.
 * It would be better, though, to write a non-blocking read procedure
 * instead.
 */
#ifdef __CYGWIN__
#include <sys/fcntl.h>
#define MSG_DONTWAIT O_NONBLOCK
#endif

using namespace HubDB::Socket;
using namespace HubDB::Exception;

LoggerPtr DBSocket::logger(Logger::getLogger("HubDB.Socket.DBSocket"));

DBSocket::DBSocket(const int socketFD) :
        socketFD(socketFD) {
  if (logger != NULL) {
    LOG4CXX_INFO(logger, "DBSocket()");
    LOG4CXX_DEBUG(logger, "socketFD: " + TO_STR(socketFD));
  }
  readStream.str("");
  writeStream.str("");

}

DBSocket::DBSocket(const DBSocket &socket) :
        socketFD(-1) {
  if (logger != NULL) {
    LOG4CXX_INFO(logger, "DBSocket()");
    LOG4CXX_DEBUG(logger, "socket:\n" + socket.toString("\t"));
  }
  readStream.str("");
  writeStream.str("");
  socketFD = dup(socket.getSocketFD());
  if (socketFD < 0) {
    LOG4CXX_ERROR(logger, strerror(errno));
    throw DBSystemException(errno);
  }
}

DBSocket::DBSocket() :
        socketFD(-1) {
  if (logger != NULL) {
    LOG4CXX_INFO(logger, "DBSocket()");
  }
  readStream.str("");
  writeStream.str("");
}

DBSocket::~DBSocket() {
  LOG4CXX_INFO(logger, "~DBSocket()");
  LOG4CXX_DEBUG(logger, "this:\n" + toString());
  if (socketFD != -1) {
    LOG4CXX_DEBUG(logger, "close(" + TO_STR(socketFD) + ")");
    shutdown(socketFD, SHUT_RDWR);
    if (close(socketFD) != 0) {
      LOG4CXX_ERROR(logger, strerror(errno));
      throw DBSystemException(errno);
    }
  }
}

string DBSocket::toString(string linePrefix) const {
  stringstream ss;
  ss << linePrefix << "[DBSocket]" << endl;
  ss << linePrefix << "socketFD: " << socketFD << endl;
  string str = readStream.str();
  int len = str.length();
  ss << linePrefix << "readString[" << len << "]:" << endl;
  if (len > 0) {
    ss << linePrefix << str << endl;
  }
  str = writeStream.str();
  len = str.length();
  ss << linePrefix << "writeString[" << len << "]:" << endl;
  if (len > 0) {
    ss << linePrefix << str << endl;
  }
  ss << linePrefix << "----------" << endl;
  return ss.str();
}

int DBSocket::readFromSocket() {
  LOG4CXX_INFO(logger, "readFromSocket()");

  size_t len, size, all = 0;
  len = 1024;
  char buffer[len + 1];

  memset(buffer, 0, len);
  readStream.str("");

  if ((size = recv(socketFD, buffer, len, 0)) < 0) {
    LOG4CXX_ERROR(logger, "recv()<0");
    throw HubDB::Exception::DBSocketReadException(strerror(errno));
  }
  buffer[size] = '\0';

  LOG4CXX_DEBUG(logger, "recv()=" + TO_STR(size));
  all += size;
  if (size > 0)
    readStream << buffer;

  while (size == len) {
    if ((size = recv(socketFD, buffer, len, MSG_DONTWAIT)) < 0) {
      LOG4CXX_ERROR(logger, "recv()<0");
      throw HubDB::Exception::DBSocketReadException(strerror(errno));
    }
    LOG4CXX_DEBUG(logger, "recv()=" + TO_STR(size));
    all += size;
    buffer[size] = '\0';
    if (size > 0)
      readStream << buffer;
  }
  LOG4CXX_DEBUG(logger, "all: " + TO_STR(all) + ":" + (all > 0 ? readStream.str() : ""));
  return all;
}

void DBSocket::writeToSocket() {
  LOG4CXX_INFO(logger, "writeToSocket()");

  size_t size, len;
  string data = writeStream.str();
  len = data.length();
  LOG4CXX_DEBUG(logger, "len: " + TO_STR(len) + ":" + writeStream.str());
  if ((size = send(socketFD, data.c_str(), len, 0)) < 0) {
    LOG4CXX_ERROR(logger, "send()<0");
    throw HubDB::Exception::DBSocketReadException(strerror(errno));
  } else if (size < len) {
    LOG4CXX_ERROR(logger, "Invalid socket data length: " + TO_STR(size) + "!=" + TO_STR(len));
    throw HubDB::Exception::DBSocketWriteException("Invalid socket data length: " + TO_STR(size) + "!=" + TO_STR(len));
  }
  writeStream.str("");
}

DBSocketException::DBSocketException(const std::string &msg1)
        : DBException(msg1) {
}

DBSocketException::DBSocketException(const DBSocketException &src)
        : DBException(src) {
}

DBSocketException &DBSocketException::operator=(const DBSocketException &src) {
  DBException::operator=(src);
  return *this;
}

DBSocketWriteException::DBSocketWriteException(const std::string &msg1)
        : DBSocketException(msg1) {
}

DBSocketWriteException::DBSocketWriteException(const DBSocketWriteException &src)
        : DBSocketException(src) {
}

DBSocketWriteException &DBSocketWriteException::operator=(const DBSocketWriteException &src) {
  DBException::operator=(src);
  return *this;
}

DBSocketReadException::DBSocketReadException(const std::string &msg1)
        : DBSocketException(msg1) {
}

DBSocketReadException::DBSocketReadException(const DBSocketReadException &src)
        : DBSocketException(src) {
}

DBSocketReadException &DBSocketReadException::operator=(const DBSocketReadException &src) {
  DBException::operator=(src);
  return *this;
}
