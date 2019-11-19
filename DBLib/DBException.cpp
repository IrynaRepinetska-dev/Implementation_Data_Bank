//#include <execinfo.h>
#include <signal.h>
#include <hubDB/DBException.h>

using namespace HubDB::Exception;

DBException::DBException(const std::string &msg1) {
  size_t msgLen = msg1.length();
  if (msgLen > MSG_SIZE) msgLen = MSG_SIZE;
  memcpy(this->msg, (char *) msg1.data(), msgLen);
  this->msg[msgLen] = 0;
  trace = createTrace();
}

DBException::DBException(const DBException &src) : std::exception() {
  strcpy(msg, src.msg);
  trace = src.trace;
}

DBException &DBException::operator=(const DBException &src) {
  strcpy(msg, src.msg);
  trace = src.trace;
  return *this;
}

const char *DBException::what() const throw() {
  return msg;
}

string DBException::getTrace() const throw() {
  return trace;
}

string DBException::createTrace() {
  stringstream ss;
/*
	void * array[30];
	int nSize = backtrace(array, 30);
	char ** symbols = backtrace_symbols(array, nSize);
    for (int i = 0; i < nSize; i++){
    	ss << symbols[i] << endl;
    }
    free(symbols);
*/
  return ss.str();
}


DBRuntimeException::DBRuntimeException(const std::string &msg1)
        : DBException(msg1) {
}

DBRuntimeException::DBRuntimeException(const DBRuntimeException &src)
        : DBException(src) {
}

DBRuntimeException &DBRuntimeException::operator=(const DBRuntimeException &src) {
  DBException::operator=(src);
  return *this;
}

DBNullPointerException::DBNullPointerException(const std::string &msg1)
        : DBRuntimeException(msg1) {
}

DBNullPointerException::DBNullPointerException(const DBNullPointerException &src)
        : DBRuntimeException(src) {
}

DBNullPointerException &DBNullPointerException::operator=(const DBNullPointerException &src) {
  DBRuntimeException::operator=(src);
  return *this;
}

DBOutOfBoundException::DBOutOfBoundException(const std::string &msg1)
        : DBRuntimeException(msg1) {
}

DBOutOfBoundException::DBOutOfBoundException(const DBOutOfBoundException &src)
        : DBRuntimeException(src) {
}

DBOutOfBoundException &DBOutOfBoundException::operator=(const DBOutOfBoundException &src) {
  DBRuntimeException::operator=(src);
  return *this;
}

DBIllegalArgumentException::DBIllegalArgumentException(const std::string &msg1)
        : DBRuntimeException(msg1) {
}

DBIllegalArgumentException::DBIllegalArgumentException(const DBIllegalArgumentException &src)
        : DBRuntimeException(src) {
}

DBIllegalArgumentException &DBIllegalArgumentException::operator=(const DBIllegalArgumentException &src) {
  DBRuntimeException::operator=(src);
  return *this;
}

DBIllegalCommandLineArgumentException::DBIllegalCommandLineArgumentException(const std::string &msg1)
        : DBRuntimeException(msg1) {
}

DBIllegalCommandLineArgumentException::DBIllegalCommandLineArgumentException(
        const DBIllegalCommandLineArgumentException &src)
        : DBRuntimeException(src) {
}

DBIllegalCommandLineArgumentException &
DBIllegalCommandLineArgumentException::operator=(const DBIllegalCommandLineArgumentException &src) {
  DBRuntimeException::operator=(src);
  return *this;
}

DBSystemException::DBSystemException(const int num)
        : DBException(strerror(num)),
          err(num) {
}

DBSystemException::DBSystemException(const DBSystemException &src)
        : DBException(src),
          err(src.err) {
}

DBSystemException &DBSystemException::operator=(const DBSystemException &src) {
  DBException::operator=(src);
  return *this;
}

DBClassForNameException::DBClassForNameException(const std::string &msg1)
        : DBRuntimeException(msg1) {
}

DBClassForNameException::DBClassForNameException(const DBClassForNameException &src)
        : DBRuntimeException(src) {
}

DBClassForNameException &DBClassForNameException::operator=(const DBClassForNameException &src) {
  DBException::operator=(src);
  return *this;
}
