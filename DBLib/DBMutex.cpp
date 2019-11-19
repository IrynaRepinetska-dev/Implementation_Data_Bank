#include <hubDB/DBMutex.h>

using namespace HubDB::Types;
using namespace HubDB::Mutex;
using namespace HubDB::Exception;

LoggerPtr DBMutex::logger(Logger::getLogger("HubDB.Mutex.DBMutex"));

DBMutex::DBMutex() {
  if (logger != NULL) {
    LOG4CXX_INFO(logger, "DBMutex()");
    LOG4CXX_DEBUG(logger, "pthread_mutex_init()");
  }
  int err = 0;
  if ((err = pthread_mutex_init(&mutex, NULL)) != 0) {
    if (logger != NULL) LOG4CXX_ERROR(logger, "pthread_mutex_init()=" + TO_STR(err));
    throw DBMutexException(getMsg(err));
  }
}

string DBMutex::getMsg(const int err) {
  stringstream ss;
  switch (err) {
    case EBUSY:
      ss << "mutex is lock";
      break;
    case EINVAL:
      ss << "mutex is invalid";
      break;
    case EAGAIN:
      ss << "lacked of necessary resources";
      break;
    case ENOMEM:
      ss << "insufficient memory";
      break;
    case EPERM:
      ss << "no privileg";
      break;
    case EDEADLK:
      ss << "lock by another thread";
      break;
    default:
      ss << "unknown ErrorNo: " << err;
  }
  return ss.str();
}

DBMutex::~DBMutex() {
  if (logger != NULL) {
    LOG4CXX_INFO(logger, "~DBMutex()");
    LOG4CXX_DEBUG(logger, "pthread_mutex_destroy()");
  }
  int err = 0;
  if ((err = pthread_mutex_destroy(&mutex)) != 0) {
    if (logger != NULL)
      LOG4CXX_ERROR(logger, "pthread_mutex_destroy()=" + TO_STR(err));
    throw DBMutexException(getMsg(err));
  }
}

string DBMutex::toString(string linePrefix) const {
  stringstream ss;
  ss << linePrefix << "[DBMutex]" << endl;
  ss << linePrefix << "mutex: ";
  int rc = pthread_mutex_trylock(const_cast<pthread_mutex_t *>(&mutex));
  if (rc == 0) {
    ss << "unlocked" << endl;
    pthread_mutex_unlock(const_cast<pthread_mutex_t *>(&mutex));
  } else if (rc == EBUSY) {
    ss << "locked" << endl;
  } else {
    LOG4CXX_ERROR(logger, "pthread_mutex_trylock()=" + TO_STR(rc));
    throw DBMutexException(getMsg(rc));
  }
  ss << linePrefix << "---------" << endl;
  return ss.str();
}

void DBMutex::lock() const {
  LOG4CXX_INFO(logger, "lock()");
  LOG4CXX_DEBUG(logger, "pthread_mutex_lock()");
  int err;
  if ((err = pthread_mutex_lock(getPthread_mutex())) != 0) {
    LOG4CXX_ERROR(logger, "pthread_mutex_lock()=" + TO_STR(err));
    throw DBMutexException(getMsg(err));
  }
}

void DBMutex::unlock() const {
  LOG4CXX_INFO(logger, "unlock()");
  LOG4CXX_DEBUG(logger, "pthread_mutex_unlock()");
  int err;
  if ((err = pthread_mutex_unlock(getPthread_mutex())) != 0) {
    LOG4CXX_ERROR(logger, "pthread_mutex_unlock()=" + TO_STR(err));
    throw DBMutexException(getMsg(err));
  }
}

DBMutexException::DBMutexException(const std::string &msg1)
        : DBRuntimeException(msg1) {
}

DBMutexException::DBMutexException(const DBMutexException &src)
        : DBRuntimeException(src) {
}

DBMutexException &DBMutexException::operator=(const DBMutexException &src) {
  DBException::operator=(src);
  return *this;
}
