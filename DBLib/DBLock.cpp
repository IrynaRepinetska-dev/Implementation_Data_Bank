#include <hubDB/DBLock.h>
#include <hubDB/DBException.h>

using namespace HubDB::Mutex;
using namespace HubDB::Types;
using namespace HubDB::Exception;

LoggerPtr DBLock::logger(Logger::getLogger("HubDB.Mutex.DBLock"));

DBLock::DBLock() :
        lockCnt(0) {
  if (logger != NULL) {
    LOG4CXX_INFO(logger, "DBLock()");
    LOG4CXX_DEBUG(logger, "pthread_cond_init");
  }
  int err = 0;
  if ((err = pthread_cond_init(&cond, NULL)) != 0) {
    if (logger != NULL) LOG4CXX_ERROR(logger, "pthread_cond_init()=" + TO_STR(err));
    throw DBMutexException(DBMutex::getMsg(err));
  }
}

DBLock::~DBLock() {
  if (logger != NULL) {
    LOG4CXX_INFO(logger, "~DBLock()");
    LOG4CXX_DEBUG(logger, "pthread_cond_destroy()");
  }
  int err = 0;
  if ((err = pthread_cond_destroy(&cond)) != 0) {
    if (logger != NULL)
      LOG4CXX_ERROR(logger, "pthread_cond_destroy()=" + TO_STR(err));
    throw DBMutexException(DBMutex::getMsg(err));
  }
}

string DBLock::toString(string linePrefix) const {
  stringstream ss;
  ss << linePrefix << "[DBLock]" << endl;
  ss << linePrefix << "lockCnt: " << lockCnt << endl;
  ss << linePrefix << "threadId:" << threadId << endl;
  ss << linePrefix << "mutex:" << endl;
  ss << mutex.toString(linePrefix + "\t") << endl;
  ss << linePrefix << "--------" << endl;
  return ss.str();
}

void DBLock::lock() {
  LOG4CXX_INFO(logger, "lock()");
  mutex.lock();
  if (lockCnt != 0 && pthread_equal(threadId, pthread_self()) == 0) {
    while (lockCnt != 0) {
      LOG4CXX_DEBUG(logger, "wait");
      pthread_cond_wait(&cond, mutex.getPthread_mutex());
    }
  }
  threadId = pthread_self();
  ++lockCnt;
  mutex.unlock();
}

void DBLock::unlock() {
  LOG4CXX_INFO(logger, "unlock()");
  mutex.lock();
  if (lockCnt != 0 && pthread_equal(threadId, pthread_self()) != 0) {
    --lockCnt;
  } else {
    if (lockCnt == 0) {
      LOG4CXX_WARN(logger, "lockCnt was already zero");
    } else {
      LOG4CXX_WARN(logger, "lock is not owned by this thread");
    }
  }
  if (lockCnt == 0)
    pthread_cond_signal(&cond);
  mutex.unlock();
}
