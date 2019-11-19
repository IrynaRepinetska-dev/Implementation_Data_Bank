#include <hubDB/DBBCB.h>

using namespace HubDB::Exception;
using namespace HubDB::Buffer;

LoggerPtr DBBCB::logger(Logger::getLogger("HubDB.Buffer.DBBCB"));

DBBCB::DBBCB(DBFile &file, const BlockNo blockNum) :
        fileBlock(file, blockNum),
        modified(false),
        dirty(false),
        mode(LOCK_FREE) {
  if (logger != NULL) {
    LOG4CXX_INFO(logger, "DBBCB()");
    LOG4CXX_DEBUG(logger, "this:\n" + this->toString("\t"));
  }
}

DBBCB::~DBBCB() {
  if (logger != NULL) {
    LOG4CXX_INFO(logger, "~DBBCB()");
    LOG4CXX_DEBUG(logger, "this:\n" + this->toString("\t"));
  }
}

string DBBCB::LockMode2String(DBBCBLockMode mode) {
  string rc;
  switch (mode) {
    case LOCK_FREE:
      rc = "FREE";
      break;
    case LOCK_SHARED:
      rc = "SHARED";
      break;
    case LOCK_EXCLUSIVE:
      rc = "EXCLUSIVE";
      break;
    case LOCK_INTWRITE:
      rc = "INTENSION WRITE";
      break;
    default:
      throw DBBCBException("unknown lock mode");
  }
  return rc;
}

string DBBCB::toString(string linePrefix) const {
  stringstream ss;
  ss << linePrefix << "[DBBCB] " << this << endl;
  ss << linePrefix << "modified: " << TO_STR(modified) << endl;
  ss << linePrefix << "dirty: " << TO_STR(dirty) << endl;
  ss << linePrefix << "mode: " << LockMode2String(mode) << endl;
  ss << linePrefix << "threadIdsToMode: " << endl;
  map<pthread_t, DBBCBLockMode>::const_iterator i = threadIdsToMode.begin();
  while (i != threadIdsToMode.end()) {
    ss << linePrefix << "threadId: " << hex << (*i).first << dec << " Mode: " << (*i).second << endl;
    ++i;
  }
  ss << linePrefix << "fileBlock:" << endl << fileBlock.toString(linePrefix + "\t");
  ss << linePrefix << "-------" << endl;
  return ss.str();
}

bool DBBCB::upgradeLock() {
  LOG4CXX_INFO(logger, "upgradeLock()");
  LOG4CXX_DEBUG(logger, "mode: " + LockMode2String(getLockMode4Thread()));
  if (getLockMode4Thread() < LOCK_INTWRITE)
    throw DBBCBException("upgrade from wrong lock mode");
  return grantExclusiveAccess();
}

bool DBBCB::downgradeLock() {
  LOG4CXX_INFO(logger, "downgradeLock()");
  LOG4CXX_DEBUG(logger, "mode: " + LockMode2String(getLockMode4Thread()));
  map<pthread_t, DBBCBLockMode>::iterator i;
  i = threadIdsToMode.find(pthread_self());
  if (modified == true || dirty == true) {
    throw DBBCBException("downgrade on modified or dirty page");
  }
  if (i != threadIdsToMode.end()) {
    DBBCBLockMode m = (*i).second;
    threadIdsToMode.erase(i);
    LOG4CXX_DEBUG(logger, "m = " + LockMode2String(mode));
    switch (m) {
      case LOCK_EXCLUSIVE:
      case LOCK_INTWRITE:
      case LOCK_SHARED:
        mode = LOCK_SHARED;
        break;
      default:
        throw DBBCBException("downgrade from wrong lock mode");
    }
    (*i).second = LOCK_SHARED;
  } else {
    LOG4CXX_WARN(logger, "lock is not owned by this thread");
  }
  return true;
}

bool DBBCB::grantAccess(DBBCBLockMode m) {
  LOG4CXX_INFO(logger, "lock()");
  LOG4CXX_DEBUG(logger, "m: " + LockMode2String(m));
  LOG4CXX_DEBUG(logger, "this:\n" + toString("\t"));
  bool lockOkay = false;

  DBBCBLockMode req = getLockMode4Thread();

  if (m > req)
    req = m;

  LOG4CXX_DEBUG(logger, "req = " + LockMode2String(req));

  switch (req) {
    case LOCK_SHARED:
      if (mode != LOCK_EXCLUSIVE) {
        lockOkay = true;
        if (mode != LOCK_INTWRITE)
          mode = LOCK_SHARED;
      }
      break;
    case LOCK_INTWRITE:
      if (mode != LOCK_EXCLUSIVE &&
          (mode != LOCK_INTWRITE || getLockMode4Thread() == LOCK_INTWRITE)) {
        lockOkay = true;
        mode = LOCK_INTWRITE;
      }
      break;
    case LOCK_EXCLUSIVE:
      if (mode == LOCK_FREE ||
          (getLockMode4Thread() >= LOCK_INTWRITE && threadIdsToMode.size() == 1)) {
        lockOkay = true;
        mode = LOCK_EXCLUSIVE;
      }
      break;
    default:
      throw DBBCBException("unknown lock mode" + TO_STR(req));
  }

  LOG4CXX_DEBUG(logger, "lockOkay = " + TO_STR(lockOkay));

  if (lockOkay)
    threadIdsToMode[pthread_self()] = req;

  return lockOkay;
}

void DBBCB::unlock() {
  LOG4CXX_INFO(logger, "unlock()");
  LOG4CXX_DEBUG(logger, "this:\n" + toString("\t"));
  map<pthread_t, DBBCBLockMode>::iterator i;
  i = threadIdsToMode.find(pthread_self());
  if (i != threadIdsToMode.end()) {
    DBBCBLockMode m = (*i).second;
    threadIdsToMode.erase(i);
    LOG4CXX_DEBUG(logger, "m = " + LockMode2String(mode));
    switch (m) {
      case LOCK_INTWRITE:
        mode = LOCK_SHARED;
      default:
        if (threadIdsToMode.end() == threadIdsToMode.begin())
          mode = LOCK_FREE;
    }
  } else {
    LOG4CXX_WARN(logger, "lock is not owned by this thread");
  }
}

DBBCBLockMode DBBCB::getLockMode4Thread() const {
  LOG4CXX_INFO(logger, "getLockMode4Thread()");
  LOG4CXX_DEBUG(logger, "this:\n" + toString("\t"));
  DBBCBLockMode m = LOCK_FREE;
  map<pthread_t, DBBCBLockMode>::const_iterator i;
  i = threadIdsToMode.find(pthread_self());

  if (i != threadIdsToMode.end()) {
    m = (*i).second;
  }
  LOG4CXX_DEBUG(logger, "m = " + LockMode2String(mode));
  return m;
}

void DBBCB::setModified() {
  LOG4CXX_INFO(logger, "setModified()");
  if (LOCK_EXCLUSIVE != getLockMode4Thread())
    throw DBBCBException("try to set modified on read only bcb");
  modified = true;
};

void DBBCB::setDirty() {
  LOG4CXX_INFO(logger, "setDirty()");
  if (LOCK_EXCLUSIVE != getLockMode4Thread())
    throw DBBCBException("try to set dirty on read only bcb");
  if (getModified() == false)
    throw DBBCBException("try to set dirty on unmodified bcb");
  dirty = true;
};

DBBCBException::DBBCBException(const std::string &msg1)
        : DBException(msg1) {
}

DBBCBException::DBBCBException(const DBBCBException &src)
        : DBException(src) {
}

DBBCBException &DBBCBException::operator=(const DBBCBException &src) {
  DBException::operator=(src);
  return *this;
}
