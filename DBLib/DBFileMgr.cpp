#include <hubDB/DBFileMgr.h>
#include <hubDB/DBException.h>
#include <hubDB/DBMonitorMgr.h>

using namespace HubDB::Manager;
using namespace HubDB::Exception;

LoggerPtr DBFileMgr::logger(Logger::getLogger("HubDB.File.DBFileMgr"));

DBFileMgr::~DBFileMgr() {
  LOG4CXX_INFO(logger, "~DBFileMgr()");
  LOG4CXX_DEBUG(logger, "this:\n" + toString("\t"));
  while (fileList.begin() != fileList.end()) {
    DBFile *back = fileList.back();
    fileList.pop_back();
    delete back;
  }
}

string DBFileMgr::toString(string linePrefix) const {
  stringstream ss;
  lock();
  ss << linePrefix << "[DBFileMgr]" << endl;
  ss << DBManager::toString(linePrefix + "\t");
  ss << linePrefix << "fileList(size:" << fileList.size() << "):" << endl;
  list<DBFile *>::const_iterator i = fileList.begin();
  while (i != fileList.end()) {
    ss << (*i)->toString(linePrefix + "\t") << endl;
    ++i;
  }
  ss << linePrefix << "-----------" << endl;
  unlock();
  return ss.str();
}

void DBFileMgr::createFile(const string &fileName) {
  LOG4CXX_INFO(logger, "createFile()");
  LOG4CXX_DEBUG(logger, "fileName:" + fileName);
  LOG4CXX_DEBUG(logger, "this:\n" + toString("\t"));
  lock();
  list<DBFile *>::iterator i = find(fileName);

  try {
    if (i == fileList.end()) {
      DBFile::create(fileName, false);
    } else {
      throw DBFileMgrException("can not create file, file is open");
    }
  } catch (DBException e) {
    unlock();
    throw e;
  }
  unlock();
}

void DBFileMgr::dropFile(const string &fileName) {
  LOG4CXX_INFO(logger, "dropFile()");
  LOG4CXX_DEBUG(logger, "fileName:" + fileName);
  LOG4CXX_DEBUG(logger, "this:\n" + toString("\t"));

  lock();
  list<DBFile *>::iterator i = find(fileName);
  try {
    if (i == fileList.end())
      DBFile::drop(fileName.c_str(), false);
    else
      throw DBFileMgrException("can not drop file, file is open");
  } catch (DBException e) {
    unlock();
    throw e;
  }
  unlock();
}

DBFile &DBFileMgr::openFile(const string &fileName) {
  LOG4CXX_INFO(logger, "openFile()");
  LOG4CXX_DEBUG(logger, "fileName:" + fileName);
  LOG4CXX_DEBUG(logger, "this:\n" + toString("\t"));
  lock();
  list<DBFile *>::iterator i = find(fileName);
  if (i == fileList.end()) {
    try {
      DBFile *file = new DBFile(fileName);
      fileList.push_back(file);
      i = find(fileName);
    } catch (DBException e) {
      unlock();
      throw e;
    }
  }
  DBFile &rc = **i;
  LOG4CXX_DEBUG(logger, "rc:\n" + rc.toString());
  unlock();
  return rc;
}

DBFile *DBFileMgr::getOpenFile(const string &fileName) {
  LOG4CXX_INFO(logger, "getOpenFile()");
  LOG4CXX_DEBUG(logger, "fileName:" + fileName);
  LOG4CXX_DEBUG(logger, "this:\n" + toString("\t"));
  lock();
  list<DBFile *>::iterator i = find(fileName);
  DBFile *rc = NULL;
  if (i != fileList.end()) {
    rc = *i;
  }
  unlock();
  return rc;
}

void DBFileMgr::closeFile(DBFile &file) {
  LOG4CXX_INFO(logger, "closeFile()");
  LOG4CXX_DEBUG(logger, "file:\n" + file.toString("\t"));
  LOG4CXX_DEBUG(logger, "this:\n" + toString("\t"));
  lock();
  DBFile *f = &file;
  fileList.remove(f);
  delete f;
  unlock();
}

void DBFileMgr::readFileBlock(DBFileBlock &block) {
  LOG4CXX_INFO(logger, "readFileBlock()");
  LOG4CXX_DEBUG(logger, "block:\n" + block.toString("\t"));
  MONITOR_FUNC;
  lock();
  try {
    block.readFileBlock();
  } catch (DBException e) {
    unlock();
    throw e;
  }
  unlock();
}

void DBFileMgr::writeFileBlock(DBFileBlock &block) {
  LOG4CXX_INFO(logger, "writeFileBlock()");
  LOG4CXX_DEBUG(logger, "block:\n" + block.toString("\t"));
  MONITOR_FUNC;
  lock();
  try {
    block.writeFileBlock();
  } catch (DBException e) {
    unlock();
    throw e;
  }
  unlock();
}

list<DBFile *>::iterator DBFileMgr::find(const string &fileName) {
  LOG4CXX_INFO(logger, "find()");
  LOG4CXX_DEBUG(logger, "fileName: " + fileName);
  list<DBFile *>::iterator i = fileList.begin();
  while (i != fileList.end()) {
    if (**i == fileName) {
      break;
    }
    ++i;
  }
  return i;
}

uint DBFileMgr::getBlockCnt(DBFile &file) {
  LOG4CXX_INFO(logger, "getBlockCount()");
  LOG4CXX_DEBUG(logger, "file:\n" + file.toString("\t"));
  lock();
  uint rc = 0;
  try {
    rc = file.getBlockCnt();
  } catch (DBException e) {
    unlock();
    throw e;
  }
  unlock();
  return rc;
}

void DBFileMgr::setBlockCnt(DBFile &file, uint cnt) {
  LOG4CXX_INFO(logger, "setBlockCnt()");
  LOG4CXX_DEBUG(logger, "file:\n" + file.toString("\t"));
  LOG4CXX_DEBUG(logger, "cnt: " + TO_STR(cnt));
  lock();
  try {
    file.setBlockCnt(cnt);
  } catch (DBException e) {
    unlock();
    throw e;
  }
  unlock();
}

DBFileMgrException::DBFileMgrException(const std::string &msg1)
        : DBException(msg1) {
}

DBFileMgrException::DBFileMgrException(const DBFileMgrException &src)
        : DBException(src) {
}

DBFileMgrException &DBFileMgrException::operator=(const DBFileMgrException &src) {
  DBException::operator=(src);
  return *this;
}
