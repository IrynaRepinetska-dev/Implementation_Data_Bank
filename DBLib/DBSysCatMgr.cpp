#include <hubDB/DBSysCatMgr.h>
#include <hubDB/DBException.h>

using namespace HubDB::Manager;
using namespace HubDB::Exception;

LoggerPtr DBSysCatMgr::logger(Logger::getLogger("HubDB.SysCat.DBSysCatMgr"));
const BlockNo DBSysCatMgr::rootBlockNo(0);
const char SYSCAT_FILENAME[] = "syscat.db";
const char REL_END[] = ".db";
const char IDX_END[] = ".idx";
const char IDX_SEP = '_';

#define RELNAME(db, rel) db + FILE_SEP + rel + REL_END
#define IDXNAME(db, rel, attr) db + FILE_SEP + rel + IDX_SEP + attr + IDX_END
#define SYSCATNAME(db) db + FILE_SEP + SYSCAT_FILENAME

DBSysCatMgr::DBSysCatMgr(bool doMonitor, string bufferMgrName, bool doThreading) :
        DBManager(doThreading),
        bufMgr(NULL) {
  assert(sizeof(sysCatInfoPage) <= STD_BLOCKSIZE);
  assert(sizeof(RelDefStruct) <= STD_BLOCKSIZE);
  LOG4CXX_INFO(logger, "DBSysCatMgr()");
  LOG4CXX_DEBUG(logger, "create BufferMgr " + bufferMgrName);
  bufMgr = (DBBufferMgr *) getClassForName(bufferMgrName, 1, doThreading);
  if (doMonitor == true) {
    LOG4CXX_DEBUG(logger, "create MonitorMgr");
    DBMonitorMgr::setMonitorPtr(new DBMonitorMgr(doThreading));
  }
  LOG4CXX_DEBUG(logger, "this:\n" + toString("\t"));
}

DBSysCatMgr::~DBSysCatMgr() {
  LOG4CXX_INFO(logger, "~DBSysCatMgr()");
  if (bufMgr != NULL)
    delete bufMgr;
  if (DBMonitorMgr::getMonitorPtr() != NULL) {
    delete DBMonitorMgr::getMonitorPtr();
    DBMonitorMgr::setMonitorPtr(NULL);
  }
}

string DBSysCatMgr::toString(string linePrefix) const {
  stringstream ss;
  ss << linePrefix << "[DBSysCatMgr]" << endl;
  ss << DBManager::toString(linePrefix + "\t");
  ss << linePrefix << "bufMgr:\n" << bufMgr->toString(linePrefix + "\t");
  ss << linePrefix << "-----------" << endl;
  return ss.str();
}

void DBSysCatMgr::initialize(DBFile &file) {
  LOG4CXX_INFO(logger, "initialize()");
  LOG4CXX_DEBUG(logger, "file:\n" + file.toString("\t"));
  if (bufMgr->getBlockCount(file) != 0)
    throw DBSysCatMgrException("can not initializie exisiting syscat");

  stack<DBBACB> bacbStack;
  try {
    bacbStack.push(bufMgr->fixNewBlock(file));
    sysCatInfoPage *page = (sysCatInfoPage *) bacbStack.top().getDataPtr();
    page->blockSize = DBFileBlock::getBlockSize();
    page->isValid = true;
    bacbStack.top().setModified();
  } catch (DBException e) {
    if (bacbStack.empty() == false)
      bufMgr->unfixBlock(bacbStack.top());
    throw e;
  }
  bufMgr->unfixBlock(bacbStack.top());
  bacbStack.pop();
}

void DBSysCatMgr::createDB(string name) {
  LOG4CXX_INFO(logger, "createDB()");
  LOG4CXX_DEBUG(logger, "name: " + name);
  lock();
  try {
    string sysCatFile(SYSCATNAME(name));
    bufMgr->createDirectory(name);
    bufMgr->createFile(sysCatFile);
    DBFile &file = bufMgr->openFile(sysCatFile);
    initialize(file);
    bufMgr->closeFile(file);
  } catch (DBException &e) {
    unlock();
    throw e;
  }
  unlock();
}

void DBSysCatMgr::dropDB(string name) {
  LOG4CXX_INFO(logger, "dropDB()");
  LOG4CXX_DEBUG(logger, "name: " + name);
  lock();
  stack<DBBACB> bacbStack;
  try {
    string sysCatFile(SYSCATNAME(name));
    DBFile &file = bufMgr->openFile(sysCatFile);
    bacbStack.push(bufMgr->fixBlock(file, rootBlockNo, LOCK_EXCLUSIVE));
    sysCatInfoPage *page = (sysCatInfoPage *) bacbStack.top().getDataPtr();
    page->isValid = false;
    bacbStack.top().setModified();
    bufMgr->flushBlock(bacbStack.top());
    for (BlockNo b = 1; b < bufMgr->getBlockCount(file); ++b) {
      bacbStack.push(bufMgr->fixBlock(file, b, LOCK_EXCLUSIVE));
      DBRelDef def((const RelDefStruct *) bacbStack.top().getDataPtr());
      for (uint i = 0; i < def.attrCnt(); ++i) {
        DBAttrDef adef = def.attrDef(i);
        if (adef.isIndexed() == true) {
          bufMgr->dropFile(IDXNAME(name, def.relationName(), adef.attrName()));
        }
      }
      bufMgr->unfixBlock(bacbStack.top());
      bacbStack.pop();
      bufMgr->dropFile(RELNAME(name, def.relationName()));
    }
    bufMgr->unfixBlock(bacbStack.top());
    bacbStack.pop();
    bufMgr->dropFile(SYSCATNAME(name));
    bufMgr->dropDirectory(name);
  } catch (DBException e) {
    unlock();
    while (bacbStack.empty() == false) {
      bufMgr->unfixBlock(bacbStack.top());
      bacbStack.pop();
    }
    throw e;
  }
  unlock();
}

void DBSysCatMgr::disconnect(DBBACB *hdl) {
  LOG4CXX_INFO(logger, "disconnect()");
  LOG4CXX_DEBUG(logger, "hdl: " + TO_STR(hdl));
  lock();
  bufMgr->unfixBlock(*hdl);
  delete hdl;
  unlock();
}

DBBACB *DBSysCatMgr::connectTo(string name) {
  LOG4CXX_INFO(logger, "connectTo()");
  LOG4CXX_DEBUG(logger, "name: " + name);
  lock();
  DBBACB *rc = NULL;
  try {
    string sysCatFile(SYSCATNAME(name));
    DBFile &file = bufMgr->openFile(sysCatFile);
    rc = new DBBACB(bufMgr->fixBlock(file, rootBlockNo, LOCK_SHARED));
    sysCatInfoPage *page = (sysCatInfoPage *) rc->getDataPtr();
    if (page->isValid == false) {
      throw DBSysCatMgrException("DB is invalid");
    }
  } catch (DBException e) {
    if (rc != NULL) {
      bufMgr->unfixBlock(*rc);
      delete rc;
    }
    unlock();
    throw e;
  }
  unlock();
  return rc;
}

void DBSysCatMgr::listTables(string dbName, list<string> &tableNames) {
  LOG4CXX_INFO(logger, "listTables()");
  LOG4CXX_DEBUG(logger, "dbName: " + dbName);
  lock();
  try {
    string sysCatFile(SYSCATNAME(dbName));
    DBFile &file = bufMgr->openFile(sysCatFile);
    for (BlockNo b = 1; b < bufMgr->getBlockCount(file); ++b) {
      DBBACB bacb = bufMgr->fixBlock(file, b, LOCK_SHARED);
      DBRelDef def((const RelDefStruct *) bacb.getDataPtr());
      LOG4CXX_DEBUG(logger, (string) "relName: " + def.relationName());
      tableNames.push_back(def.relationName());
      bufMgr->unfixBlock(bacb);
    }
  } catch (DBException e) {
    unlock();
    throw e;
  }
  unlock();
}

DBRelDef DBSysCatMgr::getSchemaForTable(const string dbName, string relationName) {
  LOG4CXX_INFO(logger, "getSchemaForTable()");
  LOG4CXX_DEBUG(logger, "dbName: " + dbName);
  LOG4CXX_DEBUG(logger, "relationName: " + relationName);
  lock();
  try {
    string sysCatFile(SYSCATNAME(dbName));
    DBFile &file = bufMgr->openFile(sysCatFile);
    for (BlockNo b = 1; b < bufMgr->getBlockCount(file); ++b) {
      DBBACB bacb = bufMgr->fixBlock(file, b, LOCK_SHARED);
      DBRelDef def((const RelDefStruct *) bacb.getDataPtr());
      bufMgr->unfixBlock(bacb);
      if (relationName.compare(def.relationName()) == 0) { ;
        unlock();
        return def;
      }
    }
    throw DBSysCatMgrException("no such relation");
  } catch (DBException e) {
    unlock();
    throw e;
  }
  unlock();
}

void DBSysCatMgr::createTable(const string dbName, const RelDefStruct &def) {
  LOG4CXX_INFO(logger, "createTable()");
  LOG4CXX_DEBUG(logger, "dbName: " + dbName);
  lock();
  try {
    string sysCatFile(SYSCATNAME(dbName));
    string relFile(RELNAME(dbName, def.relationName));
    bufMgr->createFile(relFile);
    DBFile &rfile = bufMgr->openFile(relFile);
    DBTable::initializeTable(*bufMgr, rfile);
    DBFile &file = bufMgr->openFile(sysCatFile);
    DBBACB bacb = bufMgr->fixNewBlock(file);
    memcpy(bacb.getDataPtr(), &def, sizeof(def));
    bacb.setModified();
    bufMgr->flushBlock(bacb);
    bufMgr->unfixBlock(bacb);
  } catch (DBException e) {
    unlock();
    throw e;
  }
  unlock();
}

void DBSysCatMgr::dropTable(const string dbName, const string relName) {
  LOG4CXX_INFO(logger, "dropTable()");
  LOG4CXX_DEBUG(logger, "dbName: " + dbName);
  LOG4CXX_DEBUG(logger, "relName: " + relName);
  lock();
  stack<DBBACB> bacbStack;
  bool found = false;
  try {
    string sysCatFile(SYSCATNAME(dbName));
    DBFile &file = bufMgr->openFile(sysCatFile);
    for (BlockNo b = 1; found == false && b < bufMgr->getBlockCount(file); ++b) {
      bacbStack.push(bufMgr->fixBlock(file, b, LOCK_INTWRITE));
      DBRelDef def((const RelDefStruct *) bacbStack.top().getDataPtr());
      if (relName.compare(def.relationName()) == 0) { ;
        found = true;
      } else {
        bufMgr->unfixBlock(bacbStack.top());
        bacbStack.pop();
      }
    }
    if (found == false) {
      assert(bacbStack.empty() == true);
      throw DBSysCatMgrException("no such relation");
    }
    DBRelDef def((const RelDefStruct *) bacbStack.top().getDataPtr());
    for (uint i = 0; i < def.attrCnt(); ++i) {
      DBAttrDef adef = def.attrDef(i);
      if (adef.isIndexed() == true) {
        bufMgr->dropFile(IDXNAME(dbName, def.relationName(), adef.attrName()));
      }
    }
    bufMgr->dropFile(RELNAME(dbName, def.relationName()));
    uint blockCnt = bufMgr->getBlockCount(file);
    if ((blockCnt - 1) > bacbStack.top().getBlockNo()) {
      bufMgr->upgradeToExclusive(bacbStack.top());
      char *ptr = bacbStack.top().getDataPtr();
      bacbStack.push(bufMgr->fixBlock(file, blockCnt - 1, LOCK_SHARED));
      memcpy(ptr, bacbStack.top().getDataPtr(), DBFileBlock::getBlockSize());
      bufMgr->unfixBlock(bacbStack.top());
      bacbStack.pop();
      bacbStack.top().setModified();
      bufMgr->flushBlock(bacbStack.top());
    }
    bufMgr->unfixBlock(bacbStack.top());
    bacbStack.pop();
    bufMgr->setBlockCnt(file, blockCnt - 1);
  } catch (DBException e) {
    unlock();
    throw e;
  }
  unlock();
}

DBTable *DBSysCatMgr::openTable(const string dbName, const string relName, ModType mode) {
  LOG4CXX_INFO(logger, "openTable()");
  LOG4CXX_DEBUG(logger, "dbName: " + dbName);
  LOG4CXX_DEBUG(logger, "relName: " + relName);
  LOG4CXX_DEBUG(logger, "mode: " + TO_STR(mode));
  DBTable *table = NULL;
  lock();
  try {
    string relFile(RELNAME(dbName, relName));
    DBFile &file = bufMgr->openFile(relFile);
    table = new DBTable(*bufMgr, file, getSchemaForTable(dbName, relName), mode);
  } catch (DBException e) {
    unlock();
    throw e;
  }
  unlock();
  return table;
}

void DBSysCatMgr::createIndex(string dbName, const QualifiedName &qname, string indexType) {
  LOG4CXX_INFO(logger, "createIndex()");
  LOG4CXX_DEBUG(logger, "dbName: " + dbName);
  LOG4CXX_DEBUG(logger, "qname: " + qname.toString());
  LOG4CXX_DEBUG(logger, "indexType: " + indexType);
  lock();
  stack<DBBACB> bacbStack;
  bool found = false;
  try {
    string sysCatFile(SYSCATNAME(dbName));
    string idxFile(IDXNAME(dbName, qname.relationName, qname.attributeName));
    DBFile &file = bufMgr->openFile(sysCatFile);
    for (BlockNo b = 1; found == false && b < bufMgr->getBlockCount(file); ++b) {
      bacbStack.push(bufMgr->fixBlock(file, b, LOCK_INTWRITE));
      DBRelDef def((const RelDefStruct *) bacbStack.top().getDataPtr());
      if (strcmp(qname.relationName, def.relationName().c_str()) == 0) { ;
        found = true;
      } else {
        bufMgr->unfixBlock(bacbStack.top());
        bacbStack.pop();
      }
    }
    if (found == false)
      throw DBSysCatMgrException("no such relation");

    RelDefStruct *relDef = (RelDefStruct *) bacbStack.top().getDataPtr();
    found = false;
    uint pos = 0;
    for (uint i = 0; found == false && i < relDef->attrCnt; ++i) {
      if (strcmp(relDef->attrList[i].attrName, qname.attributeName) == 0) {
        found = true;
        pos = i;
      }
    }

    if (found == false)
      throw DBSysCatMgrException("no such attribute");

    if (relDef->attrList[pos].isIndexed == true)
      throw DBSysCatMgrException("already indexed");

    bufMgr->createFile(idxFile);

    bufMgr->upgradeToExclusive(bacbStack.top());
    relDef->attrList[pos].isIndexed = true;
    strcpy(relDef->attrList[pos].indexType, indexType.c_str());
    bacbStack.top().setModified();
    bufMgr->unfixBlock(bacbStack.top());
    bacbStack.pop();

  } catch (DBException e) {
    while (bacbStack.empty() == false) {
      bufMgr->unfixBlock(bacbStack.top());
      bacbStack.pop();
    }
    unlock();
    throw e;
  }
  unlock();
}

void DBSysCatMgr::dropIndex(string dbName, const QualifiedName &qname) {
  LOG4CXX_INFO(logger, "dropIndex()");
  LOG4CXX_DEBUG(logger, "dbName: " + dbName);
  LOG4CXX_DEBUG(logger, "qname: " + qname.toString());

  lock();
  stack<DBBACB> bacbStack;
  bool found = false;
  try {
    string sysCatFile(SYSCATNAME(dbName));
    DBFile &file = bufMgr->openFile(sysCatFile);
    for (BlockNo b = 1; found == false && b < bufMgr->getBlockCount(file); ++b) {
      bacbStack.push(bufMgr->fixBlock(file, b, LOCK_INTWRITE));
      DBRelDef def((const RelDefStruct *) bacbStack.top().getDataPtr());
      if (strcmp(qname.relationName, def.relationName().c_str()) == 0) { ;
        found = true;
      } else {
        bufMgr->unfixBlock(bacbStack.top());
        bacbStack.pop();
      }
    }
    if (found == false)
      throw DBSysCatMgrException("no such relation");

    RelDefStruct *relDef = (RelDefStruct *) bacbStack.top().getDataPtr();
    found = false;
    uint pos = 0;
    for (uint i = 0; found == false && i < relDef->attrCnt; ++i) {
      if (strcmp(relDef->attrList[i].attrName, qname.attributeName) == 0) {
        found = true;
        pos = i;
      }
    }

    if (found == false)
      throw DBSysCatMgrException("no such attribute");

    if (relDef->attrList[pos].isIndexed == false)
      throw DBSysCatMgrException("no index");

    bufMgr->upgradeToExclusive(bacbStack.top());
    relDef->attrList[pos].isIndexed = false;
    relDef->attrList[pos].indexType[0] = '\0';
    bacbStack.top().setModified();
    bufMgr->unfixBlock(bacbStack.top());
    bacbStack.pop();
    bufMgr->dropFile(IDXNAME(dbName, qname.relationName, qname.attributeName));
  } catch (DBException e) {
    while (bacbStack.empty() == false) {
      bufMgr->unfixBlock(bacbStack.top());
      bacbStack.pop();
    }
    unlock();
    throw e;
  }
  unlock();
}

DBIndex *DBSysCatMgr::openIndex(const string dbName, const QualifiedName &qname, ModType mode) {
  LOG4CXX_INFO(logger, "openIndex()");
  LOG4CXX_DEBUG(logger, "qname: " + qname.toString());
  LOG4CXX_DEBUG(logger, "mode: " + TO_STR(mode));

  DBIndex *index = NULL;
  lock();
  try {
    string relFile(IDXNAME(dbName, qname.relationName, qname.attributeName));
    DBRelDef def = getSchemaForTable(dbName, qname.relationName);
    DBAttrDef adef = def.attrDef(qname.attributeName);
    if (adef.isIndexed() == false)
      throw DBSysCatMgrException("no such index");
    DBFile &file = bufMgr->openFile(relFile);
    index = (DBIndex *) getClassForName(adef.indexType(), 5, bufMgr, &file, adef.attrType(), mode, adef.isUnique());
  } catch (DBException e) {
    unlock();
    throw e;
  }
  unlock();
  return index;
}


DBSysCatMgrException::DBSysCatMgrException(const std::string &msg1)
        : DBRuntimeException(msg1) {
}

DBSysCatMgrException::DBSysCatMgrException(const DBSysCatMgrException &src)
        : DBRuntimeException(src) {
}

DBSysCatMgrException &DBSysCatMgrException::operator=(const DBSysCatMgrException &src) {
  DBException::operator=(src);
  return *this;
}
