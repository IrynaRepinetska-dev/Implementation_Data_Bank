#include <hubDB/DBTable.h>
#include <hubDB/DBFileBlock.h>
#include <hubDB/DBException.h>

using namespace HubDB::Manager;
using namespace HubDB::Table;
using namespace HubDB::Exception;

LoggerPtr DBTable::logger(Logger::getLogger("HubDB.Table.DBTable"));

const BlockNo DBTable::rootBlockNo(0);

DBTable::DBTable(DBBufferMgr &bufMgr, DBFile &file, const DBRelDef &rel, ModType mode) :
        bufMgr(bufMgr),
        file(file),
        rel(rel),
        mode(mode),
        slotMaskCnt(1),
        _sizeOfPage(sizeof(tablePageLayout)) {
  if (logger != NULL) LOG4CXX_INFO(logger, "DBTable()");
  slotMaskCnt = tuplesPerPage() / 32 + 1;
  _sizeOfPage = sizeof(BlockNo) + slotMaskCnt * sizeof(uint);
  if (logger != NULL) {
    LOG4CXX_DEBUG(logger, "slotMaskCnt: " + TO_STR(slotMaskCnt));
    LOG4CXX_DEBUG(logger, "_sizeOfPage: " + TO_STR(_sizeOfPage));
  }
  assert(tuplesPerPage() >= 1);
  bacbStack.push(bufMgr.fixBlock(file, rootBlockNo, mode == READ ? LOCK_SHARED : LOCK_INTWRITE));
}

DBTable::~DBTable() {
  LOG4CXX_INFO(logger, "~DBTable()");
  unfixBACBs(false);
}

void DBTable::unfixBACBs(bool setDirty) {
  LOG4CXX_INFO(logger, "unfixBACBS()");
  LOG4CXX_DEBUG(logger, "setDirty: " + TO_STR(setDirty));
  LOG4CXX_DEBUG(logger, "bacbStack.size()= " + TO_STR(bacbStack.size()));

  while (bacbStack.empty() == false) {
    try {
      if (bacbStack.top().getModified()) {
        if (setDirty == true) {
          bacbStack.top().setDirty();
        }
      }
      bufMgr.unfixBlock(bacbStack.top());
    } catch (DBException e) {}
    bacbStack.pop();
  }
}

void DBTable::undo() {
  LOG4CXX_DEBUG(logger, "undo()");
  unfixBACBs(true);
}

uint DBTable::tuplesPerPage() {
  return (DBFileBlock::getBlockSize() - sizeOfPage()) / rel.tupleSize();
}

string DBTable::toString(string linePrefix) {
  stringstream ss;
  ss << linePrefix << "[DBTable]" << endl;
  ss << linePrefix << "-----------" << endl;
  return ss.str();
}

void DBTable::initializeTable(DBBufferMgr &bufMgr, DBFile &file) {
  LOG4CXX_INFO(logger, "initializeTable()");
  LOG4CXX_DEBUG(logger, "bufMgr:\n" + bufMgr.toString("\t"));
  LOG4CXX_DEBUG(logger, "file:\n" + file.toString("\t"));

  if (bufMgr.getBlockCount(file) != 0)
    throw DBTableException("can not initializie exisiting table");

  stack<DBBACB> bacbStack;
  try {
    bacbStack.push(bufMgr.fixNewBlock(file));
    tablePageLayout *page = (tablePageLayout *) bacbStack.top().getDataPtr();
    memset(page, 0, DBFileBlock::getBlockSize());
    page->nextFreeBlock = rootBlockNo;
    bacbStack.top().setModified();
  } catch (DBException e) {
    if (bacbStack.empty() == false)
      bufMgr.unfixBlock(bacbStack.top());
    throw e;
  }
  bufMgr.unfixBlock(bacbStack.top());
  bacbStack.pop();
}

void DBTable::assertUnique(DBTuple &tuple) {
  LOG4CXX_INFO(logger, "assertUnique()");
  LOG4CXX_DEBUG(logger, "tuple:\n" + tuple.toString("\t"));
  bool uni[MAX_ATTR_PER_REL];
  bool check = false;
  for (uint i = 0; i < rel.attrCnt(); ++i) {
    DBAttrDef attr = rel.attrDef(i);
    if (attr.isUnique() == true && attr.isIndexed() == false) {
      uni[i] = true;
      check = true;
    } else {
      uni[i] = false;
    }
  }
  LOG4CXX_DEBUG(logger, "check: " + TO_STR(check));
  if (check == false)
    return;

  list<DBTuple> tupleList;
  TID tid;
  tid.page = 0;
  tid.slot = 0;
  do {
    tupleList.clear();
    tid = readSeqFromTID(tid, 100, tupleList);
    LOG4CXX_DEBUG(logger, (string) "read " + TO_STR(tupleList.size()) + " tuples");
    list<DBTuple>::iterator i = tupleList.begin();
    while (i != tupleList.end()) {
      for (uint c = 0; c < rel.attrCnt(); ++c)
        if (uni[c] == true && (*i).getAttrVal(c) == tuple.getAttrVal(c))
          throw DBTableException("unique constraint");
      ++i;
    }
  } while (tupleList.size() == 100);
  assert(bacbStack.size() == 1);
}

void DBTable::insert(DBTuple &tuple) {
  LOG4CXX_INFO(logger, "insert()");
  LOG4CXX_DEBUG(logger, "tuple:\n" + tuple.toString("\t"));

  if (bacbStack.size() != 1)
    throw DBTableException("BACB Stack is invalid");

  assertUnique(tuple);

  if (bacbStack.top().getLockMode() != LOCK_EXCLUSIVE)
    bufMgr.upgradeToExclusive(bacbStack.top());

  tablePageLayout *page = (tablePageLayout *) bacbStack.top().getDataPtr();
  uint max = tuplesPerPage();
  LOG4CXX_DEBUG(logger, "max: " + TO_STR(max));
  uint pos = findFreeSlot(page->slotUsedMask, max);
  LOG4CXX_DEBUG(logger, "pos: " + TO_STR(pos));
  BlockNo n = page->nextFreeBlock;
  while (pos == max && n != rootBlockNo) {
    LOG4CXX_DEBUG(logger, "n: " + TO_STR(n));
    bacbStack.push(bufMgr.fixBlock(file, n, LOCK_EXCLUSIVE));
    page = (tablePageLayout *) bacbStack.top().getDataPtr();
    pos = findFreeSlot(page->slotUsedMask, max);
    LOG4CXX_DEBUG(logger, "pos: " + TO_STR(pos));
    if (pos == max) {
      n = page->nextFreeBlock;
      page->nextFreeBlock = rootBlockNo;
      bacbStack.top().setModified();
      bufMgr.unfixBlock(bacbStack.top());
      bacbStack.pop();
    }
  }
  LOG4CXX_DEBUG(logger, "n: " + TO_STR(n));
  LOG4CXX_DEBUG(logger, "pos: " + TO_STR(pos));

  if (pos == max) {
    bacbStack.push(bufMgr.fixNewBlock(file));
    page = (tablePageLayout *) bacbStack.top().getDataPtr();
    memset(page, 0, DBFileBlock::getBlockSize());
    page->nextFreeBlock = rootBlockNo;
    pos = 0;
    bacbStack.top().setModified();
  }

  TID t;
  t.page = bacbStack.top().getBlockNo();
  t.slot = pos;
  tuple.setTID(t);
  page = (tablePageLayout *) bacbStack.top().getDataPtr();
  setSlot(page->slotUsedMask, pos);
  char *ptr = bacbStack.top().getDataPtr();
  ptr += sizeOfPage();
  ptr += pos * rel.tupleSize();
  tuple.write(ptr);
  bacbStack.top().setModified();
  if (bacbStack.size() > 1) {
    bufMgr.unfixBlock(bacbStack.top());
    bacbStack.pop();
    page = (tablePageLayout *) bacbStack.top().getDataPtr();
    page->nextFreeBlock = t.page;
    bacbStack.top().setModified();
  }

  assert(bacbStack.size() == 1);

}

void DBTable::remove(const DBListTID &tidList) {
  LOG4CXX_INFO(logger, "remove()");
  if (bacbStack.size() != 1)
    throw DBTableException("BACB Stack is invalid");

  if (bacbStack.top().getLockMode() != LOCK_EXCLUSIVE)
    bufMgr.upgradeToExclusive(bacbStack.top());

  DBListTID::const_iterator i = tidList.begin();
  while (i != tidList.end()) {
    const TID &tid = (*i);
    LOG4CXX_INFO(logger, "tid: " + tid.toString());

    tablePageLayout *page = (tablePageLayout *) bacbStack.top().getDataPtr();
    BlockNo n = page->nextFreeBlock;

    if (rootBlockNo != tid.page) {
      bacbStack.push(bufMgr.fixBlock(file, tid.page, LOCK_EXCLUSIVE));
    }

    page = (tablePageLayout *) bacbStack.top().getDataPtr();

    unsetSlot(page->slotUsedMask, tid.slot);

    if (page->nextFreeBlock == rootBlockNo) {
      page->nextFreeBlock = n;
      n = bacbStack.top().getBlockNo();
    }
    bacbStack.top().setModified();

    if (bacbStack.size() > 1) {
      bufMgr.unfixBlock(bacbStack.top());
      bacbStack.pop();
      page = (tablePageLayout *) bacbStack.top().getDataPtr();
      if (n != page->nextFreeBlock) {
        page->nextFreeBlock = n;
        bacbStack.top().setModified();
      }
    }
    ++i;
  }

  assert(bacbStack.size() == 1);
}

void DBTable::readTIDs(const list<TID> &tids, list<DBTuple> &tupleList) {
  LOG4CXX_INFO(logger, "readTIDs()");
  if (bacbStack.size() != 1)
    throw DBTableException("BACB Stack is invalid");

  list<TID>::const_iterator i = tids.begin();
  while (i != tids.end()) {
    const TID &tid = (*i);
    LOG4CXX_DEBUG(logger, "tid: " + tid.toString());
    if (tid.page != rootBlockNo)
      bacbStack.push(bufMgr.fixBlock(file, tid.page, LOCK_SHARED));

    const char *ptr = bacbStack.top().getDataPtr();
    tablePageLayout *page = (tablePageLayout *) ptr;
    if (isSlot(page->slotUsedMask, tid.slot) == true) {
      DBTuple t;
      t.setTID(tid);
      ptr += sizeOfPage();
      ptr += tid.slot * rel.tupleSize();
      t.read(rel, ptr);
      LOG4CXX_DEBUG(logger, "tuple:\n" + t.toString("\t"));
      tupleList.push_back(t);
    } else {
      throw DBTableException("invalid TID");
    }

    if (tid.page != rootBlockNo) {
      bufMgr.unfixBlock(bacbStack.top());
      bacbStack.pop();
    }
    ++i;
  }
  assert(bacbStack.size() == 1);
}

TID DBTable::readSeqFromTID(TID tid,
                            uint numOfTuples,
                            list<DBTuple> &tupleList) {
  LOG4CXX_INFO(logger, "readSeqFromTID()");
  LOG4CXX_DEBUG(logger, "tid: " + tid.toString());
  LOG4CXX_DEBUG(logger, "numOfTuples: " + TO_STR(numOfTuples));
  if (bacbStack.size() != 1)
    throw DBTableException("BACB Stack is invalid");

  uint max = tuplesPerPage();
  while (numOfTuples > 0 && tid.page < bufMgr.getBlockCount(file)) {
    if (tid.page != rootBlockNo)
      bacbStack.push(bufMgr.fixBlock(file, tid.page, LOCK_SHARED));
    const char *ptr = bacbStack.top().getDataPtr();
    LOG4CXX_DEBUG(logger, "ptr: " + TO_STR((char *) ptr));
    tablePageLayout *page = (tablePageLayout *) ptr;
    ptr += sizeOfPage();
    if (tid.slot != 0) {
      ptr += rel.tupleSize() * tid.slot;
    }
    LOG4CXX_DEBUG(logger, "ptr: " + TO_STR((char *) ptr));

    for (; tid.slot < max && numOfTuples > 0; ++tid.slot) {
      LOG4CXX_DEBUG(logger, "ptr: " + TO_STR((char *) ptr));
      if (isSlot(page->slotUsedMask, tid.slot) == true) {
        --numOfTuples;
        DBTuple t;
        t.setTID(tid);
        t.read(rel, ptr);
        LOG4CXX_DEBUG(logger, "tuple:\n" + t.toString("\t"));
        tupleList.push_back(t);
        LOG4CXX_DEBUG(logger, "push_back done");
      }
      ptr += rel.tupleSize();
    }
    LOG4CXX_DEBUG(logger, "finished for");
    if (bacbStack.top().getBlockNo() != rootBlockNo) {
      bufMgr.unfixBlock(bacbStack.top());
      bacbStack.pop();
    }
    if (numOfTuples > 0) {
      ++tid.page;
      tid.slot = 0;
    }
  }
  LOG4CXX_DEBUG(logger, "return tid:" + tid.toString());
  assert(bacbStack.size() == 1);
  return tid;
}

uint DBTable::findFreeSlot(uint *slotMask, uint max) {
  LOG4CXX_INFO(logger, "findFreeSlot()");
  LOG4CXX_DEBUG(logger, "slotMask: " + TO_STR(slotMask));
  LOG4CXX_DEBUG(logger, "max: " + TO_STR(max));

  for (uint c = 0; c < slotMaskCnt; ++c) {
    uint tmp = slotMask[c];
    for (uint i = 0; i < 32; ++i) {
      LOG4CXX_DEBUG(logger, "tmp: " + TO_STR(tmp));
      if ((tmp & 1) == 0)
        return c * 32 + i;
      tmp >>= 1;
    }
  }
  return max;
}

void DBTable::setSlot(uint *slotMask, uint pos) {
  uint c = pos / 32;
  uint idx = pos % 32;
  slotMask[c] |= 1 << idx;
}

void DBTable::unsetSlot(uint *slotMask, uint pos) {
  uint c = pos / 32;
  uint idx = pos % 32;
  slotMask[c] &= ~(1 << idx);
}

bool DBTable::isSlot(uint *slotMask, uint pos) {
  uint c = pos / 32;
  uint idx = pos % 32;
  if ((slotMask[c] & (1 << idx)) == 0) {
    return false;
  } else {
    return true;
  }
}

DBTableException::DBTableException(const std::string &msg1)
        : DBRuntimeException(msg1) {
}

DBTableException::DBTableException(const DBTableException &src)
        : DBRuntimeException(src) {
}

DBTableException &DBTableException::operator=(const DBTableException &src) {
  DBException::operator=(src);
  return *this;
}
