#include <hubDB/DBRandomBufferMgr.h>
#include <hubDB/DBException.h>

using namespace HubDB::Manager;
using namespace HubDB::Exception;

LoggerPtr DBRandomBufferMgr::logger(Logger::getLogger("HubDB.Buffer.DBRandomBufferMgr"));

// registerClass()-Methode am Ende dieser Datei: macht die Klasse der Factory bekannt
int rBMgr = DBRandomBufferMgr::registerClass();

// Funktion bekannt machen
extern "C" void *createDBRandomBufferMgr(int nArgs, va_list ap);

DBRandomBufferMgr::DBRandomBufferMgr(bool doThreading, int cnt) :
        DBBufferMgr(doThreading, cnt),
        bcbList(NULL) {
  if (logger != NULL) {
    LOG4CXX_INFO(logger, "DBRandomBufferMgr()");
  }

  // Array of DBBCB pointers
  bcbList = new DBBCB *[maxBlockCnt];
  for (int i = 0; i < maxBlockCnt; i++) {
    bcbList[i] = NULL; // initialized by NULL
  }

  mapSize = cnt / 32 + 1;
  bitMap = new int[mapSize]; // use a bitmap to memorize free blocks

  for (int i = 0; i < mapSize; ++i) {
    bitMap[i] = 0;
  }

  // initially : all blocks are free
  for (int i = 0; i < cnt; ++i) {
    freeFrame(i);
  }

  if (logger != NULL) {
    LOG4CXX_DEBUG(logger, "this:\n" + toString("\t"));
  }
}

DBRandomBufferMgr::~DBRandomBufferMgr() {
  LOG4CXX_INFO(logger, "~DBRandomBufferMgr()");
  LOG4CXX_DEBUG(logger, "this:\n" + toString("\t"));
  if (bcbList != NULL) {
    for (int i = 0; i < maxBlockCnt; ++i) {
      if (bcbList[i] != NULL) {
        try {
          flushBCBBlock(*bcbList[i]);
        } catch (DBException &e) {}
        delete bcbList[i];
      }
    }
    delete[] bcbList;
    delete[] bitMap;
  }
}

string DBRandomBufferMgr::toString(string linePrefix) const {
  stringstream ss;
  ss << linePrefix << "[DBRandomBufferMgr]" << endl;
  ss << DBBufferMgr::toString(linePrefix + "\t");
  lock();

  uint i, sum = 0;
  for (i = 0; i < maxBlockCnt; ++i) {
    if (isFreeFrame(i)) {
      ++sum;
    }
  }
  ss << linePrefix << "unfixedPages( size: " << sum << " ):" << endl;
  for (i = 0; i < maxBlockCnt; ++i) {
    if (isFreeFrame(i)) {
      ss << linePrefix << i << endl;
    }
  }

  ss << linePrefix << "bcbList( size: " << maxBlockCnt << " ):" << endl;
  for (int i = 0; i < maxBlockCnt; ++i) {
    ss << linePrefix << "bcbList[" << i << "]:";
    if (bcbList[i] == NULL) {
      ss << "NULL" << endl;
    } else {
      ss << endl << bcbList[i]->toString(linePrefix + "\t");
    }
  }
  ss << linePrefix << "-------------------" << endl;
  unlock();
  return ss.str();
}

int DBRandomBufferMgr::registerClass() {
  // Register as 'DBRandomBufferMgr'
  setClassForName("DBRandomBufferMgr", createDBRandomBufferMgr);
  return 0;
}

/**
 * Sorgt dafür, dass sich der Block der angegebenen Datei, welcher die
 * angegebenen Nummer hat, im Buffer befindet. Es wird ein BCB zurückgegeben,
 * welcher den entsprechenden Frame repräsentiert. Der gepufferte Block wird
 * entsprechend des angegebenen Sperrmodus gesperrt.
 * Wenn für den Parameter read der Wert false übergeben wurde, darf der Block
 * nicht in den reservierten Frame geladen werden (z.B. weil ein neuer Block
 * geschrieben wird).
 *
 * Eine Implementierung erfordert eine Verdrängungsstrategie für
 * die Verdrängung von ungenutzten Seiten (bzw. BCBs).
 *
 * Abstrakte Methode: Implementierung in Übung 1
 * @param file Die geöffnete Datei
 * @param blockNo Die Nummer des Blocks
 * @param mode Sperrmodus
 * @param read Wenn false übergeben wurde, darf der Block nicht in den reservierten
 * 							Frame geladen werden
 * @return Es wird ein BCB zurückgegeben, welcher den entsprechenden Frame repräsentiert.
 */
DBBCB *DBRandomBufferMgr::fixBlock(DBFile &file, BlockNo blockNo, DBBCBLockMode mode, bool read) {
  LOG4CXX_INFO(logger, "fixBlock()");
  LOG4CXX_DEBUG(logger, "file:\n" + file.toString("\t"));
  LOG4CXX_DEBUG(logger, "blockNo: " + TO_STR(blockNo));
  LOG4CXX_DEBUG(logger, "mode: " + DBBCB::LockMode2String(mode));
  LOG4CXX_DEBUG(logger, "read: " + TO_STR(read));
  LOG4CXX_DEBUG(logger, "this:\n" + toString("\t"));

  int i = findBlock(file, blockNo);

  LOG4CXX_DEBUG(logger, "i:" + TO_STR(i));

  // if the block was not found (loaded)
  if (i == -1) {

    // now, free the first free block
    for (i = 0; i < maxBlockCnt; ++i) {
      if (isFreeFrame(i)) {// searches the first free block
        break;
      }
    }

    if (i == maxBlockCnt) {// no free block is available
      throw DBBufferMgrException("no more free pages");
    }

    if (bcbList[i] != NULL) {  // flush the old block to disk
      // dirty blocks are not flushed -> UNDO
      if (!bcbList[i]->getDirty()) {
        flushBCBBlock(*bcbList[i]);
      }
      delete bcbList[i];
    }

    bcbList[i] = new DBBCB(file, blockNo);
    if (read) { // read block from disk
      fileMgr.readFileBlock(bcbList[i]->getFileBlock());
    }
  }

  DBBCB *rc = bcbList[i];
  if (!rc->grantAccess(mode)) { // no access given, e.g. try to access EXCLUSIVE block
    rc = NULL;
  } else {
    reserveFrame(i); // the i-th block is reserved
  }

  LOG4CXX_DEBUG(logger, "rc: " + TO_STR(rc));
  return rc;
}

/**
 * Markiert den angegebenen BCB als ungenutzt.
 *
 * Abstrakte Methode: Implementierung in Übung 1
 * @param bcb Der freizugebende BCB
 */
void DBRandomBufferMgr::unfixBlock(DBBCB &bcb) {
  LOG4CXX_INFO(logger, "unfixBlock()");
  LOG4CXX_DEBUG(logger, "bcb:\n" + bcb.toString("\t"));
  LOG4CXX_DEBUG(logger, "this:\n" + toString("\t"));

  bcb.unlock(); // unlock (however, there can be multiple locks by different threads)
  int i = findBlock(&bcb);

  // dirty and unlocked blocks can be freed
  if (bcb.getDirty()) {
    delete bcbList[i];
    bcbList[i] = NULL;  // discard block
    freeFrame(i);  // the i-th block is freed
  } else if (bcb.isUnlocked()) {
    freeFrame(i);  // the i-th block is freed
  }
}

/**
 * Gibt true zurück, wenn sich mindestens ein Block der
 * angegebenen Datei im Buffer befindet.
 *
 * Abstrakte Methode: Implementierung  in Übung 1
 * @param file Die geöffnete Datei
 * @return true, wenn sich mindestens ein Block der angegebenen Datei im Buffer befindet.
 */
bool DBRandomBufferMgr::isBlockOfFileOpen(DBFile &file) const {
  LOG4CXX_INFO(logger, "isBlockOfFileOpen()");
  LOG4CXX_DEBUG(logger, "file:\n" + file.toString("\t"));
  LOG4CXX_DEBUG(logger, "this:\n" + toString("\t"));
  for (int i = 0; i < maxBlockCnt; ++i) {
    // test if the file is open, e.g. any block of the file was read into the buffer
    if (bcbList[i] != NULL
        && bcbList[i]->getFileBlock() == file) {
      LOG4CXX_DEBUG(logger, "rc: true");
      return true;
    }
  }
  LOG4CXX_DEBUG(logger, "rc: false");
  return false;
}

/**
 * Entfernt alle gepufferten Blöck der angegebenen Datei aus dem Buffer.
 * Falls einer der Blöcke gerade benutzt wird (also gesperrt "locked" ist), dann
 * wird eine DBBufferMgrException geworfen.
 *
 * Abstrakte Methode: Implementierung in Übung 1
 * @param file Die geöffnete Datei
 */
void DBRandomBufferMgr::closeAllOpenBlocks(DBFile &file) {
  LOG4CXX_INFO(logger, "closeAllOpenBlocks()");
  LOG4CXX_DEBUG(logger, "file:\n" + file.toString("\t"));
  LOG4CXX_DEBUG(logger, "this:\n" + toString("\t"));
  for (int i = 0; i < maxBlockCnt; ++i) {
    // write unlocked blocks to disk
    if (bcbList[i] != NULL && bcbList[i]->getFileBlock() == file) {
      if (!bcbList[i]->isUnlocked()) {
        throw DBBufferMgrException("can not close fileblock because it is still locked");
      }
      else {
        if (!bcbList[i]->getDirty()) {
          flushBCBBlock(*bcbList[i]); // flush to disk
        }
        delete bcbList[i];
        bcbList[i] = NULL;
        freeFrame(i); // the i-th block is freed
      }
    }
  }
}

/**
 * Sorgt dafür, dass sich der Block der angegebenen Datei, welcher die
 * angegebenen Nummer hat, im Buffer befindet. Es wird ein BCB zurückgegeben,
 * welcher den entsprechenden Frame repräsentiert. Der gepufferte Block wird
 * entsprechend des angegebenen Sperrmodus gesperrt.
 * Wenn für den Parameter read der Wert false übergeben wurde, darf der Block
 * nicht in den reservierten Frame geladen werden (z.B. weil ein neuer Block
 * geschrieben wird).
 *
 * Eine Implementierung erfordert eine Verdrängungsstrategie für
 * die Verdrängung von ungenutzten Seiten (bzw. BCBs).
 *
 * Abstrakte Methode: Implementierung in Übung 1
 * @param file Die geöffnete Datei
 * @param blockNo Die Nummer des Blocks
 * @param mode Sperrmodus
 * @param read Wenn false übergeben wurde, darf der Block nicht in den reservierten
 * 							Frame geladen werden
 * @return Es wird ein BCB zurückgegeben, welcher den entsprechenden Frame repräsentiert.
 */
int DBRandomBufferMgr::findBlock(DBFile &file, BlockNo blockNo) {
  LOG4CXX_INFO(logger, "findBlock()");
  int pos = -1;
  // search if the block was read into the buffer
  for (int i = 0; pos == -1 && i < maxBlockCnt; ++i) {
    if (bcbList[i] != NULL &&
        bcbList[i]->getFileBlock() == file &&
        bcbList[i]->getFileBlock().getBlockNo() == blockNo) {
      pos = i; // block was found at position i
    }
  }
  LOG4CXX_DEBUG(logger, "pos: " + TO_STR(pos));
  return pos;
}


/**
 * Liefert die Position des BCB im Frame-Array
 * Sequentielle Suche
 * @param bcb
 * @return
 */
int DBRandomBufferMgr::findBlock(DBBCB *bcb) {
  LOG4CXX_INFO(logger, "findBlock()");
  int pos = -1;

  // returns the position of the block
  for (int i = 0; pos == -1 && i < maxBlockCnt; ++i) {
    if (bcbList[i] == bcb) {
      pos = i;
    }
  }
  LOG4CXX_DEBUG(logger, "pos: " + TO_STR(pos));
  return pos;
}

extern "C" void *createDBRandomBufferMgr(int nArgs, va_list ap) {
  DBRandomBufferMgr *b = NULL;
  bool t; // multi-threading
  uint c; // size of the buffer manager
  switch (nArgs) {
    case 1: // with one argument
      t = va_arg(ap, int);
      b = new DBRandomBufferMgr(t);
      break;
    case 2: // with two arguments
      t = va_arg(ap, int);
      c = va_arg(ap, int);
      b = new DBRandomBufferMgr(t, c);
      break;
    default:
      throw DBException("Invalid number of arguments");
  }
  return b;
}
