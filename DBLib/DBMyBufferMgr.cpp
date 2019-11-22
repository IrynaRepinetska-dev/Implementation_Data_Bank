#include <hubDB/DBMyBufferMgr.h>
#include <hubDB/DBException.h>

/**
 * Die Implementierung eines BufferManagers für Aufgabe 1.
 */

using namespace HubDB::Manager;
using namespace HubDB::Exception;
using namespace std;

LoggerPtr DBMyBufferMgr::logger(Logger::getLogger("HubDB.Buffer.DBMyBufferMgr"));

// registerClass()-Methode am Ende dieser Datei: macht die Klasse der Factory bekannt
int myBMgr = DBMyBufferMgr::registerClass();

// Funktion bekannt machen
extern "C" void *createDBMyBufferMgr(int nArgs, va_list ap);


/**
 * Konstroktur
 * @param doThreading multithreading
 * @param cnt Anzahl der Blöcke im Buffer
 */
DBMyBufferMgr::DBMyBufferMgr(bool doThreading, int cnt) :
        DBBufferMgr(doThreading, cnt),
        m_unfixedList(),
        bcbList(NULL){
  if (logger != NULL) LOG4CXX_INFO(logger, "DBMyBufferMgr()");

  // TODO Code hier einfügen
    // Array of DBBCB pointers
    bcbList = new DBBCB *[maxBlockCnt];
    for (int i = 0; i < maxBlockCnt; i++) {
        bcbList[i] = NULL; // initialized by NULL
    }
    mapSize = cnt / sizeof(int) + 1;
    bitMap = new int[mapSize]; // use a bitmap to memorize free blocks

    for (int i = 0; i < mapSize; ++i) {
        bitMap[i] = 0;
    }

    // initially : all blocks are free
    for (int i = 0; i < cnt; ++i) {
        freeFrame(i);
    }


    if (logger != NULL) LOG4CXX_DEBUG(logger, "this:\n" + toString("\t"));
}

/**
 * Destruktor
 */
DBMyBufferMgr::~DBMyBufferMgr() {
  LOG4CXX_INFO(logger, "~DBMyBufferMgr()");
  LOG4CXX_DEBUG(logger, "this:\n" + toString("\t"));

  // TODO Code hier einfügen
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
        //delete m_unfixedList??
        delete[] bitMap;

        m_unfixedList.~list();
    }

}

/**
 * Ausgabe des Buffers zum Debuggen
 */
string DBMyBufferMgr::toString(string linePrefix) const {
  stringstream ss;
  ss << linePrefix << "[DBMyBufferMgr]" << endl;
  ss << DBBufferMgr::toString(linePrefix + "\t");
  lock();

  // TODO Code hier einfügen

  ss << linePrefix << "-------------------" << endl;
  unlock();
  return ss.str();
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
DBBCB * DBMyBufferMgr::fixBlock(
        DBFile & file, BlockNo blockNo, DBBCBLockMode mode, bool read)
{
  LOG4CXX_INFO(logger,"fixBlock()");
  LOG4CXX_DEBUG(logger,"file:\n" + file.toString("\t"));
  LOG4CXX_DEBUG(logger,"blockNo: " + TO_STR(blockNo));
  LOG4CXX_DEBUG(logger,"mode: " + DBBCB::LockMode2String(mode));
  LOG4CXX_DEBUG(logger,"read: " + TO_STR(read));
  LOG4CXX_DEBUG(logger,"this:\n" + toString("\t"));

  // TODO Code hier einfügen
    int i = findBlock(file, blockNo);

    LOG4CXX_DEBUG(logger, "i:" + TO_STR(i));

    // if the block was not found (loaded)
    if (i == -1) {

        // now, free the first free block
        for (i = 0; i < maxBlockCnt; ++i) {
            if (bcbList[i]== NULL) {// searches the first free frame in Buffer
                break;
            }
        }

        if (i == maxBlockCnt) {// no free block is available

            // Verdraenungsstrategie LRU:
            if(m_unfixedList.empty()){
                throw DBBufferMgrException("there are no fixed Blocks ");
            }
            // Find index of the unfixed Block to replace --> save in i
            i = m_unfixedList.back();
            //remove this Block from the list of unfixed Blocks.
            m_unfixedList.pop_back();
           // flush the old block to disk
            // dirty blocks are not flushed -> UNDO
            if (!bcbList[i]->getDirty()) {
                flushBCBBlock(*bcbList[i]);
            }
            delete bcbList[i];
            // the i-th block is reserved
            //reserveFrame(i);

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
       // if (isFreeFrame(i)){
          //  reserveFrame(i); // the i-th block is reserved
            m_unfixedList.remove(i);
        //}

    }

    LOG4CXX_DEBUG(logger, "rc: " + TO_STR(rc));
    return rc;
  //throw DBException("fixBlock() not implemented");

}

/**
 * Markiert den angegebenen BCB als ungenutzt.
 *
 * Abstrakte Methode: Implementierung in Übung 1
 * @param bcb Der freizugebende BCB
 */
void DBMyBufferMgr::unfixBlock(DBBCB &bcb) {
  LOG4CXX_INFO(logger, "unfixBlock()");
  LOG4CXX_DEBUG(logger, "bcb:\n" + bcb.toString("\t"));
  LOG4CXX_DEBUG(logger, "this:\n" + toString("\t"));

  // TODO Code hier einfügen
    bcb.unlock(); // unlock (however, there can be multiple locks by different threads)
    int i = findBlock(&bcb); // search for position of BCBBlock in bcbList
    if(i==-1){
        throw DBException("This Block cannot be unfixed, because it is not in the Buffer.");
    }

    // dirty and unlocked blocks can be freed
    if (bcb.getDirty()) {
        delete bcbList[i];
        bcbList[i] = NULL;  // discard block
        freeFrame(i);  // the i-th block is freed
        m_unfixedList.remove(i);
    } else if (bcb.isUnlocked()) {
        freeFrame(i);
        // the i-th block is freed
        //pop from the end to find a block to replace
        m_unfixedList.push_front(i);
    }

  //throw DBException("unfixBlock() not implemented");

}

/**
 * Gibt true zurück, wenn sich mindestens ein Block der
 * angegebenen Datei im Buffer befindet.
 *
 * Abstrakte Methode: Implementierung  in Übung 1
 * @param file Die geöffnete Datei
 * @return true, wenn sich mindestens ein Block der angegebenen Datei im Buffer befindet.
 */
bool DBMyBufferMgr::isBlockOfFileOpen(DBFile &file) const {
  LOG4CXX_INFO(logger, "isBlockOfFileOpen()");
  LOG4CXX_DEBUG(logger, "file:\n" + file.toString("\t"));
  LOG4CXX_DEBUG(logger, "this:\n" + toString("\t"));

  // TODO Code hier einfügen

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


  //throw DBException("isBlockOfFileOpen() not implemented");

}

/**
 * Entfernt alle gepufferten Blöck der angegebenen Datei aus dem Buffer.
 * Falls einer der Blöcke gerade benutzt wird (also gesperrt "locked" ist), dann
 * wird eine DBBufferMgrException geworfen.
 *
 * Abstrakte Methode: Implementierung in Übung 1
 * @param file Die geöffnete Datei
 */
void DBMyBufferMgr::closeAllOpenBlocks(DBFile &file) {
  LOG4CXX_INFO(logger, "closeAllOpenBlocks()");
  LOG4CXX_DEBUG(logger, "file:\n" + file.toString("\t"));
  LOG4CXX_DEBUG(logger, "this:\n" + toString("\t"));

  // TODO Code hier einfügen
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
                m_unfixedList.remove(i);
                freeFrame(i); // the i-th block is freed
            }
        }
    }

  //throw DBException("closeAllOpenBlocks() not implemented");

}

/**
 * Fügt createDBMyBufferMgr zur globalen Factory-Method-Map hinzu
 */
int DBMyBufferMgr::registerClass() {
  // Register as 'DBMyBufferMgr'
  setClassForName("DBMyBufferMgr", createDBMyBufferMgr);
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
int DBMyBufferMgr::findBlock(DBFile &file, BlockNo blockNo) {
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
int DBMyBufferMgr::findBlock(DBBCB *bcb) {
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

void DBMyBufferMgr::showlist(list<int> g) {
    list <int> :: iterator it;
    for(it = g.begin(); it != g.end(); ++it)
        cout << '\t' << *it;
    cout << '\n';
}

void DBMyBufferMgr::freeFrame(int i){ bitMap[i / sizeof(int)] |= (1 << (i % sizeof(int)));}
void DBMyBufferMgr::reserveFrame(int i){ bitMap[i / sizeof(int)] &= (~(1 << (i % sizeof(int))));}
bool DBMyBufferMgr::isFreeFrame(int i){ return (bitMap[i / sizeof(int)] & (1 << (i % sizeof(int)))) != 0 ? true : false;}

/**
 * Wird aufgerufen von HubDB::Types::getClassForName von DBTypes, um DBIndex zu erstellen
 */
extern "C" void *createDBMyBufferMgr(int nArgs, va_list ap) {
  DBMyBufferMgr *b = NULL;
  bool t; // multi-threading
  uint c; // size of the buffer manager
  switch (nArgs) {
    case 1: // with one argument
      t = va_arg(ap, int);
      b = new DBMyBufferMgr(t);
      break;
    case 2: // with two arguments
      t = va_arg(ap, int);
      c = va_arg(ap, int);
      b = new DBMyBufferMgr(t, c);
      break;
    default:
      throw DBException("Invalid number of arguments");
  }
  return b;


}
