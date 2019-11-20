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
        m_unfixedList(){
  if (logger != NULL) LOG4CXX_INFO(logger, "DBMyBufferMgr()");

  // TODO Code hier einfügen

  if (logger != NULL) LOG4CXX_DEBUG(logger, "this:\n" + toString("\t"));
}

/**
 * Destruktor
 */
DBMyBufferMgr::~DBMyBufferMgr() {
  LOG4CXX_INFO(logger, "~DBMyBufferMgr()");
  LOG4CXX_DEBUG(logger, "this:\n" + toString("\t"));

  // TODO Code hier einfügen

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
  throw DBException("fixBlock() not implemented");

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
    int i = findBlock(&bcb);

    // dirty and unlocked blocks can be freed
    if (bcb.getDirty()) {
        delete bcbList[i];
        bcbList[i] = NULL;  // discard block
        freeFrame(i);  // the i-th block is freed
    } else if (bcb.isUnlocked()) {
        freeFrame(i);  // the i-th block is freed
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
}



  throw DBException("isBlockOfFileOpen() not implemented");

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
  throw DBException("closeAllOpenBlocks() not implemented");

}

/**
 * Fügt createDBMyBufferMgr zur globalen Factory-Method-Map hinzu
 */
int DBMyBufferMgr::registerClass() {
  // Register as 'DBMyBufferMgr'
  setClassForName("DBMyBufferMgr", createDBMyBufferMgr);
  return 0;
}

void DBMyBufferMgr::showlist(list<int> g) {
    list <int> :: iterator it;
    for(it = g.begin(); it != g.end(); ++it)
        cout << '\t' << *it;
    cout << '\n';
}

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
