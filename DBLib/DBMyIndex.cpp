#include <hubDB/DBMyIndex.h>
#include <hubDB/DBException.h>

/**
 * Die Implementierung eines Index für Aufgabe 2.
 */

using namespace HubDB::Index;
using namespace HubDB::Exception;

LoggerPtr DBMyIndex::logger(Logger::getLogger("HubDB.Index.DBMyIndex"));

// registerClass()-Methode am Ende dieser Datei: macht die Klasse der Factory bekannt
int rMyIdx = DBMyIndex::registerClass();

// Funktion bekannt machen
extern "C" void *createDBMyIndex(int nArgs, va_list ap);

/**
 * Konstruktor
 */
DBMyIndex::DBMyIndex(DBBufferMgr &bufferMgr, DBFile &file,
                       enum AttrTypeEnum attrType, ModType mode, bool unique) :
      // call base constructor
      DBIndex(bufferMgr, file, attrType, mode, unique) {
  if (logger != NULL) {
    LOG4CXX_INFO(logger, "DBMyIndex()");
  }

  // TODO Code hier einfügen

  if (logger != NULL) {
    LOG4CXX_DEBUG(logger, "this:\n" + toString("\t"));
  }
}

/**
 * Destruktor
 */
DBMyIndex::~DBMyIndex() {
  LOG4CXX_INFO(logger, "~DBMyIndex()");

  // TODO Code hier einfügen

}


/**
 * Ausgabe des Indexes zum Debuggen
 */
string DBMyIndex::toString(string linePrefix) const {
  stringstream ss;
  ss << linePrefix << "[DBMyIndex]" << endl;
  ss << DBIndex::toString(linePrefix + "\t") << endl;

  // TODO Code hier einfügen

  ss << linePrefix << "-----------" << endl;
  return ss.str();
}

/**
 * Erstellt Indexdatei.
 */
void DBMyIndex::initializeIndex() {
  LOG4CXX_INFO(logger, "initializeIndex()");

  // TODO Code hier einfügen
  throw DBException("initializeIndex() not implemented");

}

/**
 * Sucht im Index nach einem bestimmten Wert
 *
 * Die Funktion ändert die übergebene Liste
 * von TID Objekten (siehe DBTypes.h: typedef list<TID> DBListTID;)
 *
 * @param val  zu suchender Schluesselwert
 * @param tids Referenz auf Liste von TID Objekten
 */
void DBMyIndex::find(const DBAttrType &val, DBListTID &tids) {
  LOG4CXX_INFO(logger, "find()");
  LOG4CXX_DEBUG(logger, "val:\n" + val.toString("\t"));

  // TODO Code hier einfügen
  throw DBException("Find() not implemented");
}

/**
 * Einfügen eines Schluesselwertes (moeglicherweise bereits vorhangen)
 * zusammen mit einer Referenz auf eine TID.
 * @param val Schlüsselwert
 * @param tid
 */
void DBMyIndex::insert(const DBAttrType &val, const TID &tid) {
  LOG4CXX_INFO(logger, "insert()");
  LOG4CXX_DEBUG(logger, "val:\n" + val.toString("\t"));
  LOG4CXX_DEBUG(logger, "tid: " + tid.toString());

  // TODO Code hier einfügen
  throw DBException("insert() not implemented");

}

/**
 * Entfernt alle Tupel aus der Liste der tids.
 *
 * Um schneller auf der richtigen Seite anfangen zu koennen,
 * wird zum Suchen zusätzlich noch der zu löschende Wert "value" übergeben
 *
 * @param val Der zu löschende Wert
 * @param tid Die Tupel Ids (mindestens eine - mehrere möglich, falls Duplikate erlaubt sind)
 */
void DBMyIndex::remove(const DBAttrType &val, const list<TID> &tid) {
  LOG4CXX_INFO(logger, "remove()");
  LOG4CXX_DEBUG(logger, "val:\n" + val.toString("\t"));

  // TODO Code hier einfügen
  throw DBException("remove() not implemented");

}


/**
 * Fügt createDBMyIndex zur globalen Factory-Method-Map hinzu
 */
int DBMyIndex::registerClass() {
  setClassForName("DBMyIndex", createDBMyIndex);
  return 0;
}

/**
 * Wird aufgerufen von HubDB::Types::getClassForName von DBTypes, um DBIndex zu erstellen
 * @param DBBufferMgr *: Buffermanager
 * @param DBFile *: Dateiobjekt
 * @param attrType: Attributtp
 * @param ModeType: READ, WRITE
 * @param bool: unique Indexattribut
 */
extern "C" void *createDBMyIndex(int nArgs, va_list ap) {
  // Genau 5 Parameter
  if (nArgs != 5) {
    throw DBException("Invalid number of arguments");
  }
  DBBufferMgr *bufMgr = va_arg(ap, DBBufferMgr *);
  DBFile *file = va_arg(ap, DBFile *);
  enum AttrTypeEnum attrType = (enum AttrTypeEnum) va_arg(ap, int);
  ModType m = (ModType) va_arg(ap, int); // READ, WRITE
  bool unique = (bool) va_arg(ap, int);
  return new DBMyIndex(*bufMgr, *file, attrType, m, unique);
}