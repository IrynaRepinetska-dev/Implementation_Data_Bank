#include <hubDB/DBMyQueryManager.h>

using namespace HubDB::Exception;
using namespace HubDB::Manager;
using namespace HubDB::Table;
using namespace HubDB::Index;

LoggerPtr DBMyQueryManager::logger(Logger::getLogger("HubDB.Query.DBMyQueryManager"));

int rMyQMgr = DBMyQueryManager::registerClass();

extern "C" void *createDBMyQueryMgr(int nArgs, va_list ap);

DBMyQueryManager::DBMyQueryManager(DBServerSocket &socket, DBSysCatMgr &sysCatMgr) :
        DBQueryMgr(socket, sysCatMgr) {
  if (logger != NULL) {
    LOG4CXX_INFO(logger, "DBMyQueryMgr()");
  }

  // TODO Code hier einfügen

}

/**
 * Destruktor
 */
DBMyQueryManager::~DBMyQueryManager() {
  LOG4CXX_INFO(logger, "~DBMyQueryManager()");

  // TODO Code hier einfügen
}

string DBMyQueryManager::toString(string linePrefix) const {
  stringstream ss;
  ss << linePrefix << "[DBMyQueryManager]" << endl;

  // TODO Code hier einfügen

  ss << linePrefix << "----------------" << endl;
  return ss.str();
}

/**
 * Diese Methode bestimmt Paare von Join-Partner-Tupeln und
 * füllt die übergegebene Tupelpaar-Liste mit diesen. Jedes Paar
 * enthä̈lt jeweils ein Tupel aus jeder der beiden angegebenen
 * Tabellen. Die einzelnen Partner-Tupel mü̈ssen jeweils für
 * sich alle Selektionsprädikate der für ihre Tabelle übergegebenen Liste
 * erfüllen. Weiterhin müssen alle Partner-Tupel bei den jeweils
 * angegebenen Attributen dieselben Werte besitzen.
 * @param table
 * @param attrJoinPos
 * @param where
 * @param tuples
 */
void DBMyQueryManager::selectJoinTuple(DBTable *table[2],
                                       uint attrJoinPos[2],
                                       DBListPredicate where[2],
                                       DBListJoinTuple &tuples) {
  LOG4CXX_INFO(logger, "selectJoinTuple()");

  // TODO Code hier einfügen
  throw DBException("selectJoinTuple() not implemented");

}

/**
 * Diese Methode selektiert aus der angegebenen Tabelle alle Tupel, welche
 * alle Selektionsprädikate in der übergegebene Liste erfüllen.
 * Mit den selektierten Tupel wird die übergegebene Tupelliste gefüllt.
 * @param table
 * @param where
 * @param tuple
 */
void DBMyQueryManager::selectTuple(DBTable *table,
        DBListPredicate &where,
        DBListTuple &tuple) {
  LOG4CXX_INFO(logger, "selectTuple()");
  LOG4CXX_DEBUG(logger, "table:\n" + table->toString("\t"));
  LOG4CXX_DEBUG(logger, "where: " + TO_STR(where));

  // TODO Code hier einfügen
  throw DBException("selectTuple() not implemented");

}

/**
 * Fügt createDBMyQueryMgr zur globalen factory method-map hinzu
 */
int DBMyQueryManager::registerClass() {
  setClassForName("DBMyQueryManager", createDBMyQueryMgr);
  return 0;
}

/**
 * Wird aufgerufen von HubDB::Types::getClassForName von DBTypes,
 * um QueryManager zu erstellen
 *
 */
extern "C" void *createDBMyQueryMgr(int nArgs, va_list ap) {
  if (nArgs != 2) {
    throw DBException("Invalid number of arguments");
  }
  DBServerSocket *socket = va_arg(ap, DBServerSocket *);
  DBSysCatMgr *sysCat = va_arg(ap, DBSysCatMgr *);
  return new DBMyQueryManager(*socket, *sysCat);
}