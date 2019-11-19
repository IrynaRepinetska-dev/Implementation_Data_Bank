#include <hubDB/DBSimpleQueryMgr.h>
#include <hubDB/DBTable.h>
#include <hubDB/DBIndex.h>
#include <hubDB/DBException.h>

using namespace HubDB::Exception;
using namespace HubDB::Manager;
using namespace HubDB::Table;
using namespace HubDB::Index;

LoggerPtr DBSimpleQueryMgr::logger(Logger::getLogger("HubDB.Query.DBSimpleQueryMgr"));

int rSimpleQMgr = DBSimpleQueryMgr::registerClass();

extern "C" void *createDBSimpleQueryMgr(int nArgs, va_list ap);

int DBSimpleQueryMgr::registerClass() {
  setClassForName("DBSimpleQueryMgr", createDBSimpleQueryMgr);
  return 0;
}

extern "C" void *createDBSimpleQueryMgr(int nArgs, va_list ap) {
  if (nArgs != 2)
    throw DBException("Invalid number of arguments");
  DBServerSocket *socket = va_arg(ap, DBServerSocket *);
  DBSysCatMgr *sysCat = va_arg(ap, DBSysCatMgr *);
  return new DBSimpleQueryMgr(*socket, *sysCat);
}

DBSimpleQueryMgr::DBSimpleQueryMgr(DBServerSocket &socket, DBSysCatMgr &sysCatMgr) :
        DBQueryMgr(socket, sysCatMgr) {
  if (logger != NULL) LOG4CXX_INFO(logger, "DBSimpleQueryMgr()");
}

string DBSimpleQueryMgr::toString(string linePrefix) const {
  stringstream ss;
  ss << linePrefix << "[DBSimpleQueryManager]" << endl;
  ss << linePrefix << "----------------" << endl;
  return ss.str();
}

void DBSimpleQueryMgr::selectJoinTuple(DBTable *table[2],
                                       uint attrJoinPos[2],
                                       DBListPredicate where[2],
                                       DBListJoinTuple &tuples) {
  LOG4CXX_INFO(logger, "selectJoinTuple()");

  DBListTuple l[2];
  for (uint i = 0; i < 2; ++i) {
    selectTuple(table[i], where[i], l[i]);
  }
  DBListTuple::iterator i = l[0].begin();
  while (i != l[0].end()) {
    DBTuple &left = (*i);

    DBListTuple::iterator u = l[1].begin();
    while (u != l[1].end()) {
      DBTuple &right = (*u);

      if (left.getAttrVal(attrJoinPos[0]) == right.getAttrVal(attrJoinPos[1])) {
        LOG4CXX_DEBUG(logger, "left:\n" + left.toString("\t"));
        LOG4CXX_DEBUG(logger, "right:\n" + right.toString("\t"));
        pair<DBTuple, DBTuple> p;
        p.first = left;
        p.second = right;
        tuples.push_back(p);
      }
      ++u;
    }
    ++i;
  }
}

void DBSimpleQueryMgr::selectTuple(DBTable *table, DBListPredicate &where, DBListTuple &tuple) {
  LOG4CXX_INFO(logger, "selectTuple()");
  LOG4CXX_DEBUG(logger, "table:\n" + table->toString("\t"));
  LOG4CXX_DEBUG(logger, "where: " + TO_STR(where));

  DBListTuple l;
  TID t;
  t.page = 0;
  t.slot = 0;
  list<uint> posList;
  list<bool> checkList;
  DBListTID tidList;
  const DBRelDef &def = table->getRelDef();
  QualifiedName qname;
  bool indexUsed = false;

  strcpy(qname.relationName, def.relationName().c_str());

  DBListPredicate::iterator u = where.begin();
  while (u != where.end()) {
    DBPredicate &p = *u;
    if (strcmp(def.relationName().c_str(), p.name().relationName) != 0)
      throw DBQueryMgrException("Predicate missmatch");
    DBAttrDef adef = def.attrDef(p.name().attributeName);
    if (adef.isIndexed() == true) {
      checkList.push_back(false);
      strcpy(qname.attributeName, adef.attrName().c_str());
      DBListTID tidListTmp;
      DBIndex *index = NULL;
      try {
        index = sysCatMgr.openIndex(connectDB, qname, READ);
        if (indexUsed == true) {
          index->find(p.val(), tidListTmp);
          tidListTmp.sort();
        } else {
          index->find(p.val(), tidList);
          tidList.sort();
        }
        delete index;
      } catch (DBException e) {
        if (index != NULL)
          delete index;
        throw e;
      }
      if (indexUsed == true) {
        DBListTID tidListNew;
        set_intersection(tidList.begin(), tidList.end(), tidListTmp.begin(), tidListTmp.end(),
                         std::inserter(tidListNew, tidListNew.begin()));
        tidList = tidListNew;
      } else {
        indexUsed = true;
      }
      LOG4CXX_DEBUG(logger, "tidList: " + TO_STR(tidList));
      if (tidList.size() == 0)
        break;
    } else {
      checkList.push_back(true);
    }
    posList.push_back(adef.attrPos());
    ++u;
  }

  do {
    l.clear();
    if (indexUsed == true) {
      table->readTIDs(tidList, l);
    } else {
      t = table->readSeqFromTID(t, 100, l);
    }
    LOG4CXX_DEBUG(logger, "read " + TO_STR(l.size()) + " tuples");
    DBListTuple::iterator i = l.begin();
    while (l.end() != i) {
      bool match = true;
      u = where.begin();
      list<uint>::iterator ii = posList.begin();
      list<bool>::iterator ic = checkList.begin();
      while (match == true && u != where.end()) {
        DBPredicate &p = *u;
        if (*ic == true &&
            !(p.val() == (*i).getAttrVal(*ii)))
          match = false;
        ++ii;
        ++ic;
        ++u;
      }
      if (match == true) {
        LOG4CXX_DEBUG(logger, "tuple: " + (*i).toString("\t"));
        tuple.push_back(*i);
      }
      ++i;
    }
  } while (l.size() == 100 && indexUsed == false);
  LOG4CXX_DEBUG(logger, "return");
}
