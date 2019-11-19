#include <hubDB/DBQueryMgr.h>
#include <hubDB/DBTable.h>
#include <hubDB/DBIndex.h>
#include <hubDB/DBException.h>

using namespace HubDB::Exception;
using namespace HubDB::Manager;
using namespace HubDB::Table;
using namespace HubDB::Index;

LoggerPtr DBQueryMgr::logger(Logger::getLogger("HubDB.Query.DBQueryMgr"));

void parse(DBQueryMgr &queryMgr);

DBQueryMgr::DBQueryMgr(DBServerSocket &socket, DBSysCatMgr &sysCatMgr) :
        socket(socket),
        sysCatMgr(sysCatMgr),
        connectDB(""),
        isConnected(false),
        sysCatHdl(NULL) {
  if (logger != NULL) LOG4CXX_INFO(logger, "DBQueryMgr()");
}

DBQueryMgr::~DBQueryMgr() {
  if (logger != NULL) {
    LOG4CXX_INFO(logger, "~DBQueryMgr()");
    LOG4CXX_DEBUG(logger, "this:\n" + toString("\t"));
  }
  if (isConnected)
    sysCatMgr.disconnect(sysCatHdl);
}

string DBQueryMgr::toString(string linePrefix) const {
  stringstream ss;
  ss << linePrefix << "[DBQueryManager]" << endl;
  ss << linePrefix << "connectDB: " << connectDB << endl;
  ss << linePrefix << "isConnected: " << isConnected << endl;
  ss << linePrefix << "sysCatHdl: " << sysCatHdl << endl;
  ss << linePrefix << "socket:\n" << socket.toString(linePrefix + "\t");
  ss << linePrefix << "sysCatMgr:\n" << sysCatMgr.toString(linePrefix + "\t");
  ss << linePrefix << "----------------" << endl;
  return ss.str();
}

void DBQueryMgr::process() {
  LOG4CXX_INFO(logger, "process()");
  try {
    parse(*this);
  } catch (DBException e) {
    LOG4CXX_ERROR(logger, e.what());
    socket.getWriteStream() << e.what() << endl;
  }
}

void DBQueryMgr::createDB(char *dbName) {
  LOG4CXX_INFO(logger, "createDB()");
  LOG4CXX_DEBUG(logger, (string) "dbName: " + dbName);
  try {
    sysCatMgr.createDB(dbName);
    socket.getWriteStream() << "DB created" << endl;
  } catch (DBException e) {
    LOG4CXX_ERROR(logger, e.what());
    socket.getWriteStream() << e.what() << endl;
  }
}

void DBQueryMgr::dropDB(char *dbName) {
  LOG4CXX_INFO(logger, "dropDB()");
  LOG4CXX_DEBUG(logger, (string) "dbName: " + dbName);
  try {
    if (strcmp(dbName, connectDB.c_str()) == 0)
      disconnect();
    sysCatMgr.dropDB(dbName);
    socket.getWriteStream() << "DB dropped" << endl;
  } catch (DBException e) {
    LOG4CXX_ERROR(logger, e.what());
    socket.getWriteStream() << e.what() << endl;
  }
}

void DBQueryMgr::connectTo(char *dbName) {
  LOG4CXX_INFO(logger, "connectTo()");
  LOG4CXX_DEBUG(logger, (string) "dbName: " + dbName);
  try {
    if (isConnected)
      disconnect();
    connectDB = dbName;
    isConnected = true;
    sysCatHdl = sysCatMgr.connectTo(connectDB);
    LOG4CXX_DEBUG(logger, "sysCatHdl: " + TO_STR(sysCatHdl));
    socket.getWriteStream() << "Connected" << endl;
  } catch (DBException e) {
    isConnected = false;
    sysCatHdl = NULL;
    LOG4CXX_ERROR(logger, e.what());
    socket.getWriteStream() << e.what() << endl;
  }
}

void DBQueryMgr::disconnect() {
  LOG4CXX_INFO(logger, "disconnect()");
  try {
    LOG4CXX_DEBUG(logger, "isConnected: " + TO_STR(isConnected));
    LOG4CXX_DEBUG(logger, "sysCatHdl: " + TO_STR(sysCatHdl));
    if (isConnected) {
      isConnected = false;
      sysCatMgr.disconnect(sysCatHdl);
      sysCatHdl = NULL;
    }
    socket.getWriteStream() << "Disconnected" << endl;
  } catch (DBException e) {
    LOG4CXX_ERROR(logger, e.what());
    socket.getWriteStream() << e.what() << endl;
  }
}

void DBQueryMgr::listTables() {
  LOG4CXX_INFO(logger, "listTables()");
  try {
    if (!isConnected)
      throw DBQueryMgrNoConnectionException();
    list<string> tables;
    sysCatMgr.listTables(connectDB, tables);
    list<string>::iterator i = tables.begin();

    socket.getWriteStream() << "-------------- TABNAME --------------" << endl;
    while (i != tables.end()) {
      socket.getWriteStream() << *i << endl;
      i++;
    }
    socket.getWriteStream() << "   " << tables.size() << " row(s) selected." << endl;
  } catch (DBException e) {
    LOG4CXX_ERROR(logger, e.what());
    socket.getWriteStream() << e.what() << endl;
  }
}

void DBQueryMgr::getSchemaForTable(char *table) {
  LOG4CXX_INFO(logger, "getSchemaForTable()");
  LOG4CXX_DEBUG(logger, (string) "table: " + table);

  try {
    if (!isConnected)
      throw DBQueryMgrNoConnectionException();

    DBRelDef def = sysCatMgr.getSchemaForTable(connectDB, table);
    socket.getWriteStream() << "-ATTRNAME-\t-ATTRPOS-\t-ATTRTYPE-\t-ATTRLEN-\t-ISUNIQUE-\t-ISINDEXED-\t-INDEXTYPE-"
                            << endl;
    for (uint i = 0; i < def.attrCnt(); ++i) {
      DBAttrDef adef = def.attrDef(i);
      socket.getWriteStream() << adef.attrName() << "\t";
      socket.getWriteStream() << adef.attrPos() << "\t";
      socket.getWriteStream() << DBAttrType::getName4Type(adef.attrType()) << "\t";
      socket.getWriteStream() << adef.attrLen() << "\t";
      socket.getWriteStream() << boolalpha << adef.isUnique() << "\t";
      socket.getWriteStream() << boolalpha << adef.isIndexed() << "\t";
      socket.getWriteStream() << adef.indexType() << endl;
    }
    socket.getWriteStream() << "   " << def.attrCnt() << " row(s) selected." << endl;
  } catch (DBException e) {
    LOG4CXX_ERROR(logger, e.what());
    socket.getWriteStream() << e.what() << endl;
  }
}

void DBQueryMgr::createTable(const RelDefStruct &def) {
  LOG4CXX_INFO(logger, "createTable()");

  try {
    if (!isConnected)
      throw DBQueryMgrNoConnectionException();
    sysCatMgr.createTable(connectDB, def);
    socket.getWriteStream() << "Table created" << endl;
  } catch (DBException e) {
    LOG4CXX_ERROR(logger, e.what());
    socket.getWriteStream() << e.what() << endl;
  }
}

void DBQueryMgr::dropTable(char *table) {
  LOG4CXX_INFO(logger, "dropTable()");
  LOG4CXX_DEBUG(logger, (string) "table: " + table);

  try {
    if (!isConnected)
      throw DBQueryMgrNoConnectionException();
    sysCatMgr.dropTable(connectDB, table);
    socket.getWriteStream() << "Table dropped" << endl;
  } catch (DBException e) {
    LOG4CXX_ERROR(logger, e.what());
    socket.getWriteStream() << e.what() << endl;
  }
}

void DBQueryMgr::createIndex(const QualifiedName &qname, char *type) {
  LOG4CXX_INFO(logger, "createIndex()");
  LOG4CXX_DEBUG(logger, (string) "qname: " + qname.toString());
  LOG4CXX_DEBUG(logger, (string) "type: " + type);

  DBTable *table = NULL;
  DBIndex *index = NULL;

  try {
    LOG4CXX_DEBUG(logger, "create index");
    if (!isConnected)
      throw DBQueryMgrNoConnectionException();
    sysCatMgr.createIndex(connectDB, qname, type);

    LOG4CXX_DEBUG(logger, "open table");
    table = sysCatMgr.openTable(connectDB, qname.relationName, READ);

    LOG4CXX_DEBUG(logger, "open index");
    index = sysCatMgr.openIndex(connectDB, qname, WRITE);

    const DBRelDef &relDef = table->getRelDef();
    uint pos = relDef.attrCnt();
    for (uint i = 0; i < relDef.attrCnt(); ++i) {
      DBAttrDef attr = relDef.attrDef(i);
      if (attr.attrName().compare(qname.attributeName) == 0) {
        pos = i;
        break;
      }
    }

    if (pos == relDef.attrCnt())
      throw DBException("invalid attrname");

    DBListTuple l;
    TID t;
    t.page = 0;
    t.slot = 0;
    do {
      l.clear();
      t = table->readSeqFromTID(t, 100, l);
      LOG4CXX_DEBUG(logger, "read " + TO_STR(l.size()) + "tuple");
      DBListTuple::iterator i = l.begin();
      while (l.end() != i) {
        LOG4CXX_DEBUG(logger, "val:\n" + (*i).getAttrVal(pos).toString("\t"));
        LOG4CXX_DEBUG(logger, "tid: " + (*i).getTID().toString());
        index->insert((*i).getAttrVal(pos), (*i).getTID());
        ++i;
      }
    } while (l.size() == 100);
    socket.getWriteStream() << "Index created" << endl;
  } catch (DBException e) {
    LOG4CXX_ERROR(logger, e.what());
    socket.getWriteStream() << e.what() << endl;
  }
  if (index != NULL)
    delete index;
  if (table != NULL)
    delete table;
}

void DBQueryMgr::dropIndex(const QualifiedName &qname) {
  LOG4CXX_INFO(logger, "dropIndex()");
  LOG4CXX_DEBUG(logger, (string) "qname: " + qname.toString());

  try {
    if (!isConnected)
      throw DBQueryMgrNoConnectionException();
    sysCatMgr.dropIndex(connectDB, qname);
    socket.getWriteStream() << "Index dropped" << endl;
  } catch (DBException &e) {
    LOG4CXX_ERROR(logger, e.what());
    socket.getWriteStream() << e.what() << endl;
  }
}

bool DBQueryMgr::insertInto(char *tableName, DBTuple *value, bool print) {
  LOG4CXX_INFO(logger, "insertInto()");
  LOG4CXX_DEBUG(logger, (string) "tableName: " + tableName);
  LOG4CXX_DEBUG(logger, (string) "value: " + value->toString());

  DBTable *table = NULL;
  DBIndex *index = NULL;
  bool undo = false;
  bool rc = true;
  try {
    if (!isConnected)
      throw DBQueryMgrNoConnectionException();

    LOG4CXX_DEBUG(logger, "open table");
    table = sysCatMgr.openTable(connectDB, tableName, WRITE);

    const DBRelDef &relDef = table->getRelDef();
    for (uint i = 0; i < relDef.attrCnt(); ++i) {
      DBAttrDef attr = relDef.attrDef(i);
      if (value->getAttrVal(i).type() != attr.attrType())
        throw DBQueryMgrException("Type missmatch");
    }
    LOG4CXX_DEBUG(logger, "insert val in table values:\n" + value->toString("\t"));
    table->insert(*value);
    LOG4CXX_DEBUG(logger, "values:\n" + value->toString("\t"));
    undo = true;
    for (uint i = 0; i < relDef.attrCnt(); ++i) {
      DBAttrDef attr = relDef.attrDef(i);
      if (attr.isIndexed()) {
        QualifiedName qname;
        strcpy(qname.relationName, tableName);
        strcpy(qname.attributeName, attr.attrName().c_str());
        LOG4CXX_DEBUG(logger, "open index");
        index = sysCatMgr.openIndex(connectDB, qname, WRITE);
        LOG4CXX_DEBUG(logger, "val:\n" + value->getAttrVal(i).toString("\t"));
        LOG4CXX_DEBUG(logger, "tid: " + value->getTID().toString());

        index->insert(value->getAttrVal(i), value->getTID());
        LOG4CXX_DEBUG(logger, "delete index");
        delete index;
        index = NULL;
      }
    }
    uint cnt = 1;
    if (print == true)
      socket.getWriteStream() << "   " << cnt << " row(s) inserted." << endl;
  } catch (DBException &e) {
    if (undo == true) {
      DBListTID l;
      l.push_back(value->getTID());
      table->remove(l);
    }
    LOG4CXX_ERROR(logger, e.what());
    socket.getWriteStream() << e.what() << endl;
    rc = false;
  }
  LOG4CXX_DEBUG(logger, "delete index:" + TO_STR(index));
  if (index != NULL)
    delete index;
  LOG4CXX_DEBUG(logger, "delete table:" + TO_STR(table));
  if (table != NULL)
    delete table;
  return rc;
}

void DBQueryMgr::deleteFromTable(char *tableName, DBListPredicate *where) {
  LOG4CXX_INFO(logger, "deleteFromTable()");
  LOG4CXX_DEBUG(logger, (string) "tableName: " + tableName);

  DBTable *table = NULL;
  DBIndex *index = NULL;
  try {
    if (!isConnected)
      throw DBQueryMgrNoConnectionException();

    LOG4CXX_DEBUG(logger, "open table");
    table = sysCatMgr.openTable(connectDB, tableName, WRITE);

    const DBRelDef &relDef = table->getRelDef();
    DBListTuple tupleList;

    LOG4CXX_DEBUG(logger, "select tuples");
    DBListPredicate tmp;

    if (where == NULL)
      where = &tmp;
    selectTuple(table, *where, tupleList);

    DBListTID l;
    DBListTuple::iterator i = tupleList.begin();
    while (i != tupleList.end()) {
      l.push_back(i->getTID());
      ++i;
    }

    if (tupleList.begin() != tupleList.end()) {
      for (uint u = 0; u < relDef.attrCnt(); ++u) {
        DBAttrDef attr = relDef.attrDef(u);
        if (attr.isIndexed()) {
          QualifiedName qname;
          strcpy(qname.relationName, tableName);
          strcpy(qname.attributeName, attr.attrName().c_str());

          bool f = false;
          if (where != NULL) {
            DBListPredicate::iterator pi = where->begin();
            while (pi != where->end()) {
              if (strcmp(pi->name().toString().c_str(), qname.toString().c_str()) == 0) {
                f = true;
                break;
              }
              ++pi;
            }
          }
          if (f == true) {
            LOG4CXX_DEBUG(logger, "open index");
            index = sysCatMgr.openIndex(connectDB, qname, WRITE);
            DBListTuple::iterator i = tupleList.begin();
            index->remove(i->getAttrVal(u), l);
            delete index;
            index = NULL;
          } else {
            if (where == NULL) {
              string type = attr.indexType();
              dropIndex(qname);
              LOG4CXX_DEBUG(logger, "create index");
              sysCatMgr.createIndex(connectDB, qname, type);
            } else {
              LOG4CXX_DEBUG(logger, "open index");
              index = sysCatMgr.openIndex(connectDB, qname, WRITE);
              DBListTuple::iterator i = tupleList.begin();
              while (i != tupleList.end()) {
                DBListTID lTmp;
                lTmp.push_back(i->getTID());
                index->remove(i->getAttrVal(u), lTmp);
                ++i;
              }
              delete index;
              index = NULL;
            }
          }
        }
      }
    }
    uint cnt = l.size();
    table->remove(l);
    socket.getWriteStream() << "   " << cnt << " row(s) deleted." << endl;
  } catch (DBException &e) {
    LOG4CXX_ERROR(logger, e.what());
    socket.getWriteStream() << e.what() << endl;
  }
  if (index != NULL)
    delete index;
  if (table != NULL)
    delete table;
}

void DBQueryMgr::importTab(char *fileName, char *table) {
  LOG4CXX_INFO(logger, "importTab()");
  LOG4CXX_DEBUG(logger, (string) "fileName: " + fileName);
  LOG4CXX_DEBUG(logger, (string) "table: " + table);
  FILE *file = NULL;
  try {
    if (!isConnected)
      throw DBQueryMgrNoConnectionException();
    const DBRelDef &def = sysCatMgr.getSchemaForTable(connectDB, table);
    file = fopen(fileName, "r");
    if (file == NULL)
      throw DBSystemException(errno);

    LOG4CXX_DEBUG(logger, (string) "file is open");
    DataImport data[MAX_ATTR_PER_REL];
    void *ptr[MAX_ATTR_PER_REL];
    uint cnt = def.attrCnt();

    char iline[100];
    iline[0] = '\0';

    for (uint i = 0; i < cnt; ++i) {
      if (i > 0)
        strcat(iline, "\t");
      switch (def.attrDef(i).attrType()) {
        case INT:
          strcat(iline, "%d");
          ptr[i] = &data[i].i;
          break;
        case DOUBLE:
          strcat(iline, "%lg");
          ptr[i] = &data[i].d;
          break;
        case VCHAR:
          strcat(iline, "%32s");
          ptr[i] = &data[i].c[0];
          break;
        default:
          throw DBException("unknow attribute type " + def.attrDef(i).attrType());
      }
    }
    LOG4CXX_DEBUG(logger, iline);

    errno = 0;
    int rc = 0;
    uint rcnt = 0, gcnt = 0;

    while (cnt ==
           (rc = fscanf(file, iline, ptr[0], ptr[1], ptr[2], ptr[3], ptr[4], ptr[5], ptr[6], ptr[7], ptr[8], ptr[9]))) {
      DBTuple *t = new DBTuple();
      for (uint i = 0; i < cnt; ++i) {
        DBAttrType *attr = DBAttrType::import(data[i], def.attrDef(i).attrType());
        t->appendAttrVal(attr);
        delete attr;
      }
      if (insertInto(table, t, false) == true)
        ++rcnt;
      delete t;
      ++gcnt;
    }
    if (errno != 0) // for scanf
      throw DBSystemException(errno);
    socket.getWriteStream() << "   " << rcnt << " from " << gcnt << " row(s) imported." << endl;
  } catch (DBException &e) {
    LOG4CXX_ERROR(logger, e.what());
    socket.getWriteStream() << e.what() << endl;
  }
  if (file != NULL)
    fclose(file);
}

void DBQueryMgr::exportTab(char *relName, char *fileName) {
  LOG4CXX_INFO(logger, "exportTab()");
  LOG4CXX_DEBUG(logger, (string) "fileName: " + fileName);
  LOG4CXX_DEBUG(logger, (string) "relName: " + relName);
  DBTable *table = NULL;
  FILE *file = NULL;

  try {
    if (!isConnected)
      throw DBQueryMgrNoConnectionException();
    table = sysCatMgr.openTable(connectDB, relName, READ);
    const DBRelDef &def = table->getRelDef();
    file = fopen(fileName, "a");
    if (file == NULL)
      throw DBSystemException(errno);

    DBListTuple lsmall;
    DBListPredicate where;
    selectTuple(table, where, lsmall);

    LOG4CXX_DEBUG(logger, "lsmall.size(): " + TO_STR(lsmall.size()));
    DBListTuple::iterator iS = lsmall.begin();

    while (lsmall.end() != iS) {
      DBTuple &tuple = *iS;
      LOG4CXX_DEBUG(logger, "iS: \n" + tuple.toString("\t"));
      for (uint i = 0; i < def.attrCnt(); ++i) {
        if (i > 0)
          fprintf(file, "\t");
        tuple.getAttrVal(i).exportToFile(file);
      }
      fprintf(file, "\n");
      ++iS;
    }
    LOG4CXX_DEBUG(logger, "done export");
    socket.getWriteStream() << "Table exported." << endl;
  } catch (DBException &e) {
    LOG4CXX_ERROR(logger, e.what());
    socket.getWriteStream() << e.what() << endl;
  }
  if (file != NULL)
    fclose(file);
  if (table != NULL)
    delete table;
}

void DBQueryMgr::select(DBListQualifiedName *projection, char *relName, DBJoin *join, DBListPredicate *where) {
  LOG4CXX_INFO(logger, "select()");

  DBTable *table[2] = {NULL, NULL};
  DBRelDef *def[2] = {NULL, NULL};
  uint joinAttrs[2] = {(uint) -1, (uint) -1};
  list<pair<uint, uint> > proList;
  const uint left = 0;
  const uint right = 1;
  DBListPredicate whereJoin[2];
  list<string> proStr;
  DBListJoinTuple l;

  try {
    if (!isConnected)
      throw DBQueryMgrNoConnectionException();

    LOG4CXX_DEBUG(logger, "open table left");
    table[left] = sysCatMgr.openTable(connectDB, relName, READ);
    def[left] = new DBRelDef(table[left]->getRelDef());
    joinAttrs[left] = def[left]->attrCnt();

    LOG4CXX_DEBUG(logger, "check join");
    if (join != NULL) {
      LOG4CXX_DEBUG(logger, "join: " + join->toString());
      LOG4CXX_DEBUG(logger, "open table right");
      table[right] = sysCatMgr.openTable(connectDB, join->relationName, READ);
      def[right] = new DBRelDef(table[right]->getRelDef());
      joinAttrs[right] = def[right]->attrCnt();

      for (uint c = 0; c < 2; ++c) {
        if (joinAttrs[left] == def[left]->attrCnt() &&
            def[left]->relationName().compare(join->attrNames[c].relationName) == 0) {
          joinAttrs[left] = def[left]->attrDef(join->attrNames[c].attributeName).attrPos();
        } else if (joinAttrs[right] == def[right]->attrCnt() &&
                   def[right]->relationName().compare(join->attrNames[c].relationName) == 0) {
          joinAttrs[right] = def[right]->attrDef(join->attrNames[c].attributeName).attrPos();
        } else {
          throw DBException((string) "invalid join relation " + join->attrNames[c].relationName);
        }
      }
      if (def[right]->attrDef(joinAttrs[right]).attrType() != def[left]->attrDef(joinAttrs[left]).attrType()) {
        throw DBException("join on different attribute types");
      }
    }

    LOG4CXX_DEBUG(logger, "check projection");
    if (projection != NULL) {
      DBListQualifiedName::iterator i = projection->begin();
      while (i != projection->end()) {
        pair<uint, uint> p;
        if (def[left]->relationName().compare((*i).relationName) == 0) {
          p.first = left;
          p.second = def[left]->attrDef((*i).attributeName).attrPos();
        } else if (join != NULL && def[right]->relationName().compare((*i).relationName) == 0) {
          p.first = right;
          p.second = def[right]->attrDef((*i).attributeName).attrPos();
        } else {
          throw DBException((string) "invalid projection " + (*i).toString());
        }
        proList.push_back(p);
        ++i;
      }
    } else {
      pair<uint, uint> p;
      p.first = left;
      for (uint i = 0; i < def[left]->attrCnt(); ++i) {
        p.second = i;
        proList.push_back(p);
      }
      if (join != NULL) {
        p.first = right;
        for (uint i = 0; i < def[right]->attrCnt(); ++i) {
          p.second = i;
          proList.push_back(p);
        }
      }

    }

    LOG4CXX_DEBUG(logger, "check where " + TO_STR(where));
    if (where != NULL) {
      LOG4CXX_DEBUG(logger, "where: " + TO_STR(*where));
      DBListPredicate::iterator i = where->begin();
      while (where->end() != i) {
        LOG4CXX_DEBUG(logger, "check predicate " + (*i).toString());

        if (def[left]->relationName().compare((*i).name().relationName) == 0) {
          if (def[left]->attrDef((*i).name().attributeName).attrType() !=
              (*i).val().type()) {
            throw DBQueryMgrException("Type missmatch");
          }
          LOG4CXX_DEBUG(logger, "left predicate " + (*i).toString());
          whereJoin[left].push_back((*i));
        } else if (join != NULL && def[right]->relationName().compare((*i).name().relationName) == 0) {
          if (def[right]->attrDef((*i).name().attributeName).attrType() !=
              (*i).val().type()) {
            throw DBQueryMgrException("Type missmatch");
          }
          LOG4CXX_DEBUG(logger, "right predicate" + (*i).toString());
          whereJoin[right].push_back((*i));
        } else {
          throw DBException((string) "invalid predicate " + (*i).toString());
        }
        ++i;
      }
    }


    LOG4CXX_DEBUG(logger, "select");

    if (join != NULL) {
      selectJoinTuple(table, joinAttrs, whereJoin, l);
      LOG4CXX_DEBUG(logger, "l.size(): " + TO_STR(l.size()));
    } else {
      DBTuple empty;
      DBListTuple lsmall;
      selectTuple(table[left], whereJoin[left], lsmall);

      LOG4CXX_DEBUG(logger, "lsmall.size(): " + TO_STR(lsmall.size()));
      DBListTuple::iterator iS = lsmall.begin();

      while (lsmall.end() != iS) {

        pair<DBTuple, DBTuple> p;
        p.first = DBTuple(*iS);
        LOG4CXX_DEBUG(logger, "iS: \n" + (*iS).toString("\t"));
        p.second = DBTuple();
        l.push_back(p);
        ++iS;
      }
      LOG4CXX_DEBUG(logger, "done copy");
    }

    list<pair<uint, uint> >::iterator pI = proList.begin();
    while (pI != proList.end()) {
      pair<uint, uint> p = (*pI);
      LOG4CXX_DEBUG(logger, "p.first " + TO_STR(p.first));
      LOG4CXX_DEBUG(logger, "p.second " + TO_STR(p.second));
      socket.getWriteStream() << setw(MAX_STR_LEN) << "-" << def[p.first]->relationName() << "."
                              << def[p.first]->attrDef(p.second).attrName() << "-" << "\t";
      ++pI;
    }
    socket.getWriteStream() << endl;

    uint cnt = l.size();
    while (l.empty() == false) {
      pI = proList.begin();
      while (pI != proList.end()) {
        pair<uint, uint> p = (*pI);
        LOG4CXX_DEBUG(logger, "p.first " + TO_STR(p.first));
        LOG4CXX_DEBUG(logger, "p.second " + TO_STR(p.second));

        if (p.first == left) {
          l.front().first.getAttrVal(p.second).print(socket.getWriteStream());
        } else {
          l.front().second.getAttrVal(p.second).print(socket.getWriteStream());
        }
        socket.getWriteStream() << "\t";
        ++pI;
      }
      l.pop_front();
      socket.getWriteStream() << endl;
    }
    socket.getWriteStream() << "   " << cnt << " row(s) selected." << endl;
  } catch (DBException e) {
    LOG4CXX_ERROR(logger, e.what());
    socket.getWriteStream() << e.what() << endl;
  }
  for (uint i = 0; i < 2; ++i) {
    LOG4CXX_DEBUG(logger, "free table[" + TO_STR(i) + "]:" + TO_STR(table[i]));
    if (table[i] != NULL)
      delete table[i];
    LOG4CXX_DEBUG(logger, "free def[" + TO_STR(i) + "]:" + TO_STR(def[i]));
    if (def[i] != NULL)
      delete def[i];
  }
}

DBQueryMgrException::DBQueryMgrException(const std::string &msg1)
        : DBRuntimeException(msg1) {
}

DBQueryMgrException::DBQueryMgrException(const DBQueryMgrException &src)
        : DBRuntimeException(src) {
}

DBQueryMgrException &DBQueryMgrException::operator=(const DBQueryMgrException &src) {
  DBException::operator=(src);
  return *this;
}


DBQueryMgrNoConnectionException::DBQueryMgrNoConnectionException()
        : DBQueryMgrException("no connection") {
}

DBQueryMgrNoConnectionException::DBQueryMgrNoConnectionException(const DBQueryMgrNoConnectionException &src)
        : DBQueryMgrException(src) {
}

DBQueryMgrNoConnectionException &
DBQueryMgrNoConnectionException::operator=(const DBQueryMgrNoConnectionException &src) {
  DBException::operator=(src);
  return *this;
}
