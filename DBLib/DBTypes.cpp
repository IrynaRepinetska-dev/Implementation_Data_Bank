#include <hubDB/DBTypes.h>
#include <hubDB/DBException.h>

using namespace HubDB::Types;
using namespace HubDB::Exception;

map<string, create_t *> *factory(NULL);

LoggerPtr HubDB::Types::logger(Logger::getLogger("HubDB.Types"));

string DBAttrType::toString(string linePrefix) const {
  stringstream ss;
  ss << linePrefix << "[DBAttrType]" << endl;
  ss << linePrefix << "------------" << endl;
  return ss.str();
}

DBAttrType *DBAttrType::clone(const DBAttrType &ref) {
  switch (ref.type()) {
    case INT:
      return new DBIntType((const DBIntType &) ref);
      break;
    case DOUBLE:
      return new DBDoubleType((const DBDoubleType &) ref);
      break;
    case VCHAR:
      return new DBVCharType((const DBVCharType &) ref);
      break;
    case NONE:
      break;
  }
  throw DBException("unknow attribute type");
}

size_t DBAttrType::getSize4Type(const AttrTypeEnum t) {
  switch (t) {
    case INT:
      return DBIntType::getSize();
      break;
    case DOUBLE:
      return DBDoubleType::getSize();
      break;
    case VCHAR:
      return DBVCharType::getSize();
      break;
    case NONE:
      break;
  }
  throw DBException("unknow attribute type " + t);
}

string DBAttrType::getName4Type(const AttrTypeEnum t) {
  stringstream ss;
  switch (t) {
    case INT:
      ss << "Integer";
      break;
    case DOUBLE:
      ss << "Double";
      break;
    case VCHAR:
      ss << "VarChar(" << DBVCharType::getSize() << ")";
      break;
    case NONE:
    default:
      throw DBException("unknow attribute type " + t);
  }
  return ss.str();

}

/**
 *
 * @param ptr zeitt auf das aktuelle Tupel
 * @param t
 * @param ptrNext zeigt nach Aufruf der Methode auf das nächste Tupel
 * @return
 */
DBAttrType *DBAttrType::read(const char *ptr, const AttrTypeEnum t, const char **ptrNext) {
  switch (t) {
    case INT:
      if (ptrNext != NULL)
        *ptrNext = ptr + getSize4Type(t);
      return new DBIntType(ptr);
    case DOUBLE:
      if (ptrNext != NULL)
        *ptrNext = ptr + getSize4Type(t);
      return new DBDoubleType(ptr);
    case VCHAR:
      if (ptrNext != NULL)
        *ptrNext = ptr + getSize4Type(t);
      return new DBVCharType(ptr);
    default:
      throw DBException("unknow attribute type " + t);
  }
}

DBAttrType *DBAttrType::import(DataImport &ptr, const AttrTypeEnum t) {
  LOG4CXX_INFO(logger, "import");
  LOG4CXX_DEBUG(logger, DBAttrType::getName4Type(t));
  char val[MAX_STR_LEN + 1];
  uint l;
  switch (t) {
    case INT: LOG4CXX_DEBUG(logger, TO_STR(ptr.i));
      return new DBIntType(ptr.i);
    case DOUBLE: LOG4CXX_DEBUG(logger, TO_STR(ptr.d));
      return new DBDoubleType(ptr.d);
    case VCHAR: LOG4CXX_DEBUG(logger, ptr.c);
      l = strlen(ptr.c);
      if (l == 0 || !(ptr.c[0] == '\'' && ptr.c[l - 1] == '\''))
        throw DBException((string) "invalid import string: " + ptr.c + " " + TO_STR(l));
      ptr.c[l - 1] = '\0';
      strcpy(val, &ptr.c[1]);
      return new DBVCharType(val);
    default:
      throw DBException("unknow attribute type " + t);
  }
}

string DBIntType::toString(string linePrefix) const {
  stringstream ss;
  ss << linePrefix << "[DBIntType]" << endl;
  ss << DBAttrType::toString(linePrefix + "\t") << endl;
  ss << linePrefix << "val: " << val << endl;
  ss << linePrefix << "-----------" << endl;
  return ss.str();
}

ostream &DBIntType::print(ostream &s, bool align) const {
  if (align)
    s << setw(MAX_STR_LEN) << val;
  else
    s << val;
  return s;
}

bool DBIntType::operator==(const DBAttrType &ref) const {
  if (typeid(ref) == typeid(*this)) {
    DBIntType &i = (DBIntType &) ref;
    return i.val == val ? true : false;
  }
  return false;
}

bool DBIntType::operator>(const DBAttrType &ref) const {
  if (typeid(ref) == typeid(*this)) {
    DBIntType &i = (DBIntType &) ref;
    return i.val < val ? true : false;
  }
  return false;
}

bool DBIntType::operator<(const DBAttrType &ref) const {
  if (typeid(ref) == typeid(*this)) {
    DBIntType &i = (DBIntType &) ref;
    return i.val > val ? true : false;
  }
  return false;
}

string DBDoubleType::toString(string linePrefix) const {
  stringstream ss;
  ss << linePrefix << "[DBDoubleType]" << endl;
  ss << DBAttrType::toString(linePrefix + "\t") << endl;
  ss << linePrefix << "val: " << val << endl;
  ss << linePrefix << "-----------" << endl;
  return ss.str();
}

ostream &DBDoubleType::print(ostream &s, bool align) const {
  if (align)
    s << setw(MAX_STR_LEN) << val;
  else
    s << val;
  return s;
}

bool DBDoubleType::operator==(const DBAttrType &ref) const {
  if (typeid(ref) == typeid(*this)) {
    DBDoubleType &i = (DBDoubleType &) ref;
    return i.val == val ? true : false;
  }
  return false;
}

bool DBDoubleType::operator>(const DBAttrType &ref) const {
  if (typeid(ref) == typeid(*this)) {
    DBDoubleType &i = (DBDoubleType &) ref;
    return i.val < val ? true : false;
  }
  return false;
}

bool DBDoubleType::operator<(const DBAttrType &ref) const {
  if (typeid(ref) == typeid(*this)) {
    DBDoubleType &i = (DBDoubleType &) ref;
    return i.val > val ? true : false;
  }
  return false;
}

string DBVCharType::toString(string linePrefix) const {
  stringstream ss;
  ss << linePrefix << "[DBVCharType]" << endl;
  ss << DBAttrType::toString(linePrefix + "\t") << endl;
  ss << linePrefix << "val: " << val << endl;
  ss << linePrefix << "-----------" << endl;
  return ss.str();
}

ostream &DBVCharType::print(ostream &s, bool align) const {
  if (align)
    s << setw(MAX_STR_LEN) << val;
  else
    s << val;
  return s;
}

bool DBVCharType::operator==(const DBAttrType &ref) const {
  if (typeid(ref) == typeid(*this)) {
    DBVCharType &i = (DBVCharType &) ref;
    int rc = strcmp(val, i.val);
    return rc == 0 ? true : false;
  }
  return false;
}

bool DBVCharType::operator>(const DBAttrType &ref) const {
  if (typeid(ref) == typeid(*this)) {
    DBVCharType &i = (DBVCharType &) ref;
    int rc = strcmp(val, i.val);
    return rc > 0 ? true : false;
  }
  return false;
}

bool DBVCharType::operator<(const DBAttrType &ref) const {
  if (typeid(ref) == typeid(*this)) {
    DBVCharType &i = (DBVCharType &) ref;
    int rc = strcmp(val, i.val);
    return rc < 0 ? true : false;
  }
  return false;
}

string DBAttrDef::toString(string linePrefix) const {
  stringstream ss;
  ss << linePrefix << "[DBAttrDef]" << endl;
  ss << linePrefix << "name: " << attrName() << endl;
  ss << linePrefix << "pos: " << attrPos() << endl;
  ss << linePrefix << "type: " << attrType() << endl;
  ss << linePrefix << "len: " << attrLen() << endl;
  ss << linePrefix << "unique: " << boolalpha << isUnique() << endl;
  ss << linePrefix << "idx: " << boolalpha << isIndexed() << endl;
  if (isIndexed())
    ss << linePrefix << "idxType: " << indexType() << endl;
  ss << linePrefix << "-----------" << endl;
  return ss.str();
}

DBAttrDef DBRelDef::attrDef(string attrName) const {
  for (uint i = 0; i < attrCnt(); ++i) {
    if (strcmp(relation.attrList[i].attrName, attrName.c_str()) == 0)
      return DBAttrDef(&relation.attrList[i]);
  }
  throw DBOutOfBoundException("invalid argument");
}

DBAttrDef DBRelDef::attrDef(uint pos) const {
  if (pos >= attrCnt())
    throw DBOutOfBoundException("invalid pos");
  return DBAttrDef(&relation.attrList[pos]);
}

enum AttrTypeEnum DBRelDef::attrTypeDef(uint pos) const {
  if (pos >= attrCnt())
    throw DBOutOfBoundException("invalid pos");
  return relation.attrList[pos].attrType;
}

string DBRelDef::toString(string linePrefix) const {
  stringstream ss;
  ss << linePrefix << "[DBRelDef]" << endl;
  ss << linePrefix << "name: " << relationName() << endl;
  ss << linePrefix << "attrCnt: " << attrCnt() << endl;
  ss << linePrefix << "tupleSize: " << tupleSize() << endl;
  for (uint i = 0; i < attrCnt(); ++i) {
    ss << attrDef(i).toString(linePrefix + "\t");
  }
  ss << linePrefix << "----------" << endl;
  return ss.str();
}

DBTuple::DBTuple(const DBTuple &ref) {
  LOG4CXX_DEBUG(logger, "DBTuple()");
  LOG4CXX_DEBUG(logger, "ref:\n" + ref.toString("\t"));
  tid = ref.tid;
  vector<DBAttrType *>::const_iterator i = ref.vals.begin();
  LOG4CXX_DEBUG(logger, "vals.size: " + TO_STR(ref.vals.size()));
  while (i != ref.vals.end()) {
    LOG4CXX_DEBUG(logger, "i:\n" + (*i)->toString("\t"));
    vals.push_back(DBAttrType::clone(*(*i)));
    ++i;
  }
  LOG4CXX_DEBUG(logger, "this:\n" + toString("\t"));
}

DBTuple::DBTuple() :
        tid(invalidTID) {
  LOG4CXX_DEBUG(logger, "DBTuple()");
}

DBTuple::~DBTuple() {
  while (vals.empty() == false) {
    DBAttrType *val = vals.back();
    vals.pop_back();
    delete val;
  }
}

DBTuple &DBTuple::operator=(const DBTuple &ref) {
  tid = ref.tid;
  while (vals.empty() == false) {
    DBAttrType *val = vals.back();
    vals.pop_back();
    delete val;
  }
  vector<DBAttrType *>::const_iterator i = ref.vals.begin();
  LOG4CXX_DEBUG(logger, "vals.size: " + TO_STR(ref.vals.size()));
  while (i != ref.vals.end()) {
    LOG4CXX_DEBUG(logger, "i:\n" + (*i)->toString("\t"));
    vals.push_back(DBAttrType::clone(*(*i)));
    ++i;
  }
  LOG4CXX_DEBUG(logger, "this:\n" + toString("\t"));
  return *this;
}

string DBTuple::toString(string linePrefix) const {
  stringstream ss;
  ss << linePrefix << "[DBTuple]" << endl;
  ss << linePrefix << "tid: " << tid.toString() << endl;
  vector<DBAttrType *>::const_iterator i = vals.begin();
  while (i != vals.end()) {
    ss << linePrefix << "vals[]=";
    (*i)->print(ss, false);
    ss << endl;
    ++i;
  }
  ss << linePrefix << "---------" << endl;
  return ss.str();
}

const char *DBTuple::read(const DBRelDef &relDef, const char *ptr) {
  for (uint i = 0; i < relDef.attrCnt(); ++i) {
    vals.push_back(DBAttrType::read(ptr, relDef.attrTypeDef(i), &ptr));
  }
  return ptr;
}

char *DBTuple::write(char *ptr) const {
  vector<DBAttrType *>::const_iterator i = vals.begin();
  while (i != vals.end()) {
    ptr = (*i)->write(ptr);
    ++i;
  }
  return ptr;
}

void DBTuple::appendAttrVal(DBAttrType *val) {
  vals.push_back(DBAttrType::clone(*val));
}

const DBAttrType &DBTuple::getAttrVal(uint pos) const {
  if (pos >= vals.size())
    throw DBOutOfBoundException("invalid pos");
  return *vals[pos];
}

void *HubDB::Types::getClassForName(const string &name, int nArgs, ...) {
  va_list ap;
  va_start(ap, nArgs);
  if (factory == NULL || (*factory).end() == (*factory).find(name))
    throw DBClassForNameException("no such class name");
  void *ptr = (*factory)[name](nArgs, ap);
  va_end(ap);
  return ptr;
}

void HubDB::Types::setClassForName(const string &name, create_t *func) {
  if (factory == NULL) {
    factory = new map<string, create_t *>;
  }
  (*factory)[name] = func;
}

list<string> HubDB::Types::getKnownClassNames() {
  list<string> l;
  if (factory != NULL) {
    map<string, create_t *>::iterator i = (*factory).begin();
    while (i != (*factory).end()) {
      l.push_back((*i).first);
      ++i;
    }
  }
  return l;
}

string HubDB::Types::getKnownClassNames(char sep) {
  stringstream ss;
  map<string, create_t *>::iterator i = (*factory).begin();
  while (i != (*factory).end()) {
    ss << (*i).first << sep;
    ++i;
  }
  return ss.str();
}

string HubDB::Types::toString(DBListPredicate &l) {
  stringstream ss;
  DBListPredicate::iterator i = l.begin();
  while (i != l.end()) {
    ss << (*i).toString();
    ++i;
    if (i != l.end())
      ss << " AND ";
  }
  return ss.str();
}

string HubDB::Types::toString(DBListTID &l) {
  stringstream ss;
  DBListTID::iterator i = l.begin();
  while (i != l.end()) {
    ss << (*i).toString();
    ++i;
    if (i != l.end())
      ss << " | ";
  }
  return ss.str();
}
