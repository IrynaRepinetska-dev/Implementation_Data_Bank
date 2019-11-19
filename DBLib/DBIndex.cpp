#include <hubDB/DBIndex.h>

using namespace HubDB::Index;
using namespace HubDB::Exception;

LoggerPtr DBIndex::logger(Logger::getLogger("HubDB.Index.DBIndex"));

string DBIndex::toString(string linePrefix) const {
  stringstream ss;
  ss << linePrefix << "[DBIndex]" << endl;
  ss << linePrefix << "file:" << endl;
  ss << file.toString(linePrefix + "\t") << endl;
  /*
ss << linePrefix <<"bufMgr:"<<endl;
ss << bufMgr.toString(linePrefix+"\t") << endl;
  */
  ss << linePrefix << "attrType:" << attrType << endl;
  ss << linePrefix << "attrTypeSize:" << attrTypeSize << endl;
  ss << linePrefix << "mode:" << mode << endl;
  ss << linePrefix << "unique:" << unique << endl;
  ss << linePrefix << "-----------" << endl;
  return ss.str();
}

DBIndexException::DBIndexException(const std::string &msg1)
        : DBRuntimeException(msg1) {
}

DBIndexException::DBIndexException(const DBIndexException &src)
        : DBRuntimeException(src) {
}

DBIndexException &DBIndexException::operator=(const DBIndexException &src) {
  DBException::operator=(src);
  return *this;
}

DBIndexUniqueKeyException::DBIndexUniqueKeyException(const std::string &msg1)
        : DBIndexException(msg1) {
}

DBIndexUniqueKeyException::DBIndexUniqueKeyException(const DBIndexUniqueKeyException &src)
        : DBIndexException(src) {
}

DBIndexUniqueKeyException &DBIndexUniqueKeyException::operator=(const DBIndexUniqueKeyException &src) {
  DBException::operator=(src);
  return *this;
}

