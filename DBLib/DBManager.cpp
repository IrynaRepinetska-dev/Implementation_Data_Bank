#include <hubDB/DBManager.h>

using namespace HubDB::Manager;

LoggerPtr DBManager::logger(Logger::getLogger("HubDB.Manager.DBManager"));

string DBManager::toString(string linePrefix) const {
  stringstream ss;
  ss << linePrefix << "[DBManager]" << endl;
  ss << linePrefix << "threading: " << threading << endl;
  ss << linePrefix << "mutex:" << endl;
  ss << lockMutex.toString(linePrefix + "\t") << endl;
  ss << linePrefix << "-----------" << endl;
  return ss.str();
}
