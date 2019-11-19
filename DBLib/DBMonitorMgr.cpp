#include <hubDB/DBMonitorMgr.h>

using namespace HubDB::Manager;

LoggerPtr DBMonitorMgr::logger(Logger::getLogger("HubDB.Monitor.DBMonitorMgr"));
DBMonitorMgr *DBMonitorMgr::monitorPtr = NULL;

DBMonitorMgr::~DBMonitorMgr() {
  LOG4CXX_INFO(logger, "~DBMonitorMgr()");
}

string DBMonitorMgr::toString(string linePrefix) const {
  lock();
  stringstream ss;
  ss << linePrefix << "[DBMonitorMgr]" << endl;
  ss << DBManager::toString(linePrefix + "\t");
  ss << linePrefix << "monitorStat:" << endl;
  printStatistic(ss, linePrefix);
  ss << linePrefix << "--------------" << endl;
  unlock();
  return ss.str();
}

void DBMonitorMgr::inc(const string name) {
  lock();
  LOG4CXX_DEBUG(logger, "inc(" + name + ")");
  map<string, uint>::iterator i = monitorStat.find(name);
  if (i == monitorStat.end()) {
    monitorStat[name] = 1;
    LOG4CXX_DEBUG(logger, "monitorStat[" + name + "]=1");
  } else {
    ++(*i).second;
    LOG4CXX_DEBUG(logger, "monitorStat[" + name + "]=" + TO_STR((*i).second));
  }
  unlock();
}

ostream &DBMonitorMgr::printStatistic(ostream &s, string linePrefix) const {
  lock();
  s << linePrefix << "Monitor Statistics" << endl;
  map<string, uint>::const_iterator i = monitorStat.begin();
  s << linePrefix << "name : count" << endl;
  while (i != monitorStat.end()) {
    s << linePrefix << (*i).first << " : " << (*i).second << endl;
    ++i;
  }
  unlock();
  return s;
} 
