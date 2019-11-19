#ifndef DBMONITORMGR_H_
#define DBMONITORMGR_H_

#include <hubDB/DBManager.h>

#define MONITOR_NAME(name) if(HubDB::Manager::DBMonitorMgr::getMonitorPtr()!=NULL) HubDB::Manager::DBMonitorMgr::getMonitorPtr()->inc(name)
#define MONITOR_FUNC MONITOR_NAME(__PRETTY_FUNCTION__)

namespace HubDB{
	namespace Manager{
		class DBMonitorMgr : public DBManager{
		public:
			DBMonitorMgr(bool threading):DBManager(threading){if(logger!=NULL) LOG4CXX_INFO(logger,"DBMonitorMgr()");};
  			~DBMonitorMgr ();
			string toString(string linePrefix="") const;
			void inc(const string name);
			ostream & printStatistic(ostream & s,string linePrefix="") const;
            static DBMonitorMgr * getMonitorPtr(){return monitorPtr;};
            static void setMonitorPtr(DBMonitorMgr * mgr){monitorPtr = mgr;};
		private:
			static LoggerPtr logger;
            static DBMonitorMgr * monitorPtr;
			map<string,uint> monitorStat;
		};
					
	}
}

#endif /*DBMONITORMGR_H_*/
