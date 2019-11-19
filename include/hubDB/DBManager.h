#ifndef DBMANAGER_H_
#define DBMANAGER_H_

#include <hubDB/DBLock.h>

using namespace HubDB::Mutex;

namespace HubDB{
    namespace Manager{
        class DBManager
        {
        public:
            DBManager(bool doThreading):threading(doThreading){ if(logger!=NULL) LOG4CXX_INFO(logger,"threading: "+ TO_STR(threading));};
            ~DBManager(){};
            string toString(string linePrefix="") const;
        protected:
            void lock()const {if(doThreading()) (const_cast<DBLock&>(lockMutex)).lock();};
            void unlock()const {if(doThreading()) (const_cast<DBLock&>(lockMutex)).unlock();};
            const DBMutex & getMutex()const {return lockMutex.getMutex();};
            bool doThreading()const{return threading;};
        private:
            DBLock lockMutex;
            static LoggerPtr logger;
            bool threading;
        };
    }
}


#endif // DBMANAGER_H_
