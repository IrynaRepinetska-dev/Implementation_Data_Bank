#ifndef DBMUTEX_H_
#define DBMUTEX_H_

#include <hubDB/DBTypes.h>
#include <hubDB/DBException.h>

namespace HubDB{
	namespace Mutex{
		class DBMutex
		{
			public:
				DBMutex();
				~DBMutex();
				string toString(string linePrefix="") const;
	 			void lock()const;
  				void unlock()const;
				pthread_mutex_t * getPthread_mutex()const;
  				static string getMsg(const int err); 
			private:
				DBMutex(const DBMutex & mutex);
				pthread_mutex_t mutex;
				static LoggerPtr logger;
		};
		inline pthread_mutex_t * DBMutex::getPthread_mutex()const {return (pthread_mutex_t*)&mutex;};
    }
    namespace Exception{
        class DBMutexException : public DBRuntimeException
        {
            public:
                DBMutexException(const std::string& msg);
                DBMutexException(const DBMutexException&);
                DBMutexException& operator=(const DBMutexException&);
        };
	}
}

#endif /*DBMUTEX_H_*/
