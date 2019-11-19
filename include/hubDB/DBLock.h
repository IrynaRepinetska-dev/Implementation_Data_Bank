#ifndef DBLOCK_H_
#define DBLOCK_H_

#include <hubDB/DBMutex.h>

namespace HubDB{
	namespace Mutex{
		class DBLock
		{
			public:
				DBLock();
				~DBLock();
				string toString(string linePrefix="") const __attribute__ ((pure));
	 			void lock();
  				void unlock();
                const DBMutex & getMutex()const {return mutex;};
			private:
				DBMutex mutex;
				pthread_t threadId;
				uint lockCnt;
				pthread_cond_t cond;
				static LoggerPtr logger;
		};
	}
}

#endif /*DBLOCK_H_*/
