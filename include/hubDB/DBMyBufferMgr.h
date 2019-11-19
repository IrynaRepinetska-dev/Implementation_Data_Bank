#ifndef DBMYBUFFERMGR_H_
#define DBMYBUFFERMGR_H_

#include <list>
#include <iterator>

#include <hubDB/DBBufferMgr.h>

namespace HubDB{
	namespace Manager{
		class DBMyBufferMgr : public DBBufferMgr
		{
		
		public:
			DBMyBufferMgr (bool doThreading, int bufferBlock = STD_BUFFER_BLOCKS);
 			~DBMyBufferMgr ();
			string toString(string linePrefix="") const;

			static int registerClass();

		protected:

			bool isBlockOfFileOpen(DBFile & file) const;

			void closeAllOpenBlocks(DBFile & file);

			DBBCB * fixBlock(DBFile & file, BlockNo blockNo, DBBCBLockMode mode, bool read);

			void unfixBlock(DBBCB & bcb);

			// TODO add additional methods

		private:
			// TODO add your data structures

            list <int> m_unfixedList;

            static LoggerPtr logger;

            void showlist(list <int> g);
        };
	}
}

#endif /*DBMYBUFFERMGR_H_*/
