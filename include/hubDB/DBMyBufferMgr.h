#ifndef DBMYBUFFERMGR_H_
#define DBMYBUFFERMGR_H_

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
               void showlist(list <int> g);
            int findBlock(DBFile & file,BlockNo blockNo);
            int findBlock(DBBCB * bcb);
            void freeFrame(int i);
            void reserveFrame(int i);
            bool isFreeFrame(int i);

		private:
			// TODO add your data structures

            list <int> m_unfixedList; // save index of unfixed Block in the array of pointers bcbList.

			DBBCB **bcbList;
			int *bitMap;
			int mapSize;

			static LoggerPtr logger;
		};
	}
}

#endif /*DBMYBUFFERMGR_H_*/
