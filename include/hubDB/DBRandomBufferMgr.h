#ifndef DBRANDOMBUFFERMGR_H_
#define DBRANDOMBUFFERMGR_H_

#include <hubDB/DBBufferMgr.h>

/**
 * Naive Implementierung eines BufferManagers.
 *
 * Positionen von nicht fixierten BCBs werden in BitArray gespeichert.
 * Verdrängung: Nimmt immer die erste gefundene Seite (weiter hinten liegende
 * Seiten werden evtl. nie verdrängt)
 */
namespace HubDB{
	namespace Manager{
		class DBRandomBufferMgr : public DBBufferMgr
		{
		
		public:
			DBRandomBufferMgr (bool doThreading, int bufferBlock = STD_BUFFER_BLOCKS);
 			~DBRandomBufferMgr ();
			string toString(string linePrefix="") const;

			static int registerClass();

		protected:
			bool isBlockOfFileOpen(DBFile & file) const;
			void closeAllOpenBlocks(DBFile & file);
			DBBCB * fixBlock(DBFile & file,BlockNo blockNo,DBBCBLockMode mode,bool read);
            void unfixBlock(DBBCB & bcb);

            int findBlock(DBFile & file,BlockNo blockNo);
			int findBlock(DBBCB * bcb);

			void freeFrame(int i){ bitMap[i / 32] |= (1 << (i % 32));}
			void reserveFrame(int i){ bitMap[i / 32] &= (~(1 << (i % 32)));}
			bool isFreeFrame(int i)const { return (bitMap[i / 32] & (1 << (i % 32))) != 0 ? true : false;}
			
		private:
			DBBCB ** bcbList;
			int * bitMap; // Bitmap: besetzte Position: 0, frei = 1
			int mapSize;
			static LoggerPtr logger;


		};
	}
}

#endif /*DBRANDOMBUFFERMGR_H_*/
