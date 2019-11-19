#ifndef DBBUFFERMGR_H_
#define DBBUFFERMGR_H_

#include <hubDB/DBManager.h>
#include <hubDB/DBFileMgr.h>
#include <hubDB/DBBCB.h>
#include <hubDB/DBBACB.h>

using namespace HubDB::File;
using namespace HubDB::Buffer;

namespace HubDB{
	namespace Manager{

		/**
		 * Der Buffermanager fungiert als Vermittler zwischen dem Dateisystem und den
		 * Zugriffsmodulen (z. B. Querymanager). Alle Zugriffsmodule arbeiten auf Seitenbasis,
		 * d. h., jedes Datentupel in einer Seite wird durch die Adresse der Seite und den
		 * relativen Offset des Tupels innerhalb der Seite identifiziert.
		 * Der Buffermanager garantiert eine logische Sicht in Form von Seiten auf die Datei.
		 * Er übersetzt dazu eine Seitenadresse in eine Blockadresse des Filemanagers in der
		 * physischen Datei. In der HubDB ist diese Ubersetzung trivial – eine Seite entspricht einem
		 * Block. Es gibt also eine statische 1:1-Ubersetzung zwischen Seiten und Blöcken.
		 *
		 * Die Hauptaufgabe des Buffermanager besteht darin, die benötigten Seiten bzw. Blöcke vom
		 * Dateisystem in den Hauptspeicherbereich des Datenbanksystems zu transferieren. Dieser
		 * Hauptspeicherbereich besteht aus einer Liste von so genannten Frames, die vom Buffermanager
		 * verwaltet werden – dabei kann jeder Frame genau eine Seite aufnehmen. Die Liste wird als
		 * Bufferpool bezeichnet. Es gibt nur eine Instanz eines Buffermanagers in HubDB.
		 *
		 * Die Anforderung einer Seite läuft wie folgt ab:
		 * Wenn eines der Zugriffsmodule eine bestimmte Seite einer Datei benötigt, setzt es
		 * eine entsprechende Aufforderung mittels einer der Methoden
		 *  - fixBlock (zur Anforderung eines existierenden Blocks einer Datei) oder
		 *  - fixNewBlock (zur Anforderung eines neu zu erzeugenden Blocks einer Datei)
		 * an den Buffermanager ab.
		 *
		 * Der Buffermanager sucht dann in der Liste der Frames zunächst nach
		 * einem freien Platz. Ist keines der Frames unbelegt, so muss über eine geeignete
		 * Verdrängungsstrategie ein freier Platz geschaffen werden.
		 *
		 * Nun erfolgt die Übersetzung der Seitenadresse in die entsprechende Blockadresse und das
		 * Laden des Blocks in den freien Eintrag im Bufferpool. Danach erhält das aufrufende Modul
		 * Zugriff auf die Seite im Hauptspeicher.
		 *
		 * Die gefüllten Frames des Bufferpool werden durch so genannte Buffer-Control-Blocks (BCB)
		 * reprässentiert
		 */
		class DBBufferMgr : public DBManager
		{
		public:
			DBBufferMgr (bool doThreading,int bufferBlock = STD_BUFFER_BLOCKS);

 			virtual ~DBBufferMgr ( );
			virtual string toString(string linePrefix="") const;

			void createFile(const string & name){fileMgr.createFile(name);};
			void createDirectory(const string & name){fileMgr.createDirectory(name);};

			void dropFile(const string & name);
			void dropDirectory(const string & name){fileMgr.dropDirectory(name);};
			
			DBFile & openFile(const string & name){return fileMgr.openFile(name);};
			void closeFile(DBFile & file);

			uint getBlockCount(DBFile & file){return fileMgr.getBlockCnt(file);};
			void setBlockCnt(DBFile & file,uint cnt){fileMgr.setBlockCnt(file,cnt);};

			/**
			 * Zur Anforderung eines existierenden Blocks einer Datei
			 * @param file
			 * @param blockNo
			 * @param mode
			 * @return
			 */
			DBBACB fixBlock(DBFile & file,BlockNo blockNo,DBBCBLockMode mode);

			/**
			 * Zur Anforderung eines neu zu erzeugenden Blocks einer Datei
			 * @param file
			 * @return
			 */
			DBBACB fixNewBlock(DBFile & file);

      /**
       * Zur Anforderung eines neu zu erzeugenden leeren Blocks einer Datei
       * @param file
       * @return
       */
			DBBACB fixEmptyBlock(DBFile & file,BlockNo blockNo);

      /**
       * Gibt den Block frei
       * @param file
       * @return
       */
			void unfixBlock(const DBBACB & bacb);
			void flushBlock(DBBACB & bacb);
			DBBACB upgradeToExclusive(const DBBACB & bacb);
			const DBBACB downgradeToShared(const DBBACB & bacb);

		protected:

      /**
       * Gibt true zurück, wenn sich mindestens ein Block der
       * angegebenen Datei im Buffer befindet.
       *
       * Abstrakte Methode: Implementierung  in Übung 1
       * @param file Die geöffnete Datei
       * @return true, wenn sich mindestens ein Block der angegebenen Datei im Buffer befindet.
       */
			virtual bool isBlockOfFileOpen(DBFile & file) const = 0;

			/**
			 * Entfernt alle gepufferten Blöck der angegebenen Datei aus dem Buffer.
			 * Falls einer der Blöcke gerade benutzt wird (also gesperrt "locked" ist), dann
			 * wird eine DBBufferMgrException geworfen.
			 *
			 * Abstrakte Methode: Implementierung in Übung 1
			 * @param file Die geöffnete Datei
			 */
			virtual void closeAllOpenBlocks(DBFile & file) = 0;

			/**
			 * Sorgt dafür, dass sich der Block der angegebenen Datei, welcher die
			 * angegebenen Nummer hat, im Buffer befindet. Es wird ein BCB zurückgegeben,
			 * welcher den entsprechenden Frame repräsentiert. Der gepufferte Block wird
			 * entsprechend des angegebenen Sperrmodus gesperrt.
			 * Wenn für den Parameter read der Wert false übergeben wurde, darf der Block
			 * nicht in den reservierten Frame geladen werden (z.B. weil ein neuer Block
			 * geschrieben wird).
			 *
			 * Eine Implementierung erfordert eine Verdrängungsstrategie für
			 * die Verdrängung von ungenutzten Seiten (bzw. BCBs).
			 *
			 * Abstrakte Methode: Implementierung in Übung 1
			 * @param file Die geöffnete Datei
			 * @param blockNo Die Nummer des Blocks
			 * @param mode Sperrmodus
			 * @param read Wenn false übergeben wurde, darf der Block nicht in den reservierten
			 * 							Frame geladen werden
			 * @return Es wird ein BCB zurückgegeben, welcher den entsprechenden Frame repräsentiert.
			 */
			virtual DBBCB * fixBlock(DBFile & file,BlockNo blockNo,DBBCBLockMode mode,bool read)=0;

			/**
			 * Markiert den angegebenen BCB als ungenutzt.
			 *
			 * Abstrakte Methode: Implementierung in Übung 1
			 * @param bcb Der freizugebende BCB
			 */
			virtual void unfixBlock(DBBCB & bcb) = 0;

			/**
			 * Schreibt einen BCB auf Disk
			 * @param bcb Der zu schreibende BCB
			 */
			void flushBCBBlock(DBBCB & bcb);

			void waitForLock();
			void emitSignal();

			DBFileMgr fileMgr;
			int maxBlockCnt;
			
		private:
  			static LoggerPtr logger;
  			pthread_cond_t cond;
		};
    }
    namespace Exception{
        
        class DBBufferMgrException : public DBException{
        public:
            DBBufferMgrException(const std::string& msg);
            DBBufferMgrException(const DBBufferMgrException&);
            DBBufferMgrException& operator=(const DBBufferMgrException&);
        };
	}
}

#endif /*DBBUFFERMGR_H_*/
