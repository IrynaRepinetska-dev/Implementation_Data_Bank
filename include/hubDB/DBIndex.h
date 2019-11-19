#ifndef DBINDEX_H_
#define DBINDEX_H_

#include <hubDB/DBTypes.h>
#include <hubDB/DBBufferMgr.h>

using namespace HubDB::Types;
using namespace HubDB::Manager;

namespace HubDB{
    namespace Exception{
        class DBIndexException : public DBRuntimeException
        {
        public:
            DBIndexException(const std::string& msg);
            DBIndexException(const DBIndexException&);
            DBIndexException& operator=(const DBIndexException&);
        };
        class DBIndexUniqueKeyException : public DBIndexException
        {
        public:
            DBIndexUniqueKeyException(const std::string& msg);
            DBIndexUniqueKeyException(const DBIndexUniqueKeyException&);
            DBIndexUniqueKeyException& operator=(const DBIndexUniqueKeyException&);
        };
    }

    /**
     * Die abstrakte Klasse DBIndex dient als Basis für die Entwicklung eines Index.
     * Jedes Objekt einer von DBIndex abgeleiteten Klasse reprä̈sentiert eine
     * Zugriffsmöglichkeit auf eine bestimmte Indexdatei.
     * Diese Datei ist ü̈ber das Member "file" repräsentiert.
     * "mode" beschreibt die Art des Zugriffs (lesend oder schreibend).
     * Die Member attrType und attrTypeSize beschreiben den Typ und die Grö̈ße des
     * jeweils indizierten Attributs.
     */
    namespace Index{
        class DBIndex{
        public:
            DBIndex(DBBufferMgr & bufferMgr,
                    DBFile & file,
                    enum AttrTypeEnum attrType,
                    ModType mode,
                    bool unique):
                    bufMgr(bufferMgr),
                    file(file),
                    attrType(attrType),
                    mode(mode),
                    unique(unique)
            {
              attrTypeSize=DBAttrType::getSize4Type(attrType);
              if(logger!=NULL){
                LOG4CXX_INFO(logger,"DBIndex()");
                LOG4CXX_DEBUG(logger,"this:\n"+this->toString("\t"));
              }
            };

            virtual ~DBIndex(){
              LOG4CXX_INFO(logger,"~DBIndex()");
              LOG4CXX_DEBUG(logger,"this:\n"+toString("\t"));
            };

            virtual string toString(string linePrefix="") const;

            /**
             * Diese Methode initialisiert die Indexdatei nach dem Anlegen eines Index.
             * Sie wird nur einmal aufgerufen.
             */
            virtual void initializeIndex() = 0;

            /**
             * Füllt die übergegebene Liste mit Tupelidentifikatoren, welche unter dem
             * angegebenen Schlüsselelement indiziert sind.
             * @param val
             * @param tids
             */
            virtual void find(const DBAttrType & val,DBListTID & tids) = 0;

            /**
             * Fügt den übergegebenen Tupelidentifikator unter dem angegebenen Schlüsselelement ein.
             * @param val
             * @param tid
             */
            virtual void insert(const DBAttrType & val,const TID & tid) = 0;

            /**
             * Entfernt alle übergegebenen Tupelidentifikatoren unter dem angegebenen Schlüsselelement.
             * @param val
             * @param tid
             */
            virtual void remove(const DBAttrType & val,const DBListTID & tid) = 0;

            /**
             * Gibt zurück, ob (keine) Duplikate erlaubt sind
             * @return true, falls keine Duplikate erlaubt sind.
             */
            bool isUnique() const { return unique; };

        protected:
            DBBufferMgr & bufMgr;
            DBFile & file;        // Die Datei in der der Index gespeichert ist
            enum AttrTypeEnum attrType; // Typ des indizierten Attributes
            size_t attrTypeSize;        // Größe des indizierten Attributes
            ModType mode;         // Art des Zugriffs: lesend oder schreibend
            bool unique;          // unique: keine Duplikate im Index erlaubt

        private:
            static LoggerPtr logger;
        };
    }
}

#endif /*DBINDEX_H_*/
