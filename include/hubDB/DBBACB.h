#ifndef DBBACB_H_
#define DBBACB_H_

#include <hubDB/DBBCB.h>

/**
 * Der Buffer-Access-Control-Block (BACB) kapselt einen BCB (Buffer-Controll-Block).
 * Übergibt den Frame an das Zugriffsmodul (Indexmanager, Tupelmanager, Recoverymanager).
 *
 */
namespace HubDB{
    namespace Manager{
        class DBBufferMgr;
    }
    namespace Buffer{
        class DBBACB
        {
            friend class HubDB::Manager::DBBufferMgr;
        public:
            DBBACB(DBBCB & bcb):bcb(bcb){
              if(logger!=NULL){
                LOG4CXX_INFO(logger,"DBBACB(DBBCB)");
                LOG4CXX_DEBUG(logger,"this:\n"+toString("\t"));
              }
            };
            DBBACB(const DBBACB & bacb) : bcb(bacb.bcb){
              if(logger!=NULL){
                LOG4CXX_INFO(logger,"DBBACB(DBBACB)");
                LOG4CXX_DEBUG(logger,"this:\n"+toString("\t"));
              }
            };
            string toString(string linePrefix="") const;
            char * getDataPtr() { return bcb.getDataPtr();};
            const char * getDataPtr() const { return bcb.getDataPtr(); };
            void setModified() { bcb.setModified();};
            void setDirty() { bcb.setDirty();};
            bool getModified() const { return bcb.getModified();};
            bool getDirty() const { return bcb.getDirty();};
            BlockNo getBlockNo() const { return bcb.getFileBlock().getBlockNo();};
            DBBCBLockMode getLockMode() const { return bcb.getLockMode4Thread(); };

        protected:
            DBBCB & getBCB() const {return bcb;};
        private:
            static LoggerPtr logger;
            DBBCB & bcb;
        };
    }
}
#endif /*DBBACB_H_*/
