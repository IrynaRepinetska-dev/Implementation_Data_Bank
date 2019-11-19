#ifndef DBTYPE_H_
#define DBTYPE_H_

#include <iostream>
#include <sstream>
#include <cstdlib>
#include <string>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <pthread.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>
#include <poll.h>
#include <assert.h>
#include <stdarg.h>
#include <list>
#include <unordered_set>
#include <map>
#include <stack>
#include <typeinfo>
#include <iomanip>
#include <algorithm>

using namespace std;

#include <log4cxx/logger.h>
using namespace log4cxx;

const int  STD_PORT = 6666;
const uint STD_SOCKET_BUFFER_SIZE = 512;
const uint STD_LISTEN_CNT = 6;
const uint STD_BLOCKSIZE = 1024;
const uint STD_BUFFER_BLOCKS = 50;
const uint MAX_THREADS = 5;
const string STD_HOST = "localhost";
const string DEFAULT_BUFMGR = "DBRandomBufferMgr";
const string DEFAULT_IDXTYPE = "DBSeqIndex";
const string DEFAULT_QUERYEXEC = "DBSimpleQueryMgr";
const string FILE_SEP = "/";
const bool DEFAULT_MONITOR_STAT = true;
const bool DEFAULT_THREAD_STAT = true;
const uint MAX_STR_LEN = 30;
const uint MAX_ATTR_PER_REL = 10;

typedef unsigned int uint;
typedef int FileNo;
typedef uint BlockNo;

namespace HubDB{
    namespace Types{
        extern LoggerPtr logger;
        enum ModType{
            READ,
            WRITE
        };

        struct TID {
            BlockNo page;
            uint slot;
            bool operator<(const TID & tid)const{ return (page<tid.page || ( page==tid.page && slot < tid.slot )) ? true : false;};
            bool operator==(const TID & tid)const { return (page==tid.page && slot==tid.slot) ? true :false;};
            const char * read(const char * ptr){ memcpy(this,ptr,sizeof(TID)); return ptr+sizeof(TID);};
            char * write(char * ptr)const{ memcpy(ptr,this,sizeof(TID)); return ptr+sizeof(TID);};
            TID & operator=(const TID & tid){ page = tid.page; slot= tid.slot; return *this;};
            string toString() const { stringstream ss; ss << page <<":" << slot; return ss.str();};
        };

        /**
         * Hash-Funktion für TIDs
         */
        struct hash_TIDs {
            std::size_t operator()(TID const& s) const {
              return std::hash<std::string>()(s.toString());
            }
        };

        typedef list< TID > DBListTID;

        string toString(DBListTID & l);

        enum AttrTypeEnum{
            NONE,
            INT,
            DOUBLE,
            VCHAR
        };

        union DataImport{
            double d;
            int i;
            char c[MAX_STR_LEN+3];
        };

        class DBAttrType{
        public:
            virtual ~DBAttrType(){};
            virtual bool operator==(const DBAttrType & ref)const = 0;
            virtual bool operator<(const DBAttrType & ref)const = 0;
            virtual bool operator>(const DBAttrType & ref)const = 0;
            virtual string toString(string linePrefix="") const;
            virtual AttrTypeEnum type()const =0;
            virtual char * write(char * ptr)const =0;
            virtual ostream & print(ostream & s,bool align=true) const = 0;
            virtual void exportToFile(FILE * file) const = 0;
            static DBAttrType * read(const char * ptr,const AttrTypeEnum t,const char ** ptrNext = NULL);
            static DBAttrType * import(DataImport & ptr,const AttrTypeEnum t);
            static size_t getSize4Type(const AttrTypeEnum type);
            static string getName4Type(const AttrTypeEnum t);
            static DBAttrType * clone(const DBAttrType & ref);
        };

        class DBIntType : public DBAttrType{
        public:
            DBIntType(int val):val(val){};
            DBIntType(const char * ptr){ memcpy(&val,ptr,getSize());};
            DBIntType(const DBIntType & ref):val(ref.val){};
            DBIntType operator=(const DBIntType & ref){val =  ref.val; return *this;};

            string toString(string linePrefix="") const;
            bool operator==(const DBAttrType & ref)const;
            bool operator<(const DBAttrType & ref)const;
            bool operator>(const DBAttrType & ref)const;
            AttrTypeEnum type()const{ return INT;};
            int getVal()const {return val;};
            char * write(char * ptr)const { memcpy(ptr,&val,getSize());return ptr+ getSize();};
            static size_t getSize(){return sizeof(int);};
            ostream & print(ostream & s,bool align=true) const;
            void exportToFile(FILE * file)const{fprintf(file,"%d",val);};
        protected:
            int val;
        };

        class DBDoubleType : public DBAttrType{
        public:
            DBDoubleType(double val):val(val){};
            DBDoubleType(const char * ptr){ memcpy(&val,ptr,getSize());};
            DBDoubleType(const DBDoubleType & ref):val(ref.val){};
            DBDoubleType operator=(const DBDoubleType & ref){val =  ref.val; return *this;};
            string toString(string linePrefix="") const;
            bool operator==(const DBAttrType & ref)const;
            bool operator<(const DBAttrType & ref)const;
            bool operator>(const DBAttrType & ref)const;
            double getVal()const {return val;};
            AttrTypeEnum type()const { return DOUBLE;};
            char * write(char * ptr)const{ memcpy(ptr,&val,getSize());return ptr+ getSize();};
            static size_t getSize(){return sizeof(double);};
            ostream & print(ostream & s,bool align=true) const;
            void exportToFile(FILE * file)const{fprintf(file,"%g",val);};
        protected:
            double val;
        };

        class DBVCharType : public DBAttrType{
        public:
            DBVCharType(const char * ptr){strncpy(val,ptr,getSize());val[getSize()]='\0';};
            DBVCharType(const DBVCharType & ref){memcpy(val,ref.val,getSize());val[getSize()]='\0';};
            DBVCharType operator=(const DBVCharType & ref){memcpy(val,ref.val,getSize());val[getSize()]='\0'; return *this;};
            string toString(string linePrefix="") const;
            bool operator==(const DBAttrType & ref)const;
            bool operator<(const DBAttrType & ref)const;
            bool operator>(const DBAttrType & ref)const;
            AttrTypeEnum type()const{ return VCHAR;};
            string getVal()const {return val;};
            char * write(char * ptr)const{ memcpy(ptr,val,getSize()); return ptr + getSize();};
            static size_t getSize(){return MAX_STR_LEN;};
            ostream & print(ostream & s,bool align=true) const;
            void exportToFile(FILE * file)const{fprintf(file,"'%s'",val);};
        protected:
            char val[MAX_STR_LEN+1];
        };

        typedef list<DBAttrType*> DBListAttrType;
        typedef list< DBListAttrType > DBSearchResultType;

        struct AttrDefStruct{
            char attrName[MAX_STR_LEN+1];
            uint attrPos;
            enum AttrTypeEnum attrType;
            uint attrLen;
            bool isIndexed;
            char indexType[MAX_STR_LEN+1];
            bool isUnique;
        };

        class DBAttrDef{
        public:
            DBAttrDef(){memset(&attr,0,sizeof(attr));}
            DBAttrDef(const AttrDefStruct * ptr){attr = *ptr;};
            string attrName()const{ return attr.attrName;};
            uint attrPos()const{ return attr.attrPos;};
            enum AttrTypeEnum attrType()const{ return attr.attrType;};
            uint attrLen()const{ return attr.attrLen;};
            string indexType()const{ return attr.indexType;};
            bool isIndexed()const{ return attr.isIndexed;};
            bool isUnique()const{ return attr.isUnique;};
            string toString(string linePrefix="") const;
            ostream & print(ostream & s) const;
        private:
            AttrDefStruct attr;
        };

        struct RelDefStruct{
            char relationName[MAX_STR_LEN+1];
            uint attrCnt;
            uint tupleSize;
            AttrDefStruct attrList[MAX_ATTR_PER_REL];
        };

        class DBRelDef{
        public:
            DBRelDef(const RelDefStruct * ptr){ relation=*ptr;};
            string toString(string linePrefix="") const;

            string relationName()const {return relation.relationName;};
            uint attrCnt() const { return relation.attrCnt;};
            uint tupleSize()const {return relation.tupleSize;};

            DBAttrDef attrDef(string attrName)const ;
            DBAttrDef attrDef(uint pos)const;
            enum AttrTypeEnum attrTypeDef(uint pos)const ;
            ostream & print(ostream & s) const;
        private:
            RelDefStruct relation;
        };

        class DBTuple{
        public:
            DBTuple();
            ~DBTuple();
            DBTuple(const DBTuple & ref);

            const char * read(const DBRelDef & relDef,const char * ptr);
            char * write(char * ptr)const;

            const DBAttrType & getAttrVal(uint pos)const;
            void appendAttrVal(DBAttrType * val);
            string toString(string linePrefix="") const;
            void setTID(const TID & t){tid = t;};
            TID getTID()const {return tid;};
            DBTuple & operator=(const DBTuple & ref);

        protected:
            vector<DBAttrType *> vals;
            TID tid;
        };

        typedef list< DBTuple > DBListTuple;
        typedef list< pair<DBTuple,DBTuple> > DBListJoinTuple;

        struct QualifiedName{
            char relationName[MAX_STR_LEN+1];
            char attributeName[MAX_STR_LEN+1];
            string toString()const{ return (string)relationName+"."+attributeName;};
        };

        typedef list< QualifiedName > DBListQualifiedName;

        class DBPredicate{
        public:
            DBPredicate(const DBPredicate & ref):val_(NULL),name_(ref.name_){ val_ = DBAttrType::clone(*ref.val_);};
            DBPredicate(DBAttrType * valR,QualifiedName name):val_(NULL),name_(name){ val_ = DBAttrType::clone(*valR);};
            ~DBPredicate(){ if(val_!=NULL) delete val_;};
            const DBAttrType & val(){ return *val_;};
            const QualifiedName & name(){ return name_;};
            string toString(){ stringstream ss; ss << name_.toString()<<"="; val_->print(ss,false); return ss.str();};

        private:
            DBAttrType * val_;
            QualifiedName name_;
        };

        typedef list<DBPredicate> DBListPredicate;

        struct DBJoin{
            char relationName[MAX_STR_LEN+1];
            QualifiedName attrNames[2];
            string toString()const{ stringstream ss; ss <<"join "<<relationName<<" on " <<attrNames[0].toString()<<"="<<attrNames[1].toString(); return ss.str();};
        };

        inline string toString(double i){stringstream ss; ss<<i; return ss.str();};
        inline string toString(uint i){stringstream ss; ss<<i; return ss.str();};
        inline string toString(int i){stringstream ss; ss<<i; return ss.str();};
        inline string toString(bool i){stringstream ss; ss<<boolalpha<<i; return ss.str();};
        inline string toString(void * ptr){stringstream ss; ss<<hex<<ptr; return ss.str();};
        inline string toString(off_t ptr){stringstream ss; ss<<ptr; return ss.str();};
#ifdef __x86_64__
        inline string toString(size_t t){stringstream ss; ss<<t; return ss.str();};
#endif
        inline string toString(char * ptr){return toString((void*)ptr);};
        inline string toString(RelDefStruct * ptr){DBRelDef d(ptr); return d.toString();};
        string toString(DBListPredicate & l);

#define TO_STR(X) HubDB::Types::toString(X)

        typedef void * create_t(int nArgs,va_list ap);
        void * getClassForName(const string & name,int nArg,...);
        void setClassForName(const string & name, create_t * func);
        list<string> getKnownClassNames();
        string getKnownClassNames(char sep);
    }
}

const HubDB::Types::TID invalidTID={0xFFFFFFFF,0xFFFFFFFF};

#endif /*DBTYPE_H_*/
