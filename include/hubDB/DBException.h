#ifndef DBEXCEPTION_H_
#define DBEXCEPTION_H_

#include <exception>
#include <hubDB/DBTypes.h>

namespace HubDB
{
    namespace Exception
    {
        class DBException : public ::std::exception
        {
        public:
	       DBException(const string& msg);
           DBException(const DBException& src);
           virtual ~DBException()throw(){};
           DBException& operator=(const DBException& src);
           const char* what() const throw();
           string getTrace() const throw();
        private:    
           string createTrace();
        private:
           enum { MSG_SIZE = 128 };
           char msg[MSG_SIZE + 1];
           string trace;
        };

        class DBRuntimeException : public DBException{
        public:
	       DBRuntimeException(const string& msg);
	       DBRuntimeException(const DBRuntimeException& msg);
	       DBRuntimeException& operator=(const DBRuntimeException& src);
        };

        /** Thrown when an application attempts to use null in a case where an
            object is required.
        */
        class DBNullPointerException : public DBRuntimeException{
        public:
	       DBNullPointerException(const string& msg);
	       DBNullPointerException(const DBNullPointerException& msg);
	       DBNullPointerException& operator=(const DBNullPointerException& src);
        };

        /** Thrown when an application attempts to use null in a case where an
            object is required.
        */
        class DBOutOfBoundException : public DBRuntimeException{
        public:
	       DBOutOfBoundException(const string& msg);
	       DBOutOfBoundException(const DBOutOfBoundException& msg);
	       DBOutOfBoundException& operator=(const DBOutOfBoundException& src);
        };

        /** Thrown to indicate that a method has been passed
            an illegal or inappropriate argument.
        */
        class DBIllegalArgumentException : public DBRuntimeException{
        public:
	       DBIllegalArgumentException(const string& msg);
	       DBIllegalArgumentException(const DBIllegalArgumentException&);
	       DBIllegalArgumentException& operator=(const DBIllegalArgumentException&);
        };

        /** Thrown to indicate that a method has been passed
            an illegal or inappropriate argument.
        */
        class DBIllegalCommandLineArgumentException : public DBRuntimeException{
        public:
	       DBIllegalCommandLineArgumentException(const string& msg);
	       DBIllegalCommandLineArgumentException(const DBIllegalCommandLineArgumentException&);
	       DBIllegalCommandLineArgumentException& operator=(const DBIllegalCommandLineArgumentException&);
        };

        class DBClassForNameException : public DBRuntimeException{
        public:
	       DBClassForNameException(const string& msg);
	       DBClassForNameException(const DBClassForNameException&);
	       DBClassForNameException& operator=(const DBClassForNameException&);
        };
        
        class DBSystemException : public DBException{
        public:
            DBSystemException(const int num);
            DBSystemException(const DBSystemException&);
            DBSystemException& operator=(const DBSystemException&);
            int errNo(){return err;};
        private:
            int err;
        };
    }
}

#endif // DBEXCEPTION_H_

