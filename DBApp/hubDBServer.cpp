#include <hubDB/DBServer.h>
using namespace HubDB::Server;

#include <log4cxx/logger.h>
#include <log4cxx/basicconfigurator.h>
#include <log4cxx/propertyconfigurator.h>
#include <log4cxx/helpers/exception.h>
using namespace log4cxx;
using namespace log4cxx::helpers;

//LoggerPtr loggerServer(Logger::getLogger("HubDB.Server.Main"));

int main(int argc, char *argv[])
{
  int rc = EXIT_SUCCESS;
  try{
    File f("log4c++.hubDB.properties.server");
    PropertyConfigurator::configure(f);
    DBServer::run(argc,argv);
  }catch(Exception &){
    rc = EXIT_FAILURE;
  }
   
  return rc;
}

