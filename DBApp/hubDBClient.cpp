#include <hubDB/DBClient.h>
using namespace HubDB::Client;

#include <log4cxx/logger.h>
#include <log4cxx/basicconfigurator.h>
#include <log4cxx/propertyconfigurator.h>
#include <log4cxx/helpers/exception.h>
using namespace log4cxx;
using namespace log4cxx::helpers;

LoggerPtr logger(Logger::getLogger("HubDB.Client.Main"));

int main(int argc, char *argv[])
{
  int rc = EXIT_SUCCESS;
  try{
    File f("log4c++.hubDB.properties.client");
    PropertyConfigurator::configure(f);
    DBClient::run(argc,argv);
  }catch(Exception &){
    rc = EXIT_FAILURE;
  }
  return rc;
}

