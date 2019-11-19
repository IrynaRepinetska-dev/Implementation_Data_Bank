#include <getopt.h>

#include <hubDB/DBServer.h>
#include <hubDB/DBQueryMgr.h>
#include <hubDB/DBException.h>
#include <hubDB/DBMonitorMgr.h>
#include <hubDB/DBClientSocket.h>
#include <signal.h>

using namespace HubDB::Server;
using namespace HubDB::Socket;
using namespace HubDB::Exception;
using namespace HubDB::Mutex;
using namespace HubDB::Manager;

LoggerPtr DBServer::logger(Logger::getLogger("HubDB.Server.DBServer"));
uint DBServer::actThreadCnt(0);
uint DBServer::maxCntOfThreads(MAX_THREADS);

DBMutex DBServer::threadCntMutex;
pthread_cond_t DBServer::count_threshold_cv = PTHREAD_COND_INITIALIZER;
bool DBServer::abortRecived(false);
int DBServer::port(STD_PORT);

void DBServer::catch_int(int sig_num) {
  LOG4CXX_INFO(logger, "catch_int()");
  LOG4CXX_DEBUG(logger, "catch SIGINT - wait until all connections closed");
  /* re-set the signal handler again to catch_int, for next time */
  signal(SIGINT, SIG_IGN);

  abortRecived = true;

  pthread_attr_t attr;
  LOG4CXX_DEBUG(logger, "init pthread_attr");
  pthread_attr_init(&attr);
  pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
  pthread_t callThd;

  threadCntMutex.lock();
  LOG4CXX_DEBUG(logger, "multi thread actThreadCnt: " + TO_STR(actThreadCnt));
  ++actThreadCnt;
  threadCntMutex.unlock();

  int err;
  LOG4CXX_DEBUG(logger, "create thread to close socket loop");
  if ((err = pthread_create(&callThd, &attr, DBServer::DBThreadFuncClose, NULL)) != 0) LOG4CXX_ERROR(logger,
                                                                                                     "phtread_create()=" +
                                                                                                     TO_STR(err));
}

DBServer::DBServer() :
        socket(NULL),
        sysCatMgr(NULL),
        bufferMgrName(DEFAULT_BUFMGR),
        queryExecName(DEFAULT_QUERYEXEC),
        monitor(DEFAULT_MONITOR_STAT),
        threading(DEFAULT_THREAD_STAT) {
  if (logger != NULL) LOG4CXX_INFO(logger, "DBServer()");

  /* set the INT (Ctrl-C) signal handler to 'catch_int' */
  signal(SIGINT, catch_int);
}

DBServer::~DBServer() {
  LOG4CXX_INFO(logger, "~DBServer()");
  LOG4CXX_DEBUG(logger, "this:\n" + toString("\t"));
  if (sysCatMgr != NULL)
    delete sysCatMgr;
  if (socket != NULL)
    delete socket;
}

int DBServer::run(const int argc, char *const argv[]) {
  LOG4CXX_DEBUG(logger, "run()");
  DBServer server;
  return server.execute(argc, argv);
}

string DBServer::toString(string linePrefix) const {
  stringstream ss;
  ss << linePrefix << "[DBServer]" << endl;
  ss << linePrefix << "bufferMgrName: " << bufferMgrName << endl;
  ss << linePrefix << "queryExecName: " << queryExecName << endl;
  ss << linePrefix << "port: " << port << endl;
  ss << linePrefix << boolalpha << "monitor: " << monitor << endl;
  ss << linePrefix << "threading: " << threading << endl;
  ss << linePrefix << "socket: " << (socket == NULL ? "NULL" : socket->toString()) << endl;
  ss << linePrefix << "sysCatMgr: " << (sysCatMgr == NULL ? "NULL" : sysCatMgr->toString()) << endl;
  ss << linePrefix << "----------" << endl;
  return ss.str();
}

void *DBServer::DBThreadFunc(void *arg) {
  LOG4CXX_INFO(logger, "DBThreadFunc()");
  startUserThreadProcessing((ThreadArg *) arg);
  threadCntMutex.lock();
  --actThreadCnt;
  LOG4CXX_DEBUG(logger, "actThreadCnt: " + TO_STR(actThreadCnt));
  if (actThreadCnt < maxCntOfThreads)
    pthread_cond_signal(&count_threshold_cv);
  threadCntMutex.unlock();
  pthread_exit(NULL);
}

void *DBServer::DBThreadFuncClose(void *arg) {
  LOG4CXX_INFO(logger, "DBThreadFuncClose()");
  try {
    DBClientSocket *clientSocket = DBClientSocket::createConnection(port, "localhost");
    LOG4CXX_DEBUG(logger, "delete client socket" + TO_STR(clientSocket));
    if (clientSocket != NULL) {
      delete clientSocket;
    }
    threadCntMutex.lock();
    --actThreadCnt;
    LOG4CXX_DEBUG(logger, "actThreadCnt: " + TO_STR(actThreadCnt));
    if (actThreadCnt < maxCntOfThreads)
      pthread_cond_signal(&count_threshold_cv);
    threadCntMutex.unlock();
  } catch (DBException e) {
    LOG4CXX_WARN(logger, e.what());
  }
  pthread_exit(NULL);
}

void DBServer::startUserThreadProcessing(ThreadArg *arg) {
  LOG4CXX_INFO(logger, "startUserThreadProcessing()");
  DBQueryMgr *queryMgr = NULL;
  LOG4CXX_DEBUG(logger, "create queryMgr " + arg->queryExec);
  queryMgr = (DBQueryMgr *) getClassForName(arg->queryExec, 2, arg->socket, arg->sysCat);
  queryMgr->process();
  LOG4CXX_DEBUG(logger, "write socket");
  arg->socket->writeToSocket();
  LOG4CXX_DEBUG(logger, "delete queryMgr");
  if (queryMgr != NULL)
    delete queryMgr;
  LOG4CXX_DEBUG(logger, "delete socket");
  delete arg->socket;
  LOG4CXX_DEBUG(logger, "delete arg");
  delete arg;
}

void DBServer::startListen() {
  LOG4CXX_INFO(logger, "startListen()");
  LOG4CXX_DEBUG(logger, "this:\n" + toString("\t"));
  pthread_attr_t attr;
  LOG4CXX_DEBUG(logger, "init pthread_attr");
  pthread_attr_init(&attr);
  pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
  while (abortRecived == false) {
    ThreadArg *arg = new ThreadArg;
    arg->queryExec = queryExecName;
    arg->socket = socket->acceptConnection();
    arg->sysCat = sysCatMgr;
    LOG4CXX_DEBUG(logger, "arg.socket:\n" + arg->socket->toString("\t"));
    if (abortRecived == true) {
      if (arg->socket != NULL)
        delete arg->socket;
    } else {
      if (threading == false || maxCntOfThreads == 0) {
        LOG4CXX_DEBUG(logger, "single thread");
        startUserThreadProcessing(arg);
      } else {
        threadCntMutex.lock();
        LOG4CXX_DEBUG(logger, "multi thread actThreadCnt: " + TO_STR(actThreadCnt));
        ++actThreadCnt;
        threadCntMutex.unlock();

        pthread_t callThd;
        int err;
        if ((err = pthread_create(&callThd, &attr, DBServer::DBThreadFunc, arg)) != 0) {
          threadCntMutex.lock();
          --actThreadCnt;
          threadCntMutex.unlock();
          LOG4CXX_ERROR(logger, "phtread_create()=" + TO_STR(err));
          throw DBSystemException(err);
        }

        threadCntMutex.lock();
        LOG4CXX_DEBUG(logger, "actThreadCnt: " + TO_STR(actThreadCnt));
        while (actThreadCnt >= maxCntOfThreads) {
          pthread_cond_wait(&count_threshold_cv, threadCntMutex.getPthread_mutex());
        }
        threadCntMutex.unlock();
      }
    }
  }
  threadCntMutex.lock();
  while (actThreadCnt != 0) {
    LOG4CXX_DEBUG(logger, "actThreadCnt: " + TO_STR(actThreadCnt));
    pthread_cond_wait(&count_threshold_cv, threadCntMutex.getPthread_mutex());
  }
  threadCntMutex.unlock();
}

void DBServer::checkAbortStatus() {
  LOG4CXX_INFO(logger, "checkAbortStatus()");
  if (abortRecived == true)
    throw DBException("CTRL-C");
};

int DBServer::execute(const int argc, char *const argv[]) {
  LOG4CXX_INFO(logger, "execute()");
  int rc = EXIT_SUCCESS;
  try {
    parseCommandArgs(argc, argv);
    LOG4CXX_DEBUG(logger, "create SysCat");
    sysCatMgr = new DBSysCatMgr(monitor, bufferMgrName, threading);
    LOG4CXX_DEBUG(logger, "create Socket");
    socket = DBServerSocket::createConnection(port);
    LOG4CXX_DEBUG(logger, "start Listen");
    startListen();
  } catch (DBException &e) {
    LOG4CXX_ERROR(logger, e.what());
    cerr << e.what() << endl;
    rc = EXIT_FAILURE;
  }
  return rc;
}

void DBServer::parseCommandArgs(const int argc, char *const argv[]) {
  LOG4CXX_INFO(logger, "parseCommand()");
  LOG4CXX_DEBUG(logger, "argc: " + TO_STR(argc));
  if (LOG4CXX_UNLIKELY(logger->isDebugEnabled())) {
    for (int i = 0; i < argc; ++i) LOG4CXX_DEBUG(logger, "argv[" + TO_STR(i) + "]: " + argv[i]);
  }

  struct option options[] =
          {
                  {"disablethread", no_argument,       0, 'd'},
                  {"monitor",       no_argument,       0, 'm'},
                  {"port",          required_argument, 0, 'p'},
                  {"bufferMgr",     optional_argument, 0, 'b'},
                  {"queryExec",     optional_argument, 0, 'q'},
                  {0, 0,                               0, 0}
          };
  opterr = 0;

  while (1) {
    /* getopt_long stores the option index here. */
    int option_index = 0;
    int c = getopt_long(argc, argv, "dmp:b:q:", options, &option_index);
    LOG4CXX_DEBUG(logger, "getopt_long() = " + TO_STR(c));

    /* Detect the end of the options. */
    if (c == -1)
      break;

    if (c == 0)
      c = options[option_index].val;

    LOG4CXX_DEBUG(logger, "switch(" + TO_STR(c) + ")");

    switch (c) {
      case 'm':
        monitor = true;
        LOG4CXX_INFO(logger, "enable monitoring");
        break;
      case 'd':
        threading = false;
        LOG4CXX_INFO(logger, "disable threading");
        break;
      case 'b':
        bufferMgrName = optarg;
        LOG4CXX_INFO(logger, "bufferMgrName: " + bufferMgrName);
        break;
      case 'q':
        queryExecName = optarg;
        LOG4CXX_INFO(logger, "queryExecName: " + queryExecName);
        break;
      case 'p':
        errno = 0;
        port = strtol(optarg, (char **) NULL, 10);
        LOG4CXX_INFO(logger, (string) "strtol(" + optarg + ")=" + TO_STR(port));
        if (errno != 0) {
          LOG4CXX_ERROR(logger, "invalid port");
          throw HubDB::Exception::DBIllegalCommandLineArgumentException("invalid port");
        }
        break;
      default: LOG4CXX_ERROR(logger, "unknown option ARGV-element");
        throw HubDB::Exception::DBIllegalCommandLineArgumentException("unknown option ARGV-element");
    }
  }

  LOG4CXX_DEBUG(logger, "optind:" + TO_STR(optind));

  if (optind < argc) {
    stringstream ss;
    ss.str("non-option ARGV-elements");
    while (optind < argc)
      ss << " : " << argv[optind++];
    LOG4CXX_ERROR(logger, ss.str());
    throw HubDB::Exception::DBIllegalCommandLineArgumentException(ss.str());
  }
}
