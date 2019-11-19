#include <getopt.h>

#include <hubDB/DBClient.h>
#include <hubDB/DBException.h>

using namespace HubDB::Client;
using namespace HubDB::Exception;

LoggerPtr DBClient::logger(Logger::getLogger("HubDB.Client.DBClient"));

DBClient::DBClient() :
        socket(NULL),
        port(STD_PORT),
        serverName(STD_HOST) {
  if (logger != NULL) {
    LOG4CXX_INFO(logger, "DBClient()");
    LOG4CXX_DEBUG(logger, "this:\n" + toString("\t"));
  }

}

DBClient::~DBClient() {
  LOG4CXX_INFO(logger, "~DBClient()");
  LOG4CXX_DEBUG(logger, "this:\n" + toString("\t"));
  if (socket != NULL)
    delete socket;
}

int DBClient::run(const int argc, char *const argv[]) {
  LOG4CXX_INFO(logger, "run()");
  DBClient client;
  return client.execute(argc, argv);
}


string DBClient::toString(string linePrefix) const {
  stringstream ss;
  ss << linePrefix << "[DBClient]" << endl;
  ss << linePrefix << "serverName: " << serverName << endl;
  ss << linePrefix << "port: " << port << endl;
  ss << linePrefix << "socket: \n";
  if (socket != NULL) {
    ss << socket->toString(linePrefix + "\t");
  } else {
    ss << linePrefix + "\t" + "NULL" << endl;
  }
  ss << linePrefix << "----------" << endl;
  return ss.str();
}

int DBClient::execute(const int argc, char *const argv[]) {
  LOG4CXX_INFO(logger, "execute()");
  bool flag = true;
  int rc = EXIT_SUCCESS;
  struct pollfd polls[2];
  int idx;
  try {
    parseCommandArgs(argc, argv);
    socket = DBClientSocket::createConnection(port, serverName);

    polls[0].fd = STDIN_FILENO;
    polls[0].events = POLLIN | POLLPRI;
    polls[1].fd = socket->getSocketFD();
    polls[1].events = POLLIN | POLLPRI /*| POLLRDHUP*/;

    cout << endl << "\\\\> ";
    do {

      cout.flush();
      polls[0].revents = 0;
      polls[1].revents = 0;

      if ((idx = poll(polls, 2, -1)) == -1) {
        throw DBSystemException(errno);
      }

      if (polls[0].revents != 0) {
        size_t len, size;
        len = 1024;
        char buffer[len + 1];

        memset(buffer, 0, len + 1);

        do {
          size = read(STDIN_FILENO, buffer, len);
          if (size < 0) {
            throw DBSystemException(errno);
          }
          buffer[size] = '\0';
          socket->getWriteStream() << buffer;
        } while (size == len);
        socket->writeToSocket();
      }
      if (polls[1].revents != 0) {
        socket->readFromSocket();
        if (socket->getReadString().length() == 0) {
          flag = false;
        } else {
          cout << socket->getReadString();
          cout << "\\\\> ";
          cout.flush();
        }
      }
    } while (flag);
    cout << endl;
  } catch (DBException &e) {
    LOG4CXX_ERROR(logger, e.what());
    cerr << e.what() << endl;
    rc = EXIT_FAILURE;
  }
  return rc;
}

void DBClient::parseCommandArgs(const int argc, char *const argv[]) {
  LOG4CXX_INFO(logger, "parseCommandArgs()");
  LOG4CXX_DEBUG(logger, "argc: " + TO_STR(argc));
  if (LOG4CXX_UNLIKELY(logger->isDebugEnabled())) {
    for (int i = 0; i < argc; ++i) LOG4CXX_DEBUG(logger, "argv[" + TO_STR(i) + "]: " + argv[i]);
  }
  struct option options[] =
          {
                  {"port",   required_argument, 0, 'p'},
                  {"server", required_argument, 0, 's'},
                  {0, 0,                        0, 0}
          };

  opterr = 0;

  while (1) {
    /* getopt_long stores the option index here. */
    int option_index = 0;
    int c = getopt_long(argc, argv, "p:s:", options, &option_index);
    LOG4CXX_DEBUG(logger, "getopt_long() = " + TO_STR(c));

    /* Detect the end of the options. */
    if (c == -1)
      break;

    if (c == 0)
      c = options[option_index].val;

    LOG4CXX_DEBUG(logger, "switch(" + TO_STR(c) + ")");

    switch (c) {
      case 's':
        serverName = optarg;
        LOG4CXX_INFO(logger, "serverName: " + serverName);
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
