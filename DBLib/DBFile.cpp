#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <dirent.h>

#include <hubDB/DBFile.h>
#include <hubDB/DBFileBlock.h>

using namespace HubDB::File;
using namespace HubDB::Exception;

LoggerPtr DBFile::logger(Logger::getLogger("HubDB.File.DBFile"));

DBFile::DBFile() :
        fileNum(-1) {
  if (logger != NULL) LOG4CXX_INFO(logger, "DBFile()");
}

DBFile::DBFile(const string &fileName) :
        fileName(fileName),
        fileNum(-1),
        blockCnt(0) {
  if (logger != NULL) {
    LOG4CXX_INFO(logger, "DBFile()");
    LOG4CXX_DEBUG(logger, "fileName: " + fileName);
  }
  fileNum = open(fileName.c_str(), FILE_OPEN_FLAGS);
  if (fileNum < 0) {
    if (logger != NULL) LOG4CXX_ERROR(logger, strerror(errno));
    throw DBFileException(errno);
  }
  off_t l;
  if ((l = lseek(fileNum, 0, SEEK_END)) < 0) {
    if (logger != NULL) LOG4CXX_ERROR(logger, strerror(errno));
    throw DBFileException(errno);
  }
  if (logger != NULL) LOG4CXX_DEBUG(logger, "lseek()=" + TO_STR(l));

  blockCnt = l / DBFileBlock::getBlockSize();

  if (logger != NULL) {
    LOG4CXX_DEBUG(logger, "this:\n" + toString("\t"));
  }
}

DBFile::~DBFile() {
  LOG4CXX_INFO(logger, "~DBFile()");
  LOG4CXX_DEBUG(logger, "this:\n" + toString());
  if (fileNum != -1) {
    LOG4CXX_DEBUG(logger, "close(" + TO_STR(fileNum) + ")");
    if (close(fileNum) != 0) {
      LOG4CXX_ERROR(logger, strerror(errno));
      throw DBFileException(errno);
    }
  }
}

void DBFile::create(const string &name, bool isDir) {
  LOG4CXX_INFO(logger, "create()");
  LOG4CXX_DEBUG(logger, "name: " + name);
  LOG4CXX_DEBUG(logger, "isDir: " + TO_STR(isDir));
  if (isDir == false) {
    int fd;
    if ((fd = creat(name.c_str(), FILE_CREAT_FLAGS)) < 0) {
      LOG4CXX_ERROR(logger, strerror(errno));
      throw DBFileException(errno);
    }
    close(fd);
  } else {
    if (mkdir(name.c_str(), DIR_CREAT_FLAGS) != 0) {
      LOG4CXX_ERROR(logger, strerror(errno));
      throw DBFileException(errno);
    }
  }
}

void DBFile::drop(const string &name, bool isDir) {
  LOG4CXX_INFO(logger, "drop()");
  LOG4CXX_DEBUG(logger, "name: " + name);
  LOG4CXX_DEBUG(logger, "isDir: " + TO_STR(isDir));
  if (isDir == false) {
    if (unlink(name.c_str()) != 0) {
      LOG4CXX_ERROR(logger, strerror(errno));
      throw DBFileException(errno);
    }
  } else {
    if (rmdir(name.c_str()) != 0) {
      LOG4CXX_ERROR(logger, strerror(errno));
      throw DBFileException(errno);
    }
  }
}

string DBFile::toString(string linePrefix) const {
  stringstream ss;
  ss << linePrefix << "[DBFile]" << endl;
  ss << linePrefix << "fileName: " << fileName << endl;
  ss << linePrefix << "fileNum: " << fileNum << endl;
  ss << linePrefix << "blockCnt: " << blockCnt << endl;
  ss << linePrefix << "--------" << endl;
  return ss.str();
}

void DBFile::setBlockCnt(uint cnt) {
  LOG4CXX_INFO(logger, "setBlockCnt()");
  LOG4CXX_DEBUG(logger, "cnt:" + TO_STR(cnt));
  LOG4CXX_DEBUG(logger, "this:\n" + toString());
  if (ftruncate(fileNum, cnt * DBFileBlock::getBlockSize()) != 0) {
    LOG4CXX_ERROR(logger, strerror(errno));
    throw DBFileException(errno);
  }
  blockCnt = cnt;
}

DBFileException::DBFileException(const int num)
        : DBSystemException(num) {
}

DBFileException::DBFileException(const DBFileException &src)
        : DBSystemException(src) {
}

DBFileException &DBFileException::operator=(const DBFileException &src) {
  DBException::operator=(src);
  return *this;
}
