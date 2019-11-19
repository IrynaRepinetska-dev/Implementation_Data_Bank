#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <dirent.h>

#include <hubDB/DBFileBlock.h>
#include <hubDB/DBFile.h>
#include <hubDB/DBException.h>

using namespace HubDB::File;
using namespace HubDB::Exception;

LoggerPtr DBFileBlock::logger(Logger::getLogger("HubDB.File.DBFileBlock"));
const uint DBFileBlock::blockSize(STD_BLOCKSIZE);

DBFileBlock::DBFileBlock(DBFile &file, BlockNo blockNum) :
        file(file),
        blockNum(blockNum) {
  if (logger != NULL) {
    LOG4CXX_INFO(logger, "DBFileBlock()");
    LOG4CXX_DEBUG(logger, "file:\n" + file.toString("\t"));
    LOG4CXX_DEBUG(logger, "blockNo:" + TO_STR(blockNum));
    LOG4CXX_DEBUG(logger, "this:\n" + toString("\t"));
  }
  if (blockNum >= file.getBlockCnt())
    throw DBException("invalid blockNum");
}

void DBFileBlock::readFileBlock() {
  LOG4CXX_INFO(logger, "readFileBlock()");
  LOG4CXX_DEBUG(logger, "this:\n" + toString("\t"));
  errno = 0;
  int num = file.getFileNum();
  off_t l = blockNum * getBlockSize();
  LOG4CXX_DEBUG(logger, "lseek(" + TO_STR(l) + ")");
  if (l != lseek(num, l, SEEK_SET)) {
    LOG4CXX_ERROR(logger, strerror(errno));
    throw DBFileException(errno);
  }
  LOG4CXX_DEBUG(logger, "read(num: " + TO_STR(num) + " ,pos: " + TO_STR(l) + ")");
  if ((l = read(num, data, getBlockSize())) != getBlockSize()) {
    LOG4CXX_DEBUG(logger, "read : " + TO_STR(l) + ")");
    LOG4CXX_ERROR(logger, strerror(errno));
    throw DBFileException(errno);
  }
}

void DBFileBlock::writeFileBlock() {
  LOG4CXX_INFO(logger, "writeFileBlock()");
  LOG4CXX_DEBUG(logger, "this:\n" + toString("\t"));
  int num = file.getFileNum();
  off_t l = blockNum * getBlockSize();
  LOG4CXX_DEBUG(logger, "lseek(" + TO_STR(l) + ")");
  if (l != lseek(num, l, SEEK_SET)) {
    LOG4CXX_ERROR(logger, strerror(errno));
    throw DBFileException(errno);
  }
  LOG4CXX_DEBUG(logger, "write(num: " + TO_STR(num) + " ,pos: " + TO_STR(l) + ")");
  if (getBlockSize() != write(num, data, getBlockSize())) {
    LOG4CXX_ERROR(logger, strerror(errno));
    throw DBFileException(errno);
  }
}

string DBFileBlock::toString(string linePrefix) const {
  stringstream ss;
  ss << linePrefix << "[DBFileBlock]" << endl;
  ss << linePrefix << "blockNum: " << blockNum << endl;
  ss << linePrefix << "data: " << TO_STR(data) << endl;
  ss << linePrefix << "file: " << endl;
  ss << file.toString(linePrefix + "\t");
  ss << linePrefix << "-------------" << endl;
  return ss.str();
}
