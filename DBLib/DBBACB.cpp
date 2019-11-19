#include <hubDB/DBBACB.h>

using namespace HubDB::Buffer;

LoggerPtr DBBACB::logger(Logger::getLogger("HubDB.Buffer.DBBACB"));

string DBBACB::toString(string linePrefix) const
{
	stringstream ss;
	ss << linePrefix << "[DBBACB]"<<endl;
	ss << linePrefix << "bcb:\n"<<bcb.toString(linePrefix+"\t");
	ss << linePrefix << "--------" <<endl;
	return ss.str();
}
