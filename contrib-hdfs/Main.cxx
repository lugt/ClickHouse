
#include <atomic>
#include <shared_mutex>

#include "client/InputStream.h"
#include "client/OutputStream.h"
#include "DateTime.h"
#include "Exception.h"
#include "ExceptionInternal.h"
#include "Logger.h"
#include "Memory.h"
#include "TestUtil.h"
#include "Thread.h"
#include "XmlConfig.h"

#include <memory>
#include <inttypes.h>
#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <vector>

#define O_ACCMODE	   0003
#define O_RDONLY	     00
#define O_WRONLY	     01
#define O_RDWR		     02
# define O_CREAT	   0100	/* Not fcntl.  */
# define O_APPEND	  02000

int main(){
    Hdfs::OutputStream * pps =  open_hdfs_write("/a.ccc","117.107.234.121",8020);
}

static Hdfs::OutputStream * open_hdfs_write(const char *file_path,const char *server, short unsigned int port){
    Hdfs::FileSystem *superfs;
    Hdfs::Config conf("function-test.xml");
    conf.set("output.default.packetsize", 1024);

    std::stringstream ss;
    ss << "hdfs://"<<server << ":" <<port;

    superfs = new Hdfs::FileSystem(conf);
    superfs->connect(ss.str().c_str(), HDFS_SUPERUSER, NULL);
    superfs->setWorkingDirectory(superfs->getWorkingDirectory().c_str());

    Hdfs::OutputStream * ous = new Hdfs::OutputStream();
    ous->open(*superfs, file_path, O_WRONLY | O_APPEND | O_CREAT, 0666, false, 0, 2048);
    return ous;
}