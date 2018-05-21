#pragma once

#include <IO/WriteBufferFromFileBase.h>

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
#include <Core/Types.h>

namespace DB
{

/** Use ready file descriptor. Does not open or close a file.
  */
class WriteBufferFromHDFSPlus : public WriteBufferFromFileBase
{

protected:
    void nextImpl() override;
    /// Name or some description of file.
    std::string getFileName() const override;

public:
    WriteBufferFromHDFSPlus(
        Hdfs::OutputStream * osptr_ = nullptr,
        const char * fileName_ = "test", const char * server_ = "test", unsigned short port__ = 9000,
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
        char * existing_memory = nullptr,
        size_t alignment = 0);

    ~WriteBufferFromHDFSPlus() override;

    off_t getPositionInFile() override;

    void sync() override;

    int getFD() const override;

private:

    off_t doSeek(off_t offset, int whence) override;
    void doTruncate(off_t length) override;

    Hdfs::OutputStream * osptr;
    String fileName, server;
    unsigned short port;

};

}
