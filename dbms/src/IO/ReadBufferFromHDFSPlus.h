#pragma once

#include <unistd.h>

#include <sys/fcntl.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/ReadBuffer.h>

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

namespace DB
{

/** Use IoService + FileSystem API with positionRead Functionality.
  */
class ReadBufferFromHDFSPlus : public ReadBufferFromFileBase
{
protected:
    int fd;
    off_t pos_in_file; /// What offset in file corresponds to working_buffer.end().

    bool nextImpl() override;

    /// Name or some description of file.
    std::string getFileName() const override;

public:
    ReadBufferFromHDFSPlus(Hdfs::InputStream * isptr_,
                       size_t buf_size = 10 * DBMS_DEFAULT_BUFFER_SIZE, char * existing_memory = nullptr, size_t alignment = 0)
        : ReadBufferFromFileBase(buf_size, existing_memory, alignment), pos_in_file(0) {
        isptr = isptr_;
    }

    ReadBufferFromHDFSPlus(ReadBufferFromHDFSPlus &&) = default;

    int getFD() const override
    {
        return fd;
    }

    off_t getPositionInFile() override
    {
        return pos_in_file - (working_buffer.end() - pos);
    }

private:
    /// If 'offset' is small enough to stay in buffer after seek, then true seek in file does not happen.
    off_t doSeek(off_t offset, int whence) override;

    Hdfs::InputStream * isptr;
    bool is_last_read_success = true;

    /// Assuming file descriptor supports 'select', check that we have data to read or wait until timeout.
    bool poll(size_t timeout_microseconds);
};

}
