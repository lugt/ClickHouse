#include <unistd.h>
#include <errno.h>

#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/CurrentMetrics.h>

#include <IO/WriteBufferFromHDFSPlus.h>
#include <IO/WriteHelpers.h>


namespace ProfileEvents
{
    extern const Event WriteBufferFromHDFSWrite;
    extern const Event WriteBufferFromHDFSWriteFailed;
    extern const Event WriteBufferFromHDFSWriteBytes;
}

namespace CurrentMetrics
{
    extern const Metric Write;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_WRITE_TO_FILE_DESCRIPTOR;
    extern const int CANNOT_FSYNC;
    extern const int CANNOT_SEEK_THROUGH_FILE;
    extern const int CANNOT_TRUNCATE_FILE;
}


void WriteBufferFromHDFSPlus::nextImpl()
{
    if (!offset())
        return;

    size_t bytes_written = 0;
    while (bytes_written != offset())
    {
        ProfileEvents::increment(ProfileEvents::WriteBufferFromFileDescriptorWrite);

        ssize_t res = 0;
        {
            CurrentMetrics::Increment metric_increment{CurrentMetrics::Write};
            try {
                osptr->append(working_buffer.begin(), offset() - bytes_written);
            }catch (...){
                ProfileEvents::increment(ProfileEvents::WriteBufferFromFileDescriptorWriteFailed);
                throwFromErrno("Cannot append(write) to HDFS " + getFileName(), ErrorCodes::CANNOT_WRITE_TO_FILE_DESCRIPTOR);
            }
            res = offset() - bytes_written;
            //res = ::write(fd, working_buffer.begin() + bytes_written, offset() - bytes_written);
        }

        if ((-1 == res || 0 == res) && errno != EINTR)
        {
            ProfileEvents::increment(ProfileEvents::WriteBufferFromFileDescriptorWriteFailed);
            throwFromErrno("Cannot write(zero written) to HDFS " + getFileName(), ErrorCodes::CANNOT_WRITE_TO_FILE_DESCRIPTOR);
        }

        if (res > 0)
            bytes_written += res;
    }

    ProfileEvents::increment(ProfileEvents::WriteBufferFromFileDescriptorWriteBytes, bytes_written);
}


/// Name or some description of file.
std::string WriteBufferFromHDFSPlus::getFileName() const
{
    String ppt;

    std::stringstream ss;
    ss << port;
    ppt = ss.str();

    try {
        return "(HDFS = "+ server + ":" + ppt + "/" + fileName + " # " + toString(osptr->tell()) + ")";
    }catch (...){
        return "(HDFS = "+ server + ":"+ ppt +"/" + fileName +" # [Unknown Length])";
    }
}


WriteBufferFromHDFSPlus::WriteBufferFromHDFSPlus(
    Hdfs::OutputStream * osptr_,
    String & fileName, String & server, unsigned short port,
    size_t buf_size,
    char * existing_memory,
    size_t alignment)
    : fileName(), WriteBufferFromFileBase(buf_size, existing_memory, alignment), osptr(osptr_) {}


WriteBufferFromHDFSPlus::~WriteBufferFromHDFSPlus()
{
    try
    {
        osptr->flush();
        osptr->close();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}


off_t WriteBufferFromHDFSPlus::getPositionInFile()
{
    return osptr->tell();
}


void WriteBufferFromHDFSPlus::sync()
{
    /// If buffer has pending data - write it.
    next();
    /// Request OS to sync data with storage medium.
    try{
        osptr->sync();
    }catch (...){
        throwFromErrno("Cannot HDFS write sync " + getFileName(), ErrorCodes::CANNOT_FSYNC);
    }

    /*int res = fsync(fd);
    if (-1 == res)
         throwFromErrno("Cannot fsync " + getFileName(), ErrorCodes::CANNOT_FSYNC);
     */

}


off_t WriteBufferFromHDFSPlus::doSeek(off_t offset, int whence)
{
    /*//osptr.
    off_t res = 0;
    if(whence == SEEK_SET){
        res = offset;
    }else{
        res = osptr->tell() + offset;
    }
    if (-1 == res)*/
    if(offset == 0 && whence == SEEK_CUR){
        return osptr->tell();
    }
    throwFromErrno("Cannot seek through file " + getFileName(), ErrorCodes::CANNOT_SEEK_THROUGH_FILE);
    //return res;
}


void WriteBufferFromHDFSPlus::doTruncate(off_t length)
{
    osptr->flush();
    //int res = ftruncate(fd, length);
    //if (-1 == res)
    //    throwFromErrno("Cannot truncate file " + getFileName(), ErrorCodes::CANNOT_TRUNCATE_FILE);
}

}
