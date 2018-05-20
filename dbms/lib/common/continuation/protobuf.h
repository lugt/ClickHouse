#ifndef LIBHDFSPP_COMMON_COROUTINES_PROTOBUF_H_
#define LIBHDFSPP_COMMON_COROUTINES_PROTOBUF_H_

#include "common/util.h"

#include <google/protobuf/message_lite.h>
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>

#include <cassert>

namespace hdfs {
namespace continuation {

template <class Stream, size_t MaxMessageSize = 512>
struct ReadDelimitedPBMessageContinuation : public Continuation {
  ReadDelimitedPBMessageContinuation(Stream *stream, ::google::protobuf::MessageLite *msg)
      : stream_(stream)
      , msg_(msg)
  {}
  
  virtual void Run(const Next& next) override {
    namespace pbio = google::protobuf::io;
    auto handler = [this,next](const asio::error_code &ec, size_t) {
      Status status;
      if (ec) {
        status = ToStatus(ec);
      } else {
        pbio::ArrayInputStream as(&buf_[0], buf_.size());
        pbio::CodedInputStream is(&as);
        uint32_t size = 0;
        bool v = is.ReadVarint32(&size);
        assert(v);
        is.PushLimit(size);
        msg_->Clear();
        v = msg_->MergeFromCodedStream(&is);
        assert(v);
      }
      next(status);
    };
    asio::async_read(
        *stream_, asio::buffer(buf_),
        std::bind(&ReadDelimitedPBMessageContinuation::CompletionHandler, this,
                  std::placeholders::_1, std::placeholders::_2),
        handler);
  }

 private:
  size_t CompletionHandler(const asio::error_code &ec, size_t transferred) {
    if (ec) {
      return 0;
    }

    size_t offset = 0, len = 0;
    for (size_t i = 0; i + 1 < transferred && i < sizeof(int); ++i) {
      len = (len << 7) | (buf_[i] & 0x7f);
      if ((uint8_t)buf_.at(i) < 0x80) {
        offset = i + 1;
        break;
      }
    }

    assert (offset + len < buf_.size() && "Message is too big");
    return offset ? len + offset - transferred : 1;
  }

  Stream *stream_;
  ::google::protobuf::MessageLite *msg_;
  std::array<char, MaxMessageSize> buf_;
};

template <class Stream>
struct WriteDelimitedPBMessageContinuation : Continuation {
  WriteDelimitedPBMessageContinuation(
      Stream *stream, const google::protobuf::MessageLite *msg)
      : stream_(stream)
      , msg_(msg)
  {}

  virtual void Run(const Next& next) override {
    namespace pbio = google::protobuf::io;
    int size = msg_->ByteSize();
    buf_.reserve(pbio::CodedOutputStream::VarintSize32(size) + size);
    pbio::StringOutputStream ss(&buf_);
    pbio::CodedOutputStream os(&ss);
    os.WriteVarint32(size);
    msg_->SerializeToCodedStream(&os);
    write_coroutine_ = std::shared_ptr<Continuation>(Write(stream_, asio::buffer(buf_)));
    write_coroutine_->Run([next](const Status &stat) { next(stat); });
  }

 private:
  Stream *stream_;
  const google::protobuf::MessageLite * msg_;
  std::string buf_;
  std::shared_ptr<Continuation> write_coroutine_;
};

template<class Stream, size_t MaxMessageSize = 512>
static inline Continuation*
ReadDelimitedPBMessage(Stream *stream, ::google::protobuf::MessageLite *msg) {
  return new ReadDelimitedPBMessageContinuation<Stream, MaxMessageSize>(stream, msg);
}

template<class Stream>
static inline Continuation*
WriteDelimitedPBMessage(Stream *stream, ::google::protobuf::MessageLite *msg) {
  return new WriteDelimitedPBMessageContinuation<Stream>(stream, msg);
}

}
}
#endif
