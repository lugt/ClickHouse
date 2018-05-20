/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#ifndef LIBHDFSPP_HDFS_H_
#define LIBHDFSPP_HDFS_H_

#include "libhdfs++/status.h"

namespace hdfs {

class IoService {
 public:
  static IoService *New();
  virtual void Run() = 0;
  virtual void Stop() = 0;
  virtual ~IoService();
};


class InputStream {
 public:
  virtual Status PositionRead(void *buf, size_t nbyte, size_t offset, size_t *read_bytes) = 0;
  virtual ~InputStream();
};

class FileSystem {
 public:
  static Status New(IoService *io_service, const char *server,
                    unsigned short port, FileSystem **fsptr);
  virtual Status Open(const char *path, InputStream **isptr) = 0;
  virtual ~FileSystem();
};

}

#endif
