/********************************************************************
 * Copyright (c) 2013 - 2014, Pivotal Inc.
 * All rights reserved.
 *
 * Author: Zhanwei Wang
 ********************************************************************/
/********************************************************************
 * 2014 -
 * open source under Apache License Version 2.0
 ********************************************************************/
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

#include "client/FileSystem.h"
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

using namespace Hdfs;
using namespace Hdfs::Internal;
using namespace std;

shared_ptr<Hdfs::FileSystem> superfs;
shared_ptr<Hdfs::FileSystem>fs;

#define  ASSERT_THROW(x,b) x;
#define ASSERT_NO_THROW(x) x;
#define  EXPECT_TRUE(x) x;
#define  EXPECT_THROW(x,b) x;
#define EXPECT_NO_THROW(x) x;
#define  TEST_F(a,b) void b()
#define ASSERT_TRUE(a) if(!a) exit(0);
#define ASSERT_EQ(a,b) if (a!=b) exit(0);
#define EXPECT_EQ(a,b) if (a!=b) exit(0);

#define BASE_DIR TEST_HDFS_PREFIX"/testFileSystem/"

class TestFileSystem{
public:
    TestFileSystem() :
            conf("function-test.xml") {
        conf.set("output.default.packetsize", 1024);
        conf.set("rpc.client.ping.interval", 1000);
        fs = shared_ptr<FileSystem>(new FileSystem(conf));
        superfs = shared_ptr<FileSystem>(new FileSystem(conf));
        fs->connect();
        superfs->connect(conf.getString("dfs.default.uri"), HDFS_SUPERUSER, NULL);
        superfs->setWorkingDirectory(fs->getWorkingDirectory().c_str());

        try {
            superfs->deletePath(BASE_DIR, true);
        } catch (...) {
        }

        superfs->mkdirs(BASE_DIR, 0755);
        superfs->setOwner(BASE_DIR, USER, NULL);
    }

    ~TestFileSystem() {
        try {
            superfs->deletePath(BASE_DIR, true);
            fs->disconnect();
            superfs->disconnect();
        } catch (...) {
        }
    }

protected:
    Config conf;
    //shared_ptr<FileSystem> fs;
    //shared_ptr<FileSystem> superfs;
};

#ifndef TEST_HDFS_PREFIX
#define TEST_HDFS_PREFIX "./"
#endif

class TestInputStream{
public:
    TestInputStream() :   conf("function-test.xml")
    {
        conf.set("output.default.packetsize", 1024);
        fs = new FileSystem(conf);
        fs->connect();
        superfs = new FileSystem(conf);
        superfs->connect(conf.getString("dfs.default.uri"), HDFS_SUPERUSER, NULL);
        conf.set("dfs.client.read.shortcircuit", false);
        remotefs = new FileSystem(conf);
        remotefs->connect();
        superfs->setWorkingDirectory(fs->getWorkingDirectory().c_str());

        try {
            superfs->deletePath(BASE_DIR, true);
        } catch (...) {
        }

        superfs->mkdirs(BASE_DIR, 0755);
        superfs->setOwner(TEST_HDFS_PREFIX, USER, NULL);
        superfs->setOwner(BASE_DIR, USER, NULL);
        ous.open(*fs, BASE_DIR"smallfile", Create | Overwrite, 0644, true, 0, 2048);
        char buffer1[10], buffer2[20 * 2048];
        FillBuffer(buffer1, sizeof(buffer1), 0);
        FillBuffer(buffer2, sizeof(buffer2), 0);
        ous.append(buffer1, sizeof(buffer1));
        ous.close();
        ous.open(*fs, BASE_DIR"largefile", Create, 0644, false, 0, 2048);
        ous.append(buffer2, sizeof(buffer2));
        ous.close();
    }

    ~TestInputStream() {
        try {
            superfs->deletePath(BASE_DIR, true);
        } catch (...) {
        }

        fs->disconnect();
        delete fs;
        superfs->disconnect();
        delete superfs;
        remotefs->disconnect();
        delete remotefs;
    }

    void OpenFailed(FileSystem & tfs) {
        ASSERT_THROW(ins.open(tfs, "", true), InvalidParameter);
        FileSystem fs1(conf);
        ASSERT_THROW(ins.open(fs1, BASE_DIR"smallfile", true), HdfsIOException);
        ASSERT_NO_THROW(ins.open(tfs, BASE_DIR"smallfile", true));
    }

    void OpenforRead(FileSystem & tfs) {
        EXPECT_THROW(ins.open(tfs, BASE_DIR"a", true), FileNotFoundException);
        char buff[100];
        ASSERT_THROW(ins.read(buff, 100), HdfsIOException);
        ins.close();
    }
    void Read(FileSystem & tfs, size_t size) {
        char buff[2048];
        ins.open(tfs, BASE_DIR"largefile", true);
        ASSERT_NO_THROW(ins.read(buff, size));
        EXPECT_TRUE(CheckBuffer(buff, size, 0));
        ASSERT_NO_THROW(ins.seek(0));
        ASSERT_NO_THROW(ins.read(buff, size - 100));
        EXPECT_TRUE(CheckBuffer(buff, size - 100, 0));

        if (size == 2048) {
            ASSERT_NO_THROW(ins.seek(0));
            ASSERT_NO_THROW(ins.read(buff, size + 100));
            EXPECT_TRUE(CheckBuffer(buff, size, 0));
            ASSERT_NO_THROW(ins.seek(2));
            ASSERT_NO_THROW(ins.read(buff, 100));
            EXPECT_TRUE(CheckBuffer(buff, 100, 2));
        } else {
            ASSERT_NO_THROW(ins.seek(0));
            ASSERT_NO_THROW(ins.read(buff, size + 100));
            EXPECT_TRUE(CheckBuffer(buff, size + 100, 0));
        }

        ins.close();
    }
    void Seek(FileSystem & tfs) {
        ins.open(tfs, BASE_DIR"smallfile", true);
        char buff[1024];
        ASSERT_NO_THROW(ins.read(buff, 100));
        ASSERT_THROW(ins.read(buff, 1024), HdfsEndOfStream);
        ASSERT_NO_THROW(ins.seek(0));
        ASSERT_NO_THROW(ins.read(buff, 100));
        ins.close();
        ins.open(tfs, BASE_DIR"smallfile", true);
        ASSERT_NO_THROW(ins.read(buff, 100));
        ASSERT_NO_THROW(ins.seek(0));
        ASSERT_NO_THROW(ins.read(buff, 100));
        ASSERT_NO_THROW(ins.seek(0));
        ASSERT_NO_THROW(ins.seek(10));
        ASSERT_THROW(ins.read(buff, 1), HdfsEndOfStream);
        ins.close();
        ASSERT_THROW(ins.seek(12), HdfsIOException);
        ins.open(tfs, BASE_DIR"smallfile", true);
        ASSERT_THROW(ins.seek(12), HdfsIOException);
        ins.close();
        ins.open(tfs, BASE_DIR"largefile", true);
        ASSERT_NO_THROW(ins.seek(1027));
        ASSERT_NO_THROW(ins.read(buff, 100));
        ins.close();
    }

    void CheckSum(FileSystem & tfs) {
        ins.open(tfs, BASE_DIR"largefile", false);
        std::vector<char> buff(10240);
        ASSERT_NO_THROW(ins.read(&buff[0], 512));
        ASSERT_NO_THROW(ins.seek(0));
        ASSERT_NO_THROW(ins.read(&buff[0], 1049));
        ins.close();
        ins.open(tfs, BASE_DIR"smallfile", false);
        ASSERT_THROW(ins.seek(13), HdfsIOException);
        ins.close();
    }

    void ReadFully(FileSystem & tfs, size_t size) {
        ins.open(tfs, BASE_DIR"largefile", false);
        char buff[20 * 2048 + 1];
        ASSERT_NO_THROW(ins.readFully(buff, size));
        EXPECT_TRUE(CheckBuffer(buff, size, 0));
        ASSERT_NO_THROW(ins.seek(0));
        ASSERT_NO_THROW(ins.readFully(buff, size - 100));
        EXPECT_TRUE(CheckBuffer(buff, size - 100, 0));
        ASSERT_NO_THROW(ins.seek(0));
        ASSERT_NO_THROW(ins.readFully(buff, size + 100));
        EXPECT_TRUE(CheckBuffer(buff, size + 100, 0));
        ASSERT_THROW(ins.readFully(buff, 20 * 2048 + 1), HdfsIOException);
        ins.close();
    }

protected:
    Config conf;
    FileSystem * fs;
    FileSystem * superfs;
    FileSystem * remotefs;  //test remote block reader
    InputStream ins;
    OutputStream ous;
};


int main(){
    chdir(DATA_DIR);
    TestFileSystem tfs;
    TestInputStream tis;
    tis.Read(*fs,100000);
    return 0;
}

/*int main(int argc, char ** argv) {
    cout << "OK" << endl;
    return 0;
}*/
