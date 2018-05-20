#pragma once#include <Storages/IStorage.h>#include <Poco/File.h>#include <Poco/Path.h>#include <common/logger_useful.h>#include <atomic>#include <shared_mutex>#include <ext/shared_ptr_helper.h>#include "libhdfs++/hdfs.h"#include "asio/ip/tcp.hpp"#include "fs/filesystem.h"#include <iostream>#include <Common/escapeForFileName.h>using namespace std;namespace DB{class StorageHDFSBlockInputStream;class StorageHDFSBlockOutputStream;class StorageHDFS : public ext::shared_ptr_helper<StorageHDFS>, public IStorage{public:    std::string getName() const override    {        return "HDFS";    }    std::string getTableName() const override    {        return table_name;    }    BlockInputStreams read(        const Names & column_names,        const SelectQueryInfo & query_info,        const Context & context,        QueryProcessingStage::Enum & processed_stage,        size_t max_block_size,        unsigned num_streams) override;    BlockOutputStreamPtr write(        const ASTPtr & query,        const Settings & settings) override;    void drop() override;    String getDataPath() const override {        std::stringstream path_addr;        path_addr<<server_addr<<":"<<server_port<<"/"<<file_path;        std::string full_path = path_addr.str();        return full_path;    }    void rename(const String & new_path_to_db, const String & new_database_name, const String & new_table_name) override;protected:    friend class StorageHDFSBlockInputStream;    friend class StorageHDFSBlockOutputStream;    /** there are three / four options (ordered by priority):    - use specified file descriptor if (fd >= 0)    - use specified table_path if it isn't empty    - create own tabale inside data/db/table/    */    StorageHDFS(        const std::string & server_addr_,        const std::string & file_path_,        int server_port_,        const std::string & table_name_,        const std::string & format_name_,        const ColumnsDescription & columns_,        Context & context_);private:    std::string server_addr;    std::string file_path;    int server_port;    std::string table_name;    std::string format_name;    Context & context_global;        std::atomic<bool> table_fd_was_used{false}; /// To detect repeating reads from stdin    off_t table_fd_init_offset = -1;            /// Initial position of fd, used for repeating reads    mutable std::shared_mutex rwlock;    Logger * log = &Logger::get("StorageHDFS");};}