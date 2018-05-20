#include <Storages/StorageHDFS.h>
#include <Storages/StorageFactory.h>

#include <Interpreters/Context.h>
#include <Interpreters/evaluateConstantExpression.h>

#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTIdentifier.h>

#include <IO/ReadBufferFromHDFS.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteHelpers.h>

#include <DataStreams/FormatFactory.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <DataStreams/IBlockOutputStream.h>

#include <Common/escapeForFileName.h>
#include <Common/typeid_cast.h>

#include <fcntl.h>

#include <Poco/Path.h>
#include <Poco/File.h>

using namespace ::hdfs;

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_WRITE_TO_FILE_DESCRIPTOR;
    extern const int CANNOT_SEEK_THROUGH_FILE;
    extern const int DATABASE_ACCESS_DENIED;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int UNKNOWN_IDENTIFIER;
    extern const int INCORRECT_FILE_NAME;
    extern const int EMPTY_LIST_OF_COLUMNS_PASSED;
};


  static std::string getTablePath(const std::string & db_dir_path, const std::string & table_name, const std::string & format_name)
  {
      return db_dir_path + escapeForFileName(table_name) + "/data." + escapeForFileName(format_name);
  }

  static void checkCreationIsAllowed(Context & context_global)
  {
    static_cast<void>(context_global);
    return;
  }


static InputStream * open_hdfs_file(const char *file_path,const char *server, short unsigned int port);

StorageHDFS::StorageHDFS(
        const std::string & server_addr_,
        const std::string & file_path_,
        int server_port_,
        const std::string & table_name_,
        const std::string & format_name_,
        const ColumnsDescription & columns_,
        Context & context_)
    : IStorage(columns_),
      server_addr(server_addr_), file_path(file_path_), server_port(server_port_),
      table_name(table_name_), format_name(format_name_), context_global(context_)
{
  
}


class StorageHDFSBlockInputStream : public IProfilingBlockInputStream
{
public:

    StorageHDFSBlockInputStream(StorageHDFS & storage_, const Context & context, size_t max_block_size)
        : storage(storage_)
    {
        // storage.rwlock.lock_shared();

	InputStream *isptr;
        isptr = open_hdfs_file(storage.file_path.c_str(), storage.server_addr.c_str(), storage.server_port);
        if(isptr == nullptr){
            throw Exception("Cannot open hdfs file", ErrorCodes::UNKNOWN_IDENTIFIER);
        }

        read_buf = std::make_unique<ReadBufferFromHDFS>(isptr);
        reader = FormatFactory().getInput(storage.format_name, *read_buf, storage.getSampleBlock(), context, max_block_size);
    }

    ~StorageHDFSBlockInputStream() override
    {
        //storage.rwlock.unlock_shared();
    }

    String getName() const override
    {
        return storage.getName();
    }

    Block readImpl() override
    {
        // actually extracting a block, would delayance here matter?
        return reader->read();
    }

    Block getHeader() const override { return reader->getHeader(); };

    void readPrefixImpl() override
    {
        reader->readPrefix();
    }

    void readSuffixImpl() override
    {
        reader->readSuffix();
    }

private:
    StorageHDFS & storage;
    Block sample_block;
    std::unique_ptr<ReadBufferFromHDFS> read_buf;
    BlockInputStreamPtr reader;
};


BlockInputStreams StorageHDFS::read(
    const Names & /*column_names*/,
    const SelectQueryInfo & /*query_info*/,
    const Context & context,
    QueryProcessingStage::Enum & /*processed_stage*/,
    size_t max_block_size,
    unsigned /*num_streams*/)
{
    // wait, what?
    return BlockInputStreams(1, std::make_shared<StorageHDFSBlockInputStream>(*this, context, max_block_size * 10));
}


class StorageHDFSBlockOutputStream : public IBlockOutputStream
{
public:

    explicit StorageHDFSBlockOutputStream(StorageHDFS & storage_)
        : storage(storage_), lock(storage.rwlock)
    {
        write_buf = std::make_unique<WriteBufferFromFile>("/dev/null", DBMS_DEFAULT_BUFFER_SIZE, O_WRONLY | O_APPEND | O_CREAT);
        writer = FormatFactory().getOutput(storage.format_name, *write_buf, storage.getSampleBlock(), storage.context_global);
        throw Exception("Error : not implemented hdfs write", ErrorCodes::UNKNOWN_IDENTIFIER);
    }

    void write(const Block & block) override
    {
        writer->write(block);
    }

    void writePrefix() override
    {
        writer->writePrefix();
    }

    void writeSuffix() override
    {
        writer->writeSuffix();
    }

    void flush() override
    {
        writer->flush();
    }

    Block getHeader() const override { return storage.getSampleBlock(); }

private:
    StorageHDFS & storage;
    std::unique_lock<std::shared_mutex> lock;
    std::unique_ptr<WriteBufferFromFileDescriptor> write_buf;
    BlockOutputStreamPtr writer;
};

BlockOutputStreamPtr StorageHDFS::write(
    const ASTPtr & /*query*/,
    const Settings & /*settings*/)
{
    return std::make_shared<StorageHDFSBlockOutputStream>(*this);
}


void StorageHDFS::drop()
{
    /// Extra actions are not required.
}


void StorageHDFS::rename(const String & new_path_to_db, const String & /*new_database_name*/, const String & new_table_name)
{
    throw Exception("Can't rename table '" + table_name + "' to '"+new_table_name+"' as '"+new_path_to_db+"' , HDFS not implemented", ErrorCodes::DATABASE_ACCESS_DENIED);
}


void registerStorageHDFS(StorageFactory & factory)
{
    factory.registerStorage("HDFS", [](const StorageFactory::Arguments & args)
    {
        ASTs & engine_args = args.engine_args;
        if ((engine_args.size() != 3 && engine_args.size() != 4)){
            throw Exception(
                "Storage HDFS requires 3 or 4 arguments: used format / server addr(or ip) / file path / [optional,=9000] server port",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
        }

        // format
        engine_args[0] = evaluateConstantExpressionOrIdentifierAsLiteral(engine_args[0], args.local_context);
        String format_name = static_cast<const ASTLiteral &>(*engine_args[0]).value.safeGet<String>();

        String server_ip = static_cast<const ASTLiteral &>(*engine_args[1]).value.safeGet<String>();

        String file_path = static_cast<const ASTLiteral &>(*engine_args[2]).value.safeGet<String>();

        int source_port = 9000;
        if(engine_args.size() == 4) {
            if (const ASTLiteral *literal = typeid_cast<const ASTLiteral *>(engine_args[3].get())) {
                auto type = literal->value.getType();
                if (type == Field::Types::Int64)
                    source_port = static_cast<int>(literal->value.get<Int64>());
                else if (type == Field::Types::UInt64)
                    source_port = static_cast<int>(literal->value.get<UInt64>());
            }
        }

        return StorageHDFS::create(
                (std::string) server_ip, (std::string) file_path, source_port,
            args.table_name, (std::string) format_name, args.columns,
            args.context);
    });
}

static IoService * ioService = NULL;

static int hdfs_initialize(){
    ioService = IoService::New();
    auto func = std::bind(& IoService::Run, ioService);
    std::thread first(func);
    first.detach();
    return 0;
}


static InputStream * open_hdfs_file(const char *file_path,const char *server, short unsigned int port){

    if(ioService == NULL){
        hdfs_initialize();
    }




    FileSystem * fsptr;

    auto stat = FileSystem::New(ioService, server, port, &fsptr);
    if (!stat.ok()) {
        LOG_DEBUG(&Logger::get("StorageHDFS"),
                  "Unable to establish connection to server" << server << ":" <<  port << ", stat : " << stat.ToString().c_str());
	  
        throw Exception(
                "StorageHDFS unable to establish connection, failing with stat" + stat.ToString(),
                ErrorCodes::INCORRECT_FILE_NAME);
    }else{
      LOG_DEBUG(&Logger::get("StorageHDFS"), "FileSystem Object Created"<<stat.ToString().c_str());
    }
    InputStream * isptr;
    // single time read
    stat = fsptr->Open(file_path, &isptr);
    if (!stat.ok()) {
        LOG_DEBUG(&Logger::get("StorageHDFS"), "Unable to open file " << file_path << ", " << stat.ToString().c_str());
        throw Exception(
                "StorageHDFS failed : HDFS service reported unable to open file (maybe file not exist?)  " + std::string(file_path) + "[ " + std::string(server) + " ] , stat : " + stat.ToString(),
                ErrorCodes::INCORRECT_FILE_NAME);
    }else{
      LOG_DEBUG(&Logger::get("StorageHDFS"), "Opened HDFS file " << file_path << ", " << stat.ToString().c_str());
    }
    return isptr;
}


}
