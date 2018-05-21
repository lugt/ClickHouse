#include <Storages/StorageHDFSPlus.h>
#include <Storages/StorageFactory.h>

#include <Interpreters/Context.h>
#include <Interpreters/evaluateConstantExpression.h>

#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTIdentifier.h>

#include <IO/ReadBufferFromHDFSPlus.h>
#include <IO/WriteBufferFromHDFSPlus.h>
#include <IO/WriteHelpers.h>

#include <DataStreams/FormatFactory.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <DataStreams/IBlockOutputStream.h>

#include <Common/escapeForFileName.h>
#include <Common/typeid_cast.h>

#include <fcntl.h>

#include <Poco/Path.h>
#include <Poco/File.h>

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

static Hdfs::InputStream * open_hdfs_file(const char *file_path,const char *server, short unsigned int port);
static Hdfs::OutputStream * open_hdfs_write(const char *file_path,const char *server, short unsigned int port);


StorageHDFSPlus::StorageHDFSPlus(
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


class StorageHDFSPlusBlockInputStream : public IProfilingBlockInputStream
{
public:

    StorageHDFSPlusBlockInputStream(StorageHDFSPlus & storage_, const Context & context, size_t max_block_size)
        : storage(storage_)
    {
        // storage.rwlock.lock_shared();

	    Hdfs::InputStream *isptr;
        isptr = open_hdfs_file(storage.file_path.c_str(), storage.server_addr.c_str(), storage.server_port);
        if(isptr == nullptr){
            throw Exception("Cannot open hdfs file", ErrorCodes::UNKNOWN_IDENTIFIER);
        }

        read_buf = std::make_unique<ReadBufferFromHDFSPlus>(isptr);
        reader = FormatFactory().getInput(storage.format_name, *read_buf, storage.getSampleBlock(), context, max_block_size);
    }

    ~StorageHDFSPlusBlockInputStream() override
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
    StorageHDFSPlus & storage;
    Block sample_block;
    std::unique_ptr<ReadBufferFromHDFSPlus> read_buf;
    BlockInputStreamPtr reader;
};


BlockInputStreams StorageHDFSPlus::read(
    const Names & /*column_names*/,
    const SelectQueryInfo & /*query_info*/,
    const Context & context,
    QueryProcessingStage::Enum & /*processed_stage*/,
    size_t max_block_size,
    unsigned /*num_streams*/)
{
    // wait, what?
    return BlockInputStreams(1, std::make_shared<StorageHDFSPlusBlockInputStream>(*this, context, max_block_size * 10));
}


class StorageHDFSPlusBlockOutputStream : public IBlockOutputStream
{
public:

    explicit StorageHDFSPlusBlockOutputStream(StorageHDFSPlus & storage_)
        : storage(storage_), lock(storage.rwlock)
    {
        Hdfs::OutputStream * osptr = nullptr;
        try {
            osptr = open_hdfs_write(storage.file_path.c_str(),
                                    storage.server_addr.c_str(),
                                    storage.server_port);
        }catch (...){
            throw Exception("Error : HDFS Plus Writer Init failed : " + storage.file_path + ", "+
                            storage.server_addr, ErrorCodes::CANNOT_WRITE_TO_FILE_DESCRIPTOR);
        }

        if(osptr == nullptr){
            throw Exception("Error : HDFS Plus Writer NullPtr : " + storage.file_path + ", "+
                            storage.server_addr, ErrorCodes::CANNOT_WRITE_TO_FILE_DESCRIPTOR);
        }

        write_buf = std::make_unique<WriteBufferFromHDFSPlus>(osptr,
                storage.file_path.c_str(), storage.server_addr.c_str(), storage.server_port,
                DBMS_DEFAULT_BUFFER_SIZE);
        writer = FormatFactory().getOutput(storage.format_name, *write_buf, storage.getSampleBlock(), storage.context_global);
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
    StorageHDFSPlus & storage;
    std::unique_lock<std::shared_mutex> lock;
    std::unique_ptr<WriteBufferFromHDFSPlus> write_buf;
    BlockOutputStreamPtr writer;
};

BlockOutputStreamPtr StorageHDFSPlus::write(
    const ASTPtr & /*query*/,
    const Settings & /*settings*/)
{
    return std::make_shared<StorageHDFSPlusBlockOutputStream>(*this);
}


void StorageHDFSPlus::drop()
{
    /// Extra actions are not required.
}


void StorageHDFSPlus::rename(const String & new_path_to_db, const String & /*new_database_name*/, const String & new_table_name)
{
    throw Exception("Can't rename table '" + table_name + "' to '"+new_table_name+"' as '"+new_path_to_db+"' , HDFS not implemented", ErrorCodes::DATABASE_ACCESS_DENIED);
}


void registerStorageHDFSPlus(StorageFactory & factory)
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

        return StorageHDFSPlus::create(
                (std::string) server_ip, (std::string) file_path, source_port,
            args.table_name, (std::string) format_name, args.columns,
            args.context);
    });
}

static Hdfs::InputStream * open_hdfs_file(const char *file_path,const char *server, short unsigned int port){
    Hdfs::FileSystem *superfs;
    Hdfs::Config conf("function-test.xml");
    conf.set("output.default.packetsize", 1024);
    std::stringstream ss;
    ss << "hdfs://"<<server << ":" <<port;
    superfs = new Hdfs::FileSystem(conf);
    superfs->connect(ss.str().c_str(), HDFS_SUPERUSER, NULL);
    superfs->setWorkingDirectory(superfs->getWorkingDirectory().c_str());

    Hdfs::InputStream * ins = new Hdfs::InputStream();
    ins->open(*superfs, file_path, false);
    return ins;
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
}
