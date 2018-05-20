add_library(common_hdfs hdfs.cc base64.cc datatransfer_sasl.cc sasl_digest_md5.cc status.cc)
add_dependencies(common_hdfs proto)


