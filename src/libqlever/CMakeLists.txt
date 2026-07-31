add_library(qlever Qlever.cpp NamedCachedQueryBlobManager.cpp)
qlever_target_link_libraries(qlever sparqlParser engine qlever_util index)
