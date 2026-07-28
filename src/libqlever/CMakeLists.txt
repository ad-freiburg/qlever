add_library(qlever Qlever.cpp NamedCachedQueryBlobManager.cpp)
qlever_target_link_libraries(qlever parser engine qlever_util index absl::strings)
