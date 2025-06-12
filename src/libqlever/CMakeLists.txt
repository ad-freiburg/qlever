add_library(qlever Qlever.cpp)
qlever_target_link_libraries(qlever parser engine util index absl::strings)
add_executable(LibQLeverExample LibQLeverExample.cpp)
qlever_target_link_libraries(LibQLeverExample parser engine util index qlever absl::strings)
