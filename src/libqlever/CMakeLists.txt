add_library(qlever Qlever.cpp)
qlever_target_link_libraries(qlever parser engine util index absl::strings)
