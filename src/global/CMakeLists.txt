add_library(global RuntimeParameters.cpp)
# `RuntimeParameters.h` uses `ad_utility::ParseableDuration` via `Parameters.h`.
qlever_target_link_libraries(global qlever_util)
