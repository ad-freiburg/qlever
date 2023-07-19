add_subdirectory(ConfigManager)
add_subdirectory(http)
add_library(util GeoSparqlHelpers.cpp
        antlr/ANTLRErrorHandling.cpp ParseException.cpp Conversions.cpp Date.cpp)
qlever_target_link_libraries(util)
