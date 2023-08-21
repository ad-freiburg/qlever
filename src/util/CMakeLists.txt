add_subdirectory(ConfigManager)
add_subdirectory(MemorySize)
add_subdirectory(http)
add_library(util GeoSparqlHelpers.cpp
        antlr/ANTLRErrorHandling.cpp ParseException.cpp Conversions.cpp Date.cpp antlr/GenerateAntlrExceptionMetadata.cpp Rtree.h Rtree.cpp)
qlever_target_link_libraries(util Boost::serialization)
