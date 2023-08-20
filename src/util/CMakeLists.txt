add_subdirectory(http)
add_library(util GeoSparqlHelpers.h GeoSparqlHelpers.cpp VisitMixin.h
        antlr/ANTLRErrorHandling.cpp antlr/ANTLRErrorHandling.h Conversions.cpp ResetWhenMoved.h Rtree.h Rtree.cpp)
qlever_target_link_libraries(util parser Boost::serialization)
