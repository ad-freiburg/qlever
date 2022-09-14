add_subdirectory(HttpServer)
add_library(util GeoSparqlHelpers.h GeoSparqlHelpers.cpp VisitMixin.h
        antlr/ANTLRErrorHandling.cpp antlr/ANTLRErrorHandling.h Conversions.cpp)
target_link_libraries(util absl::strings)
