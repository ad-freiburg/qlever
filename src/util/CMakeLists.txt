add_subdirectory(HttpServer)
add_library(util GeoSparqlHelpers.h GeoSparqlHelpers.cpp VisitMixin.h
        antlr/ANTLRErrorHandling.cpp antlr/ANTLRErrorHandling.h)
target_link_libraries(util absl::strings)
