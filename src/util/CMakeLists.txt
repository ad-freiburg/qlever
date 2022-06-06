add_subdirectory(HttpServer)
add_library(util GeoSparqlHelpers.h GeoSparqlHelpers.cpp Algorithm.h)
target_link_libraries(util absl::strings)
