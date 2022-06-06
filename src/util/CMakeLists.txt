add_subdirectory(HttpServer)
add_library(util GeoSparqlHelpers.h GeoSparqlHelpers.cpp)
target_link_libraries(util absl::strings)
