add_library(rdfEscaping RdfEscaping.cpp)
qlever_target_link_libraries(rdfEscaping)

add_library(rdfTypes Iri.cpp Literal.cpp Variable.cpp GeoPoint.cpp GeometryInfo.cpp)
qlever_target_link_libraries(rdfTypes antlrErrorHandling pb_util pb_util_geo pb_util_json rdfEscaping sparqlParser s2)
