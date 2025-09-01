add_library(rdfEscaping RdfEscaping.cpp)
qlever_target_link_libraries(rdfEscaping)

add_library(rdfTypes Iri.cpp Literal.cpp Variable.cpp GeoPoint.cpp GeometryInfo.cpp)
qlever_target_link_libraries(rdfTypes pb_util pb_util_geo rdfEscaping sparqlParser)
