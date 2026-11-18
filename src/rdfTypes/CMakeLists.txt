add_library(rdfEscaping RdfEscaping.cpp)
qlever_target_link_libraries(rdfEscaping stringUtils)

# `LiteralOrIri` is used by `localVocab`, so compile it with the RDF value types
# instead of `basicParser`.
add_library(rdfTypes Iri.cpp Literal.cpp Variable.cpp GeoPoint.cpp GeometryInfo.cpp GeoCellGrid.cpp GeoSparqlHelpers.cpp
        ../parser/LiteralOrIri.cpp)
qlever_target_link_libraries(rdfTypes antlrErrorHandling pb_util pb_util_geo pb_util_json rdfEscaping antlrParser s2)
