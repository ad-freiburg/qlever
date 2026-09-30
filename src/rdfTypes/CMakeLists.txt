add_library(rdfEscaping RdfEscaping.cpp)
qlever_target_link_libraries(rdfEscaping stringUtils)

add_library(rdfTypes Iri.cpp Literal.cpp Variable.cpp GeoPoint.cpp GeometryInfo.cpp GeoCellGrid.cpp GeoSparqlHelpers.cpp)
qlever_target_link_libraries(rdfTypes antlrErrorHandling pb_util pb_util_geo pb_util_json rdfEscaping antlrParser s2)

# `util/Views.h` (and hence almost every header of QLever) pulls in
# `CompressedExternalIdTable.h`, which uses HPX when `USE_HPX` is ON. `rdfTypes`
# is the lowest library in the dependency order that is affected, so linking HPX
# here publicly makes it available to everything above it, see the `USE_HPX`
# block in the top-level `CMakeLists.txt`.
if (USE_HPX)
    target_link_libraries(rdfTypes PUBLIC hpx_core)
endif ()
