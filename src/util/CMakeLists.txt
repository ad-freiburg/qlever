add_subdirectory(http)
add_library(util GeoSparqlHelpers.h GeoSparqlHelpers.cpp VisitMixin.h
        antlr/ANTLRErrorHandling.cpp antlr/ANTLRErrorHandling.h Conversions.cpp ResetWhenMoved.h websocket/WebSocketManager.cpp ../parser/ParseException.cpp)
qlever_target_link_libraries(util parser)
