add_subdirectory(HttpParser)
add_library(mediaTypes MediaTypes.h MediaTypes.cpp)
target_compile_options(mediaTypes PUBLIC -Wno-attributes)
target_link_libraries(mediaTypes ${ICU_LIBRARIES} absl::flat_hash_map)
add_library(httpServer HttpServer.h HttpUtils.h UrlParser.h UrlParser.cpp "HttpParser/AcceptHeaderQleverVisitor.h")

target_link_libraries(httpServer mediaTypes httpParser ${ICU_LIBRARIES})
