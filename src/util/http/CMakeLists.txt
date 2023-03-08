add_subdirectory(HttpParser)
add_library(mediaTypes MediaTypes.h MediaTypes.cpp)
target_compile_options(mediaTypes PUBLIC -Wno-attributes)
target_link_libraries(mediaTypes util ${ICU_LIBRARIES} absl::flat_hash_map)
add_library(http HttpServer.h HttpClient.h HttpClient.cpp HttpUtils.h HttpUtils.cpp UrlParser.h UrlParser.cpp "HttpParser/AcceptHeaderQleverVisitor.h")

target_link_libraries(http mediaTypes httpParser ${ICU_LIBRARIES} OpenSSL::SSL OpenSSL::Crypto)
