add_subdirectory(HttpParser)
add_library(mediaTypes MediaTypes.h MediaTypes.cpp)
target_compile_options(mediaTypes PUBLIC -Wno-attributes)
qlever_target_link_libraries(mediaTypes util)
add_library(http HttpServer.h HttpClient.h HttpClient.cpp HttpUtils.h HttpUtils.cpp UrlParser.h UrlParser.cpp "HttpParser/AcceptHeaderQleverVisitor.h"
        websocket/WebSocketSession.cpp websocket/MessageSender.cpp websocket/QueryToSocketDistributor.cpp
        websocket/QueryHub.cpp websocket/UpdateFetcher.cpp)

qlever_target_link_libraries(http util mediaTypes httpParser OpenSSL::SSL OpenSSL::Crypto)
