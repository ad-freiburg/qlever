add_subdirectory(HttpParser)
add_library(mediaTypes MediaTypes.h MediaTypes.cpp)
target_compile_options(mediaTypes PUBLIC -Wno-attributes)
qlever_target_link_libraries(mediaTypes util)
add_library(http HttpServer.h HttpClient.h HttpClient.cpp HttpUtils.h HttpUtils.cpp UrlParser.h UrlParser.cpp "HttpParser/AcceptHeaderQleverVisitor.h"
        websocket/WebSocketManager.cpp websocket/WebSocketManager.h
        websocket/WebSocketNotifier.cpp websocket/WebSocketNotifier.h
        websocket/QueryToSocketDistributor.cpp websocket/QueryToSocketDistributor.h
        websocket/WebSocketTracker.cpp websocket/WebSocketTracker.h
        websocket/UpdateFetcher.cpp websocket/UpdateFetcher.h)

qlever_target_link_libraries(http util mediaTypes httpParser OpenSSL::SSL OpenSSL::Crypto)
