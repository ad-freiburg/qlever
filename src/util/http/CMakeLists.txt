add_subdirectory(HttpParser)
qlever_add_library(mediaTypes MediaTypes.h MediaTypes.cpp)
target_compile_options(mediaTypes_objects PUBLIC -Wno-attributes)
target_compile_options(mediaTypes PUBLIC -Wno-attributes)
target_compile_options(mediaTypes_shared PUBLIC -Wno-attributes)
qlever_target_link_libraries(mediaTypes util)

if (NOT REDUCED_FEATURE_SET_FOR_CPP17)
qlever_add_library(http HttpServer.h HttpServer.cpp HttpClient.h HttpClient.cpp HttpUtils.h HttpUtils.cpp UrlParser.h UrlParser.cpp "HttpParser/AcceptHeaderQleverVisitor.h"
        websocket/WebSocketSession.cpp websocket/MessageSender.cpp websocket/QueryToSocketDistributor.cpp
        websocket/QueryHub.cpp websocket/UpdateFetcher.cpp)

qlever_target_link_libraries(http parser util mediaTypes httpParser OpenSSL::SSL OpenSSL::Crypto)
endif()
