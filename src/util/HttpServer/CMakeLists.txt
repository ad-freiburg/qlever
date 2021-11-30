add_library(httpServer HttpServer.h HttpUtils.h UrlParser.h UrlParser.cpp MediaTypes.h MediaTypes.cpp)
target_link_libraries(httpServer ${ICU_LIBRARIES})
