add_library(parserData Iri.cpp BlankNode.cpp)
target_link_libraries(parserData CONAN_PKG::folly)
