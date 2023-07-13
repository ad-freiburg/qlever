add_library(abstractMemory Memory.cpp
    generated/MemoryDefinitionLanguageLexer.cpp
    generated/MemoryDefinitionLanguageParser.cpp MemoryDefinitionLanguageVisitor.cpp)
qlever_target_link_libraries(abstractMemory antlr4_static util)
