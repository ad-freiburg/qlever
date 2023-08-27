add_library(memorySize MemorySize.cpp
    generated/MemorySizeLanguageLexer.cpp
    generated/MemorySizeLanguageParser.cpp MemorySizeParser.cpp)
qlever_target_link_libraries(memorySize antlr4_static util)
