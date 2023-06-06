add_library(configManager ConfigManager.cpp
    ConfigOption.cpp generated/ConfigShorthandLexer.cpp
    generated/ConfigShorthandParser.cpp ConfigShorthandVisitor.cpp ConfigToString.cpp ConfigUtils.cpp)
qlever_target_link_libraries(configManager antlr4_static util)
