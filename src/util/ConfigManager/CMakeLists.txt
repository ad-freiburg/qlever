add_library(configManager ConfigManager.cpp
    ConfigOption.cpp generated/ConfigShorthandLexer.cpp
    generated/ConfigShorthandParser.cpp ConfigShorthandVisitor.cpp ConfigUtil.cpp ConfigExceptions.cpp Validator.cpp)
qlever_target_link_libraries(configManager antlr4_static util)
