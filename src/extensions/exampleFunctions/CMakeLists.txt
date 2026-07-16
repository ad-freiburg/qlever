# Example custom-function extension: `increment`/`plus`, self-registering.
add_library(extension_exampleFunctions OBJECT ExampleFunctions.cpp)
qlever_target_link_libraries(extension_exampleFunctions parser sparqlExpressions
        engine global)
set_property(GLOBAL APPEND PROPERTY QLEVER_EXTENSION_OBJLIBS
        extension_exampleFunctions)
