add_library(testUtil IdTableHelpers.cpp ValidatorHelpers.cpp RandomTestHelpers.cpp)
# TODO<c++20> Once there is more support for it, we should be able to do this cheaper with modules.
qlever_target_link_libraries(testUtil engine)
