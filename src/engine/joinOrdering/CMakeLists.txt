add_library(joinOrdering
        QueryGraph.cpp
        IKKBZ.cpp
        RelationBasic.cpp
        EdgeInfo.cpp
        CostIKKBZ.cpp
        GOO.cpp)
qlever_target_link_libraries(joinOrdering)
