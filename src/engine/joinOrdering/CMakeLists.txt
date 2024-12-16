add_library(joinOrdering
        QueryGraph.cpp
        JoinTree.cpp JoinNode.cpp
        IKKBZ.cpp
        LinearizedDP.cpp
        RelationBasic.cpp
        EdgeInfo.cpp
        CostIKKBZ.cpp
        CostCout.cpp)
qlever_target_link_libraries(joinOrdering)
