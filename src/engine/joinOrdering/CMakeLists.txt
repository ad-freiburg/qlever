add_library(joinOrdering QueryGraph.cpp IKKBZ.cpp RelationBasic.cpp EdgeInfo.cpp CostASI.cpp)
qlever_target_link_libraries(joinOrdering engine)
