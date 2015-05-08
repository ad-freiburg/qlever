add_library(engine
              Engine.h Engine.cpp
              IndexSequence.h
              IndexMock.h IndexMock.cpp
              QueryExecutionTree.h QueryExecutionTree.cpp
              QueryGraph.h QueryGraph.cpp
              Operation.h
              ../global/Id.h
              ../global/Constants.h
              Comparators.h
              ResultTable.h ResultTable.cpp
              QueryExecutionContext.h
              IndexScan.h IndexScan.cpp
              Join.h Join.cpp
              Sort.h Sort.cpp
              TextOperationForEntities.h TextOperationForEntities.cpp
              TextOperationForContexts.h TextOperationForContexts.cpp
              Distinct.h Distinct.cpp
              OrderBy.h OrderBy.cpp
              Filter.h Filter.cpp
              Server.h Server.cpp
           )

target_link_libraries(engine index)