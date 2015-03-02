add_library(engine
              Engine.h Engine.cpp
              IndexSequence.h
              IndexMock.h IndexMock.cpp
              QueryExecutionTree.h QueryExecutionTree.cpp
              QueryGraph.h QueryGraph.cpp
              Operation.h
              Id.h
              Constants.h
              Comparators.h
              ResultTable.h ResultTable.cpp
              QueryExecutionContext.h
              IndexScan.h IndexScan.cpp
              Join.h Join.cpp
              Sort.h Sort.cpp
              OrderBy.h OrderBy.cpp
            )

target_link_libraries(engine index parser)