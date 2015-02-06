add_library(engine
              Engine.h Engine.cpp
              IndexSequence.h
              IndexMock.h
              QueryExecutionTree.h QueryExecutionTree.cpp
              QueryGraph.h QueryGraph.cpp
              Operation.h
              Id.h
              Constants.h
              ResultTable.h
              QueryExecutionContext.h
              IndexScan.h IndexScan.cpp
              Join.h Join.cpp
              Sort.h Sort.cpp
            )