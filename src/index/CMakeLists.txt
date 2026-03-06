add_subdirectory(vocabulary)
add_library(index
        Index.cpp IndexImpl.cpp IndexImpl.Text.cpp
        Vocabulary.cpp
        LocatedTriples.cpp Permutation.cpp TextMetaData.cpp
        DocsDB.cpp FTSAlgorithms.cpp
        PrefixHeuristic.cpp CompressedRelation.cpp
        PatternCreator.cpp ScanSpecification.cpp
        DeltaTriples.cpp LocalVocabEntry.cpp TextScoring.cpp TextScoringEnum.cpp TextIndexReadWrite.cpp
        TextIndexBuilder.cpp GraphFilter.cpp IndexRebuilder.cpp)
qlever_target_link_libraries(index util parser vocabulary global)
