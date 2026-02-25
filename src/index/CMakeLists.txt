add_subdirectory(vocabulary)
add_library(index
        Index.cpp IndexImpl.cpp IndexImpl.Text.cpp
        Vocabulary.cpp
        LocatedTriples.cpp Permutation.cpp TextMetaData.cpp
        DocsDB.cpp FTSAlgorithms.cpp
        PrefixHeuristic.cpp CompressedRelation.cpp
        PatternCreator.cpp ScanSpecification.cpp ../parser/TripleComponent.cpp
        DeltaTriples.cpp LocalVocabEntry.cpp TextScoring.cpp TextScoringEnum.cpp TextIndexReadWrite.cpp
        TextIndexBuilder.cpp GraphFilter.cpp IndexRebuilder.cpp ExportIds.cpp ../engine/LocalVocab.cpp ../engine/Result.cpp ../engine/Engine.cpp ../parser/WordsAndDocsFileParser.cpp ../parser/LiteralOrIri.cpp)
qlever_target_link_libraries(index util vocabulary global basicParser)
