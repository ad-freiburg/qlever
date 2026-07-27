add_subdirectory(vocabulary)
# `RdfParser`, `TripleComponent`, and `AsyncFileBlockDriver` are compiled
# directly into the `index` library (rather than as separate linked libraries)
# because they depend on full index types and structures. Making them standalone
# would create a circular dependency.
add_library(index
        Index.cpp IndexImpl.cpp IndexImpl.Text.cpp EncodedIriManager.cpp
        IndexMetaData.cpp MetaDataHandler.cpp
        Vocabulary.cpp ../parser/RdfParser.cpp
        LocatedTriples.cpp Permutation.cpp TextMetaData.cpp
        DocsDB.cpp FTSAlgorithms.cpp
        PrefixHeuristic.cpp CompressedRelation.cpp
        PatternCreator.cpp ScanSpecification.cpp ../parser/TripleComponent.cpp
        DeltaTriples.cpp LocalVocabEntry.cpp TextScoring.cpp TextScoringEnum.cpp TextIndexReadWrite.cpp
        TextIndexBuilder.cpp GraphFilter.cpp IndexRebuilder.cpp GraphNameManager.cpp
        IdTableUtils.cpp ExportIds.cpp LocalVocab.cpp
        CompressedExternalIdTableSorterInstantiations.cpp
        InputFileSpecification.cpp ../parser/AsyncFileBlockDriver.cpp)
qlever_target_link_libraries(index qlever_util basicParser vocabulary global)
