add_subdirectory(vocabulary)
add_library(index
        Index.h Index.cpp Index.Text.cpp
        Vocabulary.h Vocabulary.cpp
        VocabularyGenerator.h VocabularyGeneratorImpl.h
        ConstantsIndexBuilding.h
        VocabularyOnDisk.h VocabularyOnDisk.cpp
        IndexMetaData.h IndexMetaDataImpl.h
        MetaDataHandler.h
        StxxlSortFunctors.h
        TextMetaData.cpp TextMetaData.h
        DocsDB.cpp DocsDB.h
        FTSAlgorithms.cpp FTSAlgorithms.h
        PrefixHeuristic.cpp PrefixHeuristic.h
        CompressedRelation.h CompressedRelation.cpp
        PatternCreator.h PatternCreator.cpp)

target_link_libraries(index parser vocabulary ${STXXL_LIBRARIES} ${ICU_LIBRARIES} absl::flat_hash_map absl::flat_hash_set zstd)
