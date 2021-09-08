
add_library(index
        Index.h Index.cpp Index.Text.cpp
        Vocabulary.h Vocabulary.cpp
        VocabularyGenerator.h VocabularyGeneratorImpl.h
        ConstantsIndexBuilding.h
        ExternalVocabulary.h ExternalVocabulary.cpp
        IndexMetaData.h IndexMetaDataImpl.h
        MetaDataTypes.h MetaDataTypes.cpp
        MetaDataHandler.h
        StxxlSortFunctors.h
        TextMetaData.cpp TextMetaData.h
        DocsDB.cpp DocsDB.h
        FTSAlgorithms.cpp FTSAlgorithms.h
        PrefixHeuristic.cpp PrefixHeuristic.h)

target_link_libraries(index parser ${STXXL_LIBRARIES} ${ICU_LIBRARIES} absl::flat_hash_map absl::flat_hash_set)

target_link_libraries(index)
