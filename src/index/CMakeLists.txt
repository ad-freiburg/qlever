
add_library(index
        Index.h Index.cpp Index.Text.cpp
        Vocabulary.h VocabularyImpl.h
        VocabularyGenerator.h VocabularyGeneratorImpl.h
        ConstantsIndexCreation.h
        ExternalVocabulary.h ExternalVocabulary.cpp
        IndexMetaData.h IndexMetaDataImpl.h
        MetaDataTypes.h MetaDataTypes.cpp
        MetaDataHandler.h
        StxxlSortFunctors.h
        TextMetaData.cpp TextMetaData.h
        DocsDB.cpp DocsDB.h
        FTSAlgorithms.cpp FTSAlgorithms.h
        PrefixHeuristic.cpp PrefixHeuristic.h)

target_link_libraries(index parser ${STXXL_LIBRARIES} ${Boost_LIBRARIES})

add_library(metaConverter
            MetaDataConverter.cpp MetaDataConverter.h)

target_link_libraries(metaConverter index)
