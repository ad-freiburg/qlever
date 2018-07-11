
add_library(index
        Index.h Index.cpp Index.Text.cpp
        Vocabulary.h Vocabulary.cpp
        VocabularyGenerator.h VocabularyGenerator.cpp
        ConstantsIndexCreation.h
        ExternalVocabulary.h ExternalVocabulary.cpp
        IndexMetaData.h IndexMetaData.cpp
        StxxlSortFunctors.h
        TextMetaData.cpp TextMetaData.h
        DocsDB.cpp DocsDB.h
        FTSAlgorithms.cpp FTSAlgorithms.h)

target_link_libraries(index parser ${STXXL_LIBRARIES})
