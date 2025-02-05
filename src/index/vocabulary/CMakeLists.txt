add_library(vocabulary VocabularyInMemory.h VocabularyInMemory.cpp VocabularyInMemoryBinSearch.cpp VocabularyInternalExternal.cpp
        PolymorphicVocabulary.cpp
        VocabularyType.h)
qlever_target_link_libraries(vocabulary)
