add_library(vocabulary VocabularyInMemory.h VocabularyInMemory.cpp
                       VocabularyInMemoryBinSearch.cpp VocabularyInternalExternal.cpp
                       VocabularyOnDisk.cpp SplitVocabulary.cpp GeoVocabulary.cpp PolymorphicVocabulary.cpp )
qlever_target_link_libraries(vocabulary util rdfTypes)
