add_library(vocabulary VocabularyInMemory.h VocabularyInMemory.cpp
                       VocabularyInMemoryBinSearch.cpp VocabularyInternalExternal.cpp
                       VocabularyOnDisk.cpp SplitVocabulary.cpp)
qlever_target_link_libraries(vocabulary)
