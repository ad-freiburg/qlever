add_subdirectory(vocabulary)
add_library(index
        Index.cpp IndexImpl.cpp IndexImpl.Text.cpp
        Vocabulary.cpp VocabularyOnDisk.cpp
        Permutation.cpp TextMetaData.cpp
        DocsDB.cpp FTSAlgorithms.cpp
        PrefixHeuristic.cpp CompressedRelation.cpp
        PatternCreator.cpp)
qlever_target_link_libraries(index util parser vocabulary compilationInfo ${STXXL_LIBRARIES})
