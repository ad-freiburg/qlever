# All the vocabularies, including the `RdfsVocabulary` (`Vocabulary.cpp`) that
# combines them, and the `EncodedIriManager` (`EncodedIriManager.cpp`) that
# encodes suitable IRIs directly in an `Id` instead of storing them in a
# vocabulary. The latter two used to live in `src/index` and to be compiled into
# the `index` library, but they belong here: neither of them uses a single symbol
# of the `index` library. `PrefixHeuristic.cpp` is only used by
# `CompressedVocabulary.h` and `CompressionWrappers.h`, so it belongs here as
# well.
add_library(vocabulary VocabularyInMemory.h VocabularyInMemory.cpp
                       VocabularyInMemoryBinSearch.cpp VocabularyInternalExternal.cpp
                       VocabularyOnDisk.cpp SplitVocabulary.cpp GeoVocabulary.cpp PolymorphicVocabulary.cpp
                       Vocabulary.cpp EncodedIriManager.cpp PrefixHeuristic.cpp
                       AuxVocabulary.cpp)
qlever_target_link_libraries(vocabulary qlever_util rdfTypes)
