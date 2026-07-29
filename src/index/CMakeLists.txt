add_subdirectory(vocabulary)

# The local vocabulary, i.e. the words that are not part of the index's
# vocabulary but are created while a query is evaluated.
#
# NOTE: This has to be a separate library, and it must not depend on `index`:
# comparing two `Id`s calls `LocalVocabEntry::compareThreeWay` and
# `LocalVocabEntry::positionInVocab` (see `global/ValueId.h`), so EVERY library
# that compares `Id`s needs the symbols from `LocalVocabEntry.cpp`. That used to
# require the `index` library, which is why several files that conceptually
# belong to `parser` had to be compiled into `index`. The dependency on the
# actual index is now abstracted away by `LocalVocabContext`.
add_library(localVocab LocalVocabEntry.cpp LocalVocabContext.cpp LocalVocab.cpp)
qlever_target_link_libraries(localVocab basicParser)

add_library(index
        Index.cpp IndexImpl.cpp IndexImpl.Text.cpp
        IndexMetaData.cpp MetaDataHandler.cpp
        LocatedTriples.cpp Permutation.cpp TextMetaData.cpp
        DocsDB.cpp FTSAlgorithms.cpp
        CompressedRelation.cpp
        PatternCreator.cpp ScanSpecification.cpp
        DeltaTriples.cpp TextScoring.cpp TextScoringEnum.cpp TextIndexReadWrite.cpp
        TextIndexBuilder.cpp GraphFilter.cpp IndexRebuilder.cpp GraphNameManager.cpp
        IdTableUtils.cpp ExportIds.cpp TripleComponentConversions.cpp
        CompressedExternalIdTableSorterInstantiations.cpp
        InputFileSpecification.cpp)
qlever_target_link_libraries(index qlever_util basicParser rdfParser vocabulary localVocab global)
