add_subdirectory(vocabulary)

# Note: `RdfParser`, `TripleComponent`, and `AsyncFileBlockDriver` live in
# `src/parser`, but are compiled directly into the `index` library instead of
# into a library of their own. The reason is that a separate library would be a
# cyclic dependency:
# 1. The `index` library needs them, because `IndexImpl` builds an index from
#    RDF input files.
# 2. They in turn need the `index` library, because `TripleComponent::toValueId`
#    and friends look words up in the vocabulary of an `IndexImpl` directly.
# Note: Comparing two `Id`s calls `LocalVocabEntry::compareThreeWay` and
# `LocalVocabEntry::positionInVocab` (see `global/ValueId.h`), so every
# translation unit that compares `Id`s needs `LocalVocabEntry`. This used to be
# a dependency on all of `IndexImpl`, but is now abstracted away by the minimal
# `LocalVocabContext` interface, whose only implementation is
# `LocalVocabContextImpl`.
# TODO<joka921> Move the conversions of a `TripleComponent` that require an index
# out of `TripleComponent`, then move these files into a proper library of their
# own.
add_library(index
        Index.cpp IndexImpl.cpp IndexImpl.Text.cpp
        IndexMetaData.cpp MetaDataHandler.cpp
        ../parser/RdfParser.cpp
        LocatedTriples.cpp Permutation.cpp TextMetaData.cpp
        DocsDB.cpp FTSAlgorithms.cpp
        CompressedRelation.cpp
        PatternCreator.cpp ScanSpecification.cpp ../parser/TripleComponent.cpp
        DeltaTriples.cpp LocalVocabEntry.cpp TextScoring.cpp TextScoringEnum.cpp TextIndexReadWrite.cpp
        TextIndexBuilder.cpp GraphFilter.cpp IndexRebuilder.cpp GraphNameManager.cpp
        IdTableUtils.cpp ExportIds.cpp LocalVocab.cpp LocalVocabContext.cpp LocalVocabContextImpl.cpp
        CompressedExternalIdTableSorterInstantiations.cpp
        InputFileSpecification.cpp ../parser/AsyncFileBlockDriver.cpp
        TripleComponentConversions.cpp)
qlever_target_link_libraries(index qlever_util basicParser vocabulary global)
