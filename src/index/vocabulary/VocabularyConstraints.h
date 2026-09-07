//  Copyright 2025, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Christoph Ullinger <ullingec@cs.uni-freiburg.de>

#ifndef QLEVER_SRC_INDEX_VOCABULARY_VOCABULARYCONSTRAINTS_H
#define QLEVER_SRC_INDEX_VOCABULARY_VOCABULARYCONSTRAINTS_H

#include "index/vocabulary/CompressedVocabulary.h"
#include "index/vocabulary/SplitVocabulary.h"
#include "index/vocabulary/VocabularyInMemory.h"
#include "index/vocabulary/VocabularyInMemoryBinSearch.h"
#include "index/vocabulary/VocabularyInternalExternal.h"
#include "util/Serializer/ByteBufferSerializer.h"
#include "util/TypeTraits.h"

// This header contains type constraints used to ensure that the correct
// semantics of complex vocabulary types, like the `SplitVocabulary`, are
// preserved if new vocabulary implementations or new instantiations of the
// nested vocabulary types are added to QLever.

// Forward declaration for concepts below.
class PolymorphicVocabulary;

// The `SplitVocabulary` and the vocabularies with "holes" (see
// `VocabularyInMemoryBinSearch`) currently need a special handling for
// `getPositionOfWord` (this includes the `PolymorphicVocabulary` which may
// dynamically hold one of those). For a vocabulary with holes, the generic
// implementation would use `size()` as the "one past the end" index, which
// because of the holes is in general much smaller than the largest index that
// the vocabulary contains, so such a vocabulary provides its own
// `getPositionOfWord`.
// Note: This concept is related to, but broader than, the
// `CompressedVocabulary::underlyingHasHoles` constant (see
// `CompressedVocabulary.h`), which only states that the underlying vocabulary
// of a `CompressedVocabulary` has holes. In particular, the `SplitVocabulary`
// needs a special `getPositionOfWord` for a completely different reason.
template <typename T>
CPP_concept HasSpecialGetPositionOfWord =
    ad_utility::isInstantiation<T, SplitVocabulary> ||
    ad_utility::SameAsAny<T, PolymorphicVocabulary, VocabularyInMemoryBinSearch,
                          CompressedVocabulary<VocabularyInMemoryBinSearch>>;

// As a safeguard for the future: Concept that a vocabulary does NOT require a
// special handling for `getPositionOfWord`. Note that `CompressedVocabulary`
// may not be checked via `isInstantiation` here because we do not know about
// the requirements for `getPositionOfWord` of its underlying vocabulary in
// general.
template <typename T>
CPP_concept HasDefaultGetPositionOfWord =
    ad_utility::SameAsAny<T, VocabularyInMemory, VocabularyInternalExternal,
                          CompressedVocabulary<VocabularyInMemory>,
                          CompressedVocabulary<VocabularyInternalExternal>>;

// This concept states that the given vocabulary implementation `T` might
// provide precomputed `GeometryInfo` via a `getGeoInfo` method (for example,
// because an underlying vocabulary might be a `GeoVocabulary`). This does not
// guarantee that such information is actually available. However, if a class
// `T` satisfies this concept it is required to have a member function
// `isGeoInfoAvailable` to determine for sure.
template <typename T>
CPP_concept MaybeProvidesGeometryInfo =
    std::is_same_v<T, PolymorphicVocabulary> ||
    ad_utility::isInstantiation<T, SplitVocabulary> ||
    ad_utility::isInstantiation<T, GeoVocabulary>;

// As a safeguard for the future: This concept states that a vocabulary
// implementation will never provide precomputed `GeometryInfo` via a
// `getGeoInfo` method. A vocabulary class should only be added if it can be
// GUARANTEED that this will be the case.
// Note: This concept is very similar to `HasDefaultGetPositionOfWord`, but the
// two are semantically different (a vocabulary with holes needs a special
// `getPositionOfWord`, but still never provides geometry information).
template <typename T>
CPP_concept NeverProvidesGeometryInfo =
    ad_utility::SameAsAny<T, VocabularyInMemory, VocabularyInternalExternal,
                          VocabularyInMemoryBinSearch,
                          CompressedVocabulary<VocabularyInMemory>,
                          CompressedVocabulary<VocabularyInternalExternal>,
                          CompressedVocabulary<VocabularyInMemoryBinSearch>>;

// A variadic version of `NeverProvidesGeometryInfo` that guarantees the
// semantics of the named concept for all of its template parameters `Ts...`.
template <typename... Ts>
CPP_concept AllNeverProvideGeometryInfo =
    (... && NeverProvidesGeometryInfo<Ts>);

// A vocabulary implementation supports "zero-copy" (de)serialization if it (or,
// recursively, its underlying vocabulary) provides a static
// `fromZeroCopyDeserializer` factory. This currently holds for
// `VocabularyInMemory`, for `VocabularyInMemoryBinSearch` (the in-memory
// vocabulary with holes), and for a `CompressedVocabulary` that wraps a
// zero-copy-capable vocabulary, but not for the disk-backed
// (`VocabularyInternalExternal`, `VocabularyOnDisk`) or split
// (`SplitVocabulary`) vocabularies, which cannot be represented as a single
// contiguous, mmap-friendly blob. The concept is phrased in terms of the
// canonical `AlignedByteBufferReadSerializer`, so that it can be used as a pure
// type-level predicate (independent of the concrete serializer at the call
// site).
template <typename T>
CPP_concept VocabularySupportsZeroCopy =
    ad_utility::serialization::SupportsZeroCopyDeserialization<
        T, ad_utility::serialization::AlignedByteBufferReadSerializer>;

#endif  // QLEVER_SRC_INDEX_VOCABULARY_VOCABULARYCONSTRAINTS_H
