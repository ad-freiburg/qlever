//  Copyright 2025, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Christoph Ullinger <ullingec@cs.uni-freiburg.de>

#ifndef QLEVER_SRC_INDEX_VOCABULARY_VOCABULARYCONSTRAINTS_H
#define QLEVER_SRC_INDEX_VOCABULARY_VOCABULARYCONSTRAINTS_H

#include "index/vocabulary/CompressedVocabulary.h"
#include "index/vocabulary/SplitVocabulary.h"
#include "index/vocabulary/VocabularyInMemory.h"
#include "index/vocabulary/VocabularyInternalExternal.h"
#include "util/TypeTraits.h"

// This header contains type constraints used to ensure that the correct
// semantics of complex vocabulary types, like the `SplitVocabulary`, are
// preserved if new vocabulary implementations or new instantiations of the
// nested vocabulary types are added to QLever.

// Forward declaration for concepts below.
class PolymorphicVocabulary;

// Only the `SplitVocabulary` currently needs a special handling for
// `getPositionOfWord` (this includes the `PolymorphicVocabulary` which may
// dynamically hold a `SplitVocabulary`)
template <typename T>
CPP_concept HasSpecialGetPositionOfWord =
    std::is_same_v<T, PolymorphicVocabulary> ||
    ad_utility::isInstantiation<T, SplitVocabulary>;

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
// Note: Currently, this concept is identical with `HasDefaultGetPositionOfWord`
// by coincidence. However, both concepts are semantically different.
template <typename T>
CPP_concept NeverProvidesGeometryInfo =
    ad_utility::SameAsAny<T, VocabularyInMemory, VocabularyInternalExternal,
                          CompressedVocabulary<VocabularyInMemory>,
                          CompressedVocabulary<VocabularyInternalExternal>>;

// A variadic version of `NeverProvidesGeometryInfo` that guarantees the
// semantics of the named concept for all of its template parameters `Ts...`.
template <typename... Ts>
CPP_concept AllNeverProvideGeometryInfo =
    (... && NeverProvidesGeometryInfo<Ts>);

#endif  // QLEVER_SRC_INDEX_VOCABULARY_VOCABULARYCONSTRAINTS_H
