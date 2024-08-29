//  Copyright 2023, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#pragma once

#include "global/Constants.h"
#include "global/Id.h"
#include "util/HashMap.h"
#include "util/HashSet.h"

namespace qlever {

// A mapping from special builtin IRIs to special IDs. These IDs all have the
// `Undefined` datatype such that they do not accidentally interfere with other
// IDs.
// IMPORTANT: These IDs can only be used in the very first phase of index
// building when handing triples from the parser to the index builder. The
// `VocabularyMerger` assigns "normal" `VocabIndex` IDs for all the entries.
// These VocabIDs have to be retrieved from the vocabulary and used in all
// subsequent phases of the index building and when running the server on a
// built index.
inline const ad_utility::HashMap<std::string, Id>& specialIds() {
  static const auto ids = []() {
    using S = std::string;
    ad_utility::HashMap<std::string, Id> result{
        {S{HAS_PREDICATE_PREDICATE}, Id::fromBits(1)},
        {S{HAS_PATTERN_PREDICATE}, Id::fromBits(2)},
        {S{DEFAULT_GRAPH_IRI}, Id::fromBits(3)},
        {S{INTERNAL_GRAPH_IRI}, Id::fromBits(4)}};

    // Perform the following checks: All the special IDs are unique, all of them
    // have the `Undefined` datatype, but none of them is equal to the "actual"
    // UNDEF value.
    auto values = std::views::values(result);
    auto undefTypeButNotUndefValue = [](Id id) {
      return id != Id::makeUndefined() &&
             id.getDatatype() == Datatype::Undefined;
    };
    AD_CORRECTNESS_CHECK(
        std::ranges::all_of(values, undefTypeButNotUndefValue));
    ad_utility::HashSet<Id> uniqueIds(values.begin(), values.end());
    AD_CORRECTNESS_CHECK(uniqueIds.size() == result.size());
    return result;
  }();
  return ids;
};

// Return the [lowerBound, upperBound) for the special Ids.
// This range can be used to filter them out in cases where we want to ignore
// triples that were added by QLever for internal reasons.
static constexpr std::pair<Id, Id> getBoundsForSpecialIds() {
  constexpr auto upperBound = Id::makeFromBool(false);
  static_assert(static_cast<int>(Datatype::Undefined) == 0);
  static_assert(upperBound.getBits() == 1UL << Id::numDataBits);
  return {Id::fromBits(1), upperBound};
}
}  // namespace qlever
