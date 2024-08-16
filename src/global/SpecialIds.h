//  Copyright 2023, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_SPECIALIDS_H
#define QLEVER_SPECIALIDS_H

#include "global/Constants.h"
#include "global/Id.h"
#include "util/HashMap.h"
#include "util/HashSet.h"

namespace qlever {

// A mapping from special builtin IRIs that are not managed via the normal
// vocabulary to the IDs that are used to represent them. These IDs all have the
// `Undefined` datatype s.t. they do not accidentally interfere with other IDs.
inline const ad_utility::HashMap<std::string, Id>& specialIds() {
  static const auto ids = []() {
    ad_utility::HashMap<std::string, Id> result{
        {HAS_PREDICATE_PREDICATE, Id::fromBits(1)},
        {HAS_PATTERN_PREDICATE, Id::fromBits(2)},
        // TODO<joka921> Those shouldn't be `int`, but we also need to
        // make sure that the `undefined` stuff doesn't interfere...
        {std::string{DEFAULT_GRAPH_IRI}, Id::makeFromInt(3)},
        {INTERNAL_GRAPH_IRI, Id::makeFromInt(4)}};

    // Perform the following checks: All the special IDs are unique, all of them
    // have the `Undefined` datatype, but none of them is equal to the "actual"
    // UNDEF value.
    auto values = std::views::values(result);
    // TODO<joka921> Reinstate (see above)
    /*
    auto undefTypeButNotUndefValue = [](Id id) {
      return id != Id::makeUndefined() &&
             id.getDatatype() == Datatype::Undefined;
    };
    AD_CORRECTNESS_CHECK(
       std::ranges::all_of(values, undefTypeButNotUndefValue));
    */
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

#endif  // QLEVER_SPECIALIDS_H
