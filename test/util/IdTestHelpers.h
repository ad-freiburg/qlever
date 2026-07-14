//  Copyright 2023, University of Freiburg,
//  //              Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_TEST_UTIL_IDTESTHELPERS_H
#define QLEVER_TEST_UTIL_IDTESTHELPERS_H

#include "IndexTestHelpers.h"
#include "backports/concepts.h"
#include "global/Id.h"
#include "index/Index.h"
#include "index/LocalVocab.h"
#include "util/Synchronized.h"

// Functors to simply create an `Id` with a given value and type during unit
// tests.
//
// NOTE: These are implemented as ordinary functor structs, not as
// (non-capturing) lambdas assigned to `inline auto` variables, because
// non-capturing lambda closure types only became default-constructible in
// C++20 (P0624); under C++17 they are not, which breaks GTest matcher
// machinery elsewhere that needs to default-construct these functors' types.
namespace ad_utility::testing {

struct UndefIdT {
  Id operator()() const { return Id::makeUndefined(); }
};
inline constexpr UndefIdT UndefId;

struct IntIdT {
  Id operator()(const auto& i) const { return Id::makeFromInt(i); }
};
inline constexpr IntIdT IntId;

struct DoubleIdT {
  Id operator()(const auto& d) const { return Id::makeFromDouble(d); }
};
inline constexpr DoubleIdT DoubleId;

struct BoolIdT {
  Id operator()(bool b) const { return Id::makeFromBool(b); }
};
inline constexpr BoolIdT BoolId;

struct DateIdT {
  Id operator()(const auto& parse, const std::string& dateStr) const {
    return Id::makeFromDate(parse(dateStr));
  }
};
inline constexpr DateIdT DateId;

struct VocabIdT {
  Id operator()(const auto& v) const {
    return Id::makeFromVocabIndex(VocabIndex::make(v));
  }
};
inline constexpr VocabIdT VocabId;

struct BlankNodeIdT {
  Id operator()(const auto& v) const {
    return Id::makeFromBlankNodeIndex(BlankNodeIndex::make(v));
  }
};
inline constexpr BlankNodeIdT BlankNodeId;

struct LocalVocabIdT {
  CPP_template(typename T)(requires ql::concepts::integral<T>) Id operator()(
      T v) const {
    static ad_utility::Synchronized<LocalVocab> localVocab;
    using namespace ad_utility::triple_component;
    // Use `getQec()` to obtain a valid `LocalVocabContext` reference for
    // creating `LocalVocabEntry` objects. This works because we store the
    // indices in a static map.
    auto* qec = getQec();
    return Id::makeFromLocalVocabIndex(
        localVocab.wlock()->getIndexAndAddIfNotContained(
            LocalVocabEntry::literalWithoutQuotes(
                std::to_string(v), qec->getLocalVocabContext())));
  }
};
inline constexpr LocalVocabIdT LocalVocabId;

struct TextRecordIdT {
  Id operator()(const auto& t) const {
    return Id::makeFromTextRecordIndex(TextRecordIndex::make(t));
  }
};
inline constexpr TextRecordIdT TextRecordId;

struct WordVocabIdT {
  Id operator()(const auto& t) const {
    return Id::makeFromWordVocabIndex(WordVocabIndex ::make(t));
  }
};
inline constexpr WordVocabIdT WordVocabId;

struct GeoPointIdT {
  Id operator()(const GeoPoint& v) const { return Id::makeFromGeoPoint(v); }
};
inline constexpr GeoPointIdT GeoPointId;

}  // namespace ad_utility::testing

#endif  // QLEVER_TEST_UTIL_IDTESTHELPERS_H
