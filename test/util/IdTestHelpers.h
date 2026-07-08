//  Copyright 2023, University of Freiburg,
//  //              Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_TEST_UTIL_IDTESTHELPERS_H
#define QLEVER_TEST_UTIL_IDTESTHELPERS_H

#include "IndexTestHelpers.h"
#include "global/Id.h"
#include "index/Index.h"
#include "index/LocalVocab.h"
#include "util/Synchronized.h"

// Lambdas to simply create an `Id` with a given value and type during unit
// tests.
namespace ad_utility::testing {
using qlever::LocalVocabEntry;
using qlever::TextRecordIndex;
using qlever::WordVocabIndex;

inline auto UndefId = []() { return qlever::Id::makeUndefined(); };
inline auto IntId = [](const auto& i) { return qlever::Id::makeFromInt(i); };
inline auto DoubleId = [](const auto& d) {
  return qlever::Id::makeFromDouble(d);
};
inline auto BoolId = [](bool b) { return qlever::Id::makeFromBool(b); };

inline auto DateId = [](const auto& parse, const std::string& dateStr) {
  return qlever::Id::makeFromDate(parse(dateStr));
};

inline auto VocabId = [](const auto& v) {
  return qlever::Id::makeFromVocabIndex(qlever::VocabIndex::make(v));
};

inline auto BlankNodeId = [](const auto& v) {
  return qlever::Id::makeFromBlankNodeIndex(qlever::BlankNodeIndex::make(v));
};

inline auto LocalVocabId = [](std::integral auto v) {
  static ad_utility::Synchronized<qlever::LocalVocab> localVocab;
  using namespace ad_utility::triple_component;
  // Use `getQec()` to obtain a valid `LocalVocabContext` reference for creating
  // `LocalVocabEntry` objects. This works because we store the indices in a
  // static map.
  auto* qec = getQec();
  return qlever::Id::makeFromLocalVocabIndex(
      localVocab.wlock()->getIndexAndAddIfNotContained(
          LocalVocabEntry::literalWithoutQuotes(std::to_string(v),
                                                qec->getLocalVocabContext())));
};

inline auto TextRecordId = [](const auto& t) {
  return qlever::Id::makeFromTextRecordIndex(TextRecordIndex::make(t));
};

inline auto WordVocabId = [](const auto& t) {
  return qlever::Id::makeFromWordVocabIndex(WordVocabIndex ::make(t));
};

inline auto GeoPointId = [](const GeoPoint& v) {
  return qlever::Id::makeFromGeoPoint(v);
};

}  // namespace ad_utility::testing

#endif  // QLEVER_TEST_UTIL_IDTESTHELPERS_H
