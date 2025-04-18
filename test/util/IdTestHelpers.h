//  Copyright 2023, University of Freiburg,
//  //              Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_TEST_UTIL_IDTESTHELPERS_H
#define QLEVER_TEST_UTIL_IDTESTHELPERS_H

#include "engine/LocalVocab.h"
#include "global/Id.h"
#include "util/Synchronized.h"

// Lambdas to simply create an `Id` with a given value and type during unit
// tests.
namespace ad_utility::testing {

inline auto UndefId = []() { return Id::makeUndefined(); };
inline auto IntId = [](const auto& i) { return Id::makeFromInt(i); };
inline auto DoubleId = [](const auto& d) { return Id::makeFromDouble(d); };
inline auto BoolId = [](bool b) { return Id::makeFromBool(b); };

inline auto DateId = [](const auto& parse, const std::string& dateStr) {
  return Id::makeFromDate(parse(dateStr));
};

inline auto VocabId = [](const auto& v) {
  return Id::makeFromVocabIndex(VocabIndex::make(v));
};

inline auto BlankNodeId = [](const auto& v) {
  return Id::makeFromBlankNodeIndex(BlankNodeIndex::make(v));
};

inline auto LocalVocabId = [](std::integral auto v) {
  static ad_utility::Synchronized<LocalVocab> localVocab;
  using namespace ad_utility::triple_component;
  return Id::makeFromLocalVocabIndex(
      localVocab.wlock()->getIndexAndAddIfNotContained(
          LiteralOrIri::literalWithoutQuotes(std::to_string(v))));
};

inline auto TextRecordId = [](const auto& t) {
  return Id::makeFromTextRecordIndex(TextRecordIndex ::make(t));
};

inline auto WordVocabId = [](const auto& t) {
  return Id::makeFromWordVocabIndex(WordVocabIndex ::make(t));
};
}  // namespace ad_utility::testing

#endif  // QLEVER_TEST_UTIL_IDTESTHELPERS_H
