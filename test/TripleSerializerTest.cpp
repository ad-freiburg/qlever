//   Copyright 2025, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Johannes Kalmbach <kalmbach@informatik.uni-freiburg.de>

#include <gmock/gmock.h>

#include "./util/IdTestHelpers.h"
#include "util/Serializer/TripleSerializer.h"

namespace {
auto I = ad_utility::testing::IntId;
auto V = ad_utility::testing::IntId;
TEST(TripleSerializer, simpleExample) {
  LocalVocab localVocab;
  std::vector<std::vector<Id>> ids;

  ids.emplace_back(std::vector{I(3), I(4), I(7)});
  ids.emplace_back(std::vector{I(1), V(2), V(3)});
  std::string filename = "tripleSerializerTestSimpleExample.dat";
  ad_utility::serializeIds(filename, localVocab, ids);

  ad_utility::BlankNodeManager bm;
  auto [localVocabOut, idsOut] = ad_utility::deserializeIds(filename, &bm);
  EXPECT_EQ(idsOut, ids);
  EXPECT_EQ(localVocabOut.size(), localVocab.size());
}
}  // namespace
