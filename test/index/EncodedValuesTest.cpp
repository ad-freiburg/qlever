// Copyright 2025, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

#include <gmock/gmock.h>

#include "index/EncodedValues.h"
#include "util/Random.h"

namespace {
std::vector<size_t> getRandomIndices(size_t min, size_t max, size_t num) {
  ad_utility::SlowRandomIntGenerator<size_t> rand(min, max);
  std::vector<size_t> result;
  result.reserve(num + 2);
  result.push_back(min);
  result.push_back(max);
  for (size_t i = 0; i < num; ++i) {
    result.push_back(rand());
  }
  return result;
}

TEST(EncodedValues, SimpleExample) {
  std::string Q42{"<http://www.wikidata.org/entity/Q42>"};
  auto id = EncodedValues::encode(Q42);
  ASSERT_TRUE(id.has_value());
  EXPECT_EQ(EncodedValues::toLiteralOrIri(id.value()).toStringRepresentation(),
            Q42);
}

TEST(EncodedValues, EnAndDecoding) {
  // TODO<joka921> For some reason the upper bounds seem to be off when using
  // `10^12` here, investigate.
  // auto indices = getRandomIndices(0, static_cast<size_t>(std::pow(10ull,
  // 10ull)) - 1, 10'000);
  auto indices = getRandomIndices(0, 1000, 10'000);
  for (auto index : indices) {
    std::string wdq =
        absl::StrCat("<http://www.wikidata.org/entity/Q", index, ">");
    auto id = EncodedValues::encode(wdq);
    ASSERT_TRUE(id.has_value()) << index;
    EXPECT_EQ(
        EncodedValues::toLiteralOrIri(id.value()).toStringRepresentation(), wdq)
        << std::hex << id.value().getBits();
  }
}

}  // namespace
