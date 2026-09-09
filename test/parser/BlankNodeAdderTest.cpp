// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <absl/strings/str_cat.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "parser/BlankNodeAdder.h"
#include "util/MemorySize/MemorySize.h"

namespace {
using ad_utility::MemorySize;

// _____________________________________________________________________________
TEST(BlankNodeAdderTest, labelsAreResolvedConsistently) {
  ad_utility::BlankNodeManager manager;
  BlankNodeAdder adder{&manager};

  Id b0 = adder.getBlankNodeIndex("_:b0");
  EXPECT_THAT(b0.getDatatype(), testing::Eq(Datatype::BlankNodeIndex));
  EXPECT_THAT(
      adder.localVocab_.isBlankNodeIndexContained(b0.getBlankNodeIndex()),
      testing::IsTrue());

  // The same label always yields the same `Id`, no matter whether it is passed
  // with or without the leading `_:`.
  EXPECT_THAT(adder.getBlankNodeIndex("_:b0"), testing::Eq(b0));
  EXPECT_THAT(adder.getBlankNodeIndexForLabelWithoutPrefix("b0"),
              testing::Eq(b0));
  EXPECT_THAT(adder.getBlankNodeIndexForLabelWithoutPrefix("b1"),
              testing::Ne(b0));
  EXPECT_THAT(adder.map_, testing::SizeIs(2));

  // A label without the leading `_:` violates the precondition of
  // `getBlankNodeIndex`.
  EXPECT_ANY_THROW(adder.getBlankNodeIndex("b0"));

  // A different `BlankNodeAdder` yields different `Id`s for the same labels.
  BlankNodeAdder otherAdder{&manager};
  EXPECT_THAT(otherAdder.getBlankNodeIndex("_:b0"), testing::Ne(b0));
}

// _____________________________________________________________________________
TEST(BlankNodeAdderTest, mappingIsAccountedForInTheMemoryLimit) {
  ad_utility::BlankNodeManager manager;
  BlankNodeAdder adder{
      &manager,
      ad_utility::makeAllocatorWithLimit<BlankNodeAdder::Map::value_type>(
          MemorySize::bytes(512))};

  // Resolving sufficiently many distinct labels exceeds the limit of the
  // mapping. Note that the number of labels required for this depends on the
  // implementation of `std::unordered_map`, so we simply use a number that is
  // large enough for all of them.
  auto addManyBlankNodes = [&adder]() {
    for (size_t i = 0; i < 10'000; ++i) {
      adder.getBlankNodeIndexForLabelWithoutPrefix(absl::StrCat("b", i));
    }
  };
  EXPECT_THROW(addManyBlankNodes(),
               ad_utility::detail::AllocationExceedsLimitException);
}
}  // namespace
