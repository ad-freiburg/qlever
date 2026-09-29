// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Pascal Keßler <kesslerp@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gtest/gtest.h>

#include <vector>

#include "backports/algorithm.h"
#include "global/Id.h"

// _____________________________________________________________________________
TEST(IdTest, isUndefinedIdMatchesTheMethod) {
  EXPECT_TRUE(isUndefinedId(Id::makeUndefined()));
  EXPECT_EQ(isUndefinedId(Id::makeUndefined()),
            Id::makeUndefined().isUndefined());

  for (Id id : {Id::makeFromInt(0), Id::makeFromInt(-42),
                Id::makeFromDouble(1.5), Id::makeFromBool(true)}) {
    EXPECT_FALSE(isUndefinedId(id));
    EXPECT_EQ(isUndefinedId(id), id.isUndefined());
  }
}

// _____________________________________________________________________________
TEST(IdTest, isDefinedIdIsTheExactOppositeOfIsUndefinedId) {
  for (Id id : {Id::makeUndefined(), Id::makeFromInt(0), Id::makeFromInt(7),
                Id::makeFromDouble(-3.14)}) {
    EXPECT_EQ(isDefinedId(id), !id.isUndefined());
    EXPECT_EQ(isDefinedId(id), !isUndefinedId(id));
  }
}

// _____________________________________________________________________________
TEST(IdTest, compareIdsWithoutLocalVocabMatchesTheMethod) {
  constexpr Id a = Id::makeFromInt(1);
  constexpr Id b = Id::makeFromInt(2);
  constexpr Id c = Id::makeFromInt(1);

  EXPECT_EQ(compareIdsWithoutLocalVocab(a, b), a.compareWithoutLocalVocab(b));
  EXPECT_EQ(compareIdsWithoutLocalVocab(b, a), b.compareWithoutLocalVocab(a));
  EXPECT_EQ(compareIdsWithoutLocalVocab(a, c), a.compareWithoutLocalVocab(c));

  EXPECT_TRUE(compareIdsWithoutLocalVocab(a, b) < 0);
  EXPECT_TRUE(compareIdsWithoutLocalVocab(b, a) > 0);
  EXPECT_TRUE(compareIdsWithoutLocalVocab(a, c) == 0);
}

// _____________________________________________________________________________
TEST(IdTest, isLessThanWithoutLocalVocabMatchesTheMethod) {
  constexpr Id a = Id::makeFromInt(1);
  constexpr Id b = Id::makeFromInt(2);
  constexpr Id c = Id::makeFromInt(1);

  EXPECT_EQ(isLessThanWithoutLocalVocab(a, b),
            a.compareWithoutLocalVocab(b) < 0);
  EXPECT_TRUE(isLessThanWithoutLocalVocab(a, b));
  EXPECT_FALSE(isLessThanWithoutLocalVocab(b, a));
  EXPECT_FALSE(isLessThanWithoutLocalVocab(a, c));
}

// _____________________________________________________________________________
TEST(IdTest, getIdBitsMatchesTheMethod) {
  for (Id id : {Id::makeUndefined(), Id::makeFromInt(42), Id::makeFromInt(-42),
                Id::makeFromDouble(13.37), Id::makeFromBool(true)}) {
    EXPECT_EQ(getIdBits(id), id.getBits());
  }
}

// _____________________________________________________________________________
TEST(IdTest, isUndefinedIdWorksAsAGenericAlgorithmPredicateLikeTheLambdaDid) {
  // The whole point of `isUndefinedId` replacing the local
  // `[](const Id& id) { return id.isUndefined(); }` lambdas at their call
  // sites: passed to a generic algorithm (here `ql::ranges::any_of`), a
  // reference to the global function must behave exactly like the inline
  // lambda it replaced.
  std::vector withoutUndefined{Id::makeFromInt(1), Id::makeFromInt(2),
                               Id::makeFromDouble(3.5)};
  std::vector withUndefined{Id::makeFromInt(1), Id::makeUndefined(),
                            Id::makeFromDouble(3.5)};

  auto isUndefinedLambda = [](const Id& id) { return id.isUndefined(); };

  // 1. The lambda directly
  EXPECT_FALSE(ql::ranges::any_of(withoutUndefined, isUndefinedLambda));
  EXPECT_TRUE(ql::ranges::any_of(withUndefined, isUndefinedLambda));

  // 2. &isUndefinedId, the replacement
  EXPECT_FALSE(ql::ranges::any_of(withoutUndefined, &isUndefinedId));
  EXPECT_TRUE(ql::ranges::any_of(withUndefined, &isUndefinedId));

  for (const auto& ids : {withoutUndefined, withUndefined}) {
    EXPECT_EQ(ql::ranges::any_of(ids, isUndefinedLambda),
              ql::ranges::any_of(ids, &isUndefinedId));
  }
}
