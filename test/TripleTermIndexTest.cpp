// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Prashanth Premakumar <prashanthp0703@gmail.com>, UFR

// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gtest/gtest.h>

#include <stdexcept>
#include <type_traits>

#include "index/TripleTermIndex.h"

namespace {
using namespace qlever::tripleTermPrototype;
using Builder = TripleTermIndexBuilder;
// Components are global `Id`s; most tests use vocabulary IRIs.
constexpr auto v = [](uint64_t value) {
  return Id::makeFromVocabIndex(VocabIndex::make(value));
};
static_assert(!std::is_convertible_v<TripleTermIndex, VocabIndex>);

TEST(TripleTermIndex, IdentityAndReverseLookup) {
  Builder builder;
  Builder::Triple triple{v(1), v(2), v(3)};
  auto first = builder.intern(triple);
  EXPECT_EQ(first, builder.intern(triple));
  EXPECT_EQ(builder.size(), 1);
  EXPECT_EQ(builder.resolve(first), triple);
  // Every component participates in identity, including its position.
  EXPECT_NE(first, builder.intern({v(4), v(2), v(3)}));
  EXPECT_NE(first, builder.intern({v(1), v(4), v(3)}));
  EXPECT_NE(first, builder.intern({v(1), v(2), v(4)}));
  EXPECT_NE(first, builder.intern({v(3), v(2), v(1)}));
  EXPECT_EQ(builder.size(), 5);
  EXPECT_THROW(builder.resolve(TripleTermIndex::make(5)), std::out_of_range);
}

TEST(TripleTermIndex, RuntimeIdsPreserveTheReferenceType) {
  Builder builder;
  Builder::Triple triple{v(1), v(2), v(3)};
  Id id = builder.internId(triple);
  EXPECT_EQ(id.getDatatype(), Datatype::TripleTermIndex);
  EXPECT_EQ(builder.resolve(id), triple);
  EXPECT_EQ(builder.internId(triple), id);
  EXPECT_THROW(builder.resolve(Id::makeFromInt(0)), std::invalid_argument);
  EXPECT_THROW(builder.resolve(v(0)), std::invalid_argument);
  const std::vector<std::array<Id, 2>> expected{{v(1), id}};
  EXPECT_EQ(builder.scanIds(v(2), v(3)), expected);
}

TEST(TripleTermIndex, ScanBoundariesAndDeduplication) {
  Builder builder;
  EXPECT_TRUE(builder.scan(v(2), v(3)).empty());
  auto alice = builder.intern({v(1), v(2), v(3)});
  builder.intern({v(1), v(2), v(4)});  // Different object.
  builder.intern({v(1), v(1), v(3)});  // Predicate before range.
  builder.intern({v(1), v(3), v(3)});  // Predicate after range.
  auto dave = builder.intern({v(5), v(2), v(3)});
  builder.intern({v(1), v(2), v(3)});  // No duplicate scan row.
  std::vector<Builder::Match> expected{{v(1), alice}, {v(5), dave}};
  EXPECT_EQ(builder.scan(v(2), v(3)), expected);
  EXPECT_TRUE(builder.scan(v(2), v(9)).empty());
  EXPECT_TRUE(builder.scan(v(9), v(3)).empty());
}

TEST(TripleTermIndex, JoinPreservesAllReportsForSharedTerm) {
  Builder builder;
  auto bob = builder.intern({v(1), v(2), v(3)});
  auto charlie = builder.intern({v(1), v(2), v(4)});
  auto repeated = builder.intern({v(1), v(2), v(3)});
  // Prototype outer relation, not yet QLever's ordinary index or planner.
  std::vector<std::pair<Id, TripleTermIndex>> reports{
      {v(10), bob}, {v(11), repeated}, {v(12), charlie}};
  std::vector<std::pair<Id, Id>> results;
  for (const auto& [person, term] : builder.scan(v(2), v(3))) {
    for (const auto& [report, object] : reports) {
      if (object == term) {
        results.emplace_back(report, person);
      }
    }
  }
  const std::vector<std::pair<Id, Id>> expected{{v(10), v(1)}, {v(11), v(1)}};
  EXPECT_EQ(results, expected);
}

TEST(TripleTermIndex, AcceptsEncodedIrisAndRejectsOtherComponents) {
  Builder builder;
  // An IRI encoded directly into the `Id` is a valid component. It is a
  // different IRI than the vocabulary entry with the same payload bits.
  auto encoded = Id::makeFromEncodedVal(3);
  auto withEncoded = builder.intern({v(1), v(2), encoded});
  EXPECT_NE(withEncoded, builder.intern({v(1), v(2), v(3)}));
  EXPECT_EQ(builder.resolve(withEncoded),
            (Builder::Triple{v(1), v(2), encoded}));
  const std::vector<Builder::Match> expected{{v(1), withEncoded}};
  EXPECT_EQ(builder.scan(v(2), encoded), expected);
  // Literals (here a folded number), blank nodes and nested term references
  // are outside the non-nested, IRI-only scope. Rejection leaves no entry.
  const auto nested = builder.internId({v(4), v(5), v(6)});
  ASSERT_EQ(builder.size(), 3);
  for (Id unsupported :
       {Id::makeFromInt(3), Id::makeFromBlankNodeIndex(BlankNodeIndex::make(3)),
        nested, Id::makeUndefined()}) {
    SCOPED_TRACE(unsupported);
    EXPECT_THROW(builder.intern({v(1), v(2), unsupported}),
                 std::invalid_argument);
    EXPECT_THROW(builder.intern({unsupported, v(2), v(3)}),
                 std::invalid_argument);
  }
  EXPECT_EQ(builder.size(), 3);
}
}  // namespace
