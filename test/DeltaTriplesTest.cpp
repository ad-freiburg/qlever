// Copyright 2023 - 2024, University of Freiburg
//  Chair of Algorithms and Data Structures.
//  Authors:
//    2023 Hannah Bast <bast@cs.uni-freiburg.de>
//    2024 Julian Mundhahs <mundhahj@tf.uni-freiburg.de>

#include <gtest/gtest.h>

#include "./util/GTestHelpers.h"
#include "./util/IndexTestHelpers.h"
#include "absl/strings/str_split.h"
#include "engine/ExportQueryExecutionTrees.h"
#include "index/DeltaTriples.h"
#include "index/IndexImpl.h"
#include "index/Permutation.h"
#include "parser/TurtleParser.h"

namespace Matchers {
inline auto InAllPermutations = [](auto InnerMatcher) {
  return testing::AllOfArray(ad_utility::transform(
      Permutation::ALL, [&InnerMatcher](const Permutation::Enum& perm) {
        return testing::ResultOf(
            absl::StrCat(".getLocatedTriplesPerBlock(",
                         Permutation::toString(perm), ")"),
            [perm](const DeltaTriples& deltaTriples) {
              return deltaTriples.getLocatedTriplesPerBlock(perm);
            },
            InnerMatcher);
      }));
};
inline auto NumTriplesInAllPermutations =
    [](size_t expectedNumTriples) -> testing::Matcher<const DeltaTriples&> {
  return InAllPermutations(AD_PROPERTY(LocatedTriplesPerBlock,
                                       LocatedTriplesPerBlock::numTriples,
                                       testing::Eq(expectedNumTriples)));
};
}  // namespace Matchers
namespace m = Matchers;

// Fixture that sets up a test index.
class DeltaTriplesTest : public ::testing::Test {
 protected:
  // The triples in our test index (as a `std::vector` so that we have easy
  // access to each triple separately in the tests below).
  static constexpr const char* testTurtle =
      "<a> <upp> <A> . "
      "<b> <upp> <B> . "
      "<c> <upp> <C> . "
      "<A> <low> <a> . "
      "<B> <low> <b> . "
      "<C> <low> <c> . "
      "<a> <next> <b> . "
      "<b> <next> <c> . "
      "<A> <next> <B> . "
      "<B> <next> <C> . "
      "<b> <prev> <a> . "
      "<c> <prev> <b> . "
      "<B> <prev> <A> . "
      "<C> <prev> <B>";

  // Query execution context with index for testing, see `IndexTestHelpers.h`.
  QueryExecutionContext* testQec = ad_utility::testing::getQec(testTurtle);

  // Make `TurtleTriple` from given Turtle input.
  std::vector<TurtleTriple> makeTurtleTriples(
      std::vector<std::string> turtles) {
    TurtleStringParser<Tokenizer> parser;
    // turtles is copied here
    std::ranges::for_each(turtles, [&parser](const std::string& turtle) {
      parser.parseUtf8String(turtle);
    });
    AD_CONTRACT_CHECK(parser.getTriples().size() == turtles.size());
    return parser.getTriples();
  }

  // Make `IdTriple` from given Turtle input (the first argument is not `const`
  // because we might change the local vocabulary).
  std::vector<IdTriple<0>> makeIdTriples(DeltaTriples& deltaTriples,
                                         std::vector<std::string> turtles) {
    auto& localVocab = deltaTriples.localVocab();
    auto& vocab = deltaTriples.getIndex().getVocab();
    auto toID = [&localVocab, &vocab](TurtleTriple triple) {
      std::array<Id, 3> ids{
          std::move(triple.subject_).toValueId(vocab, localVocab),
          std::move(TripleComponent(triple.predicate_))
              .toValueId(vocab, localVocab),
          std::move(triple.object_).toValueId(vocab, localVocab)};
      return IdTriple<0>(ids);
    };
    return ad_utility::transform(
        makeTurtleTriples(turtles),
        [&toID](TurtleTriple triple) { return toID(triple); });
  }

  // Check that all six `triplesWithPositionsPerBlock` lists have the given
  // number of `LocatedTriple` objects.
  void checkTriplesWithPositionsPerBlockSize(const DeltaTriples& deltaTriples,
                                             size_t expectedSize) {
    for (Permutation::Enum permutation : Permutation::ALL) {
      ASSERT_EQ(
          deltaTriples.getLocatedTriplesPerBlock(permutation).numTriples(),
          expectedSize);
    }
  }
};

// Test clear after inserting or deleting a few triples.
TEST_F(DeltaTriplesTest, clear) {
  auto cancellationHandle =
      std::make_shared<ad_utility::CancellationHandle<>>();

  DeltaTriples deltaTriples(testQec->getIndex());

  ASSERT_EQ(deltaTriples.numInserted(), 0);
  ASSERT_EQ(deltaTriples.numDeleted(), 0);
  EXPECT_THAT(deltaTriples, m::NumTriplesInAllPermutations(0));

  // Insert then clear.
  deltaTriples.insertTriples(cancellationHandle,
                             makeIdTriples(deltaTriples, {"<a> <UPP> <A>"}));

  ASSERT_EQ(deltaTriples.numInserted(), 1);
  ASSERT_EQ(deltaTriples.numDeleted(), 0);
  EXPECT_THAT(deltaTriples, m::NumTriplesInAllPermutations(1));

  deltaTriples.clear();

  ASSERT_EQ(deltaTriples.numInserted(), 0);
  ASSERT_EQ(deltaTriples.numDeleted(), 0);
  EXPECT_THAT(deltaTriples, m::NumTriplesInAllPermutations(0));

  // Delete, insert and then clear.
  deltaTriples.deleteTriples(cancellationHandle,
                             makeIdTriples(deltaTriples, {"<A> <low> <a>"}));
  ASSERT_EQ(deltaTriples.numInserted(), 0);
  ASSERT_EQ(deltaTriples.numDeleted(), 1);
  EXPECT_THAT(deltaTriples, m::NumTriplesInAllPermutations(1));

  deltaTriples.insertTriples(cancellationHandle,
                             makeIdTriples(deltaTriples, {"<a> <UPP> <A>"}));

  ASSERT_EQ(deltaTriples.numInserted(), 1);
  ASSERT_EQ(deltaTriples.numDeleted(), 1);
  EXPECT_THAT(deltaTriples, m::NumTriplesInAllPermutations(2));

  deltaTriples.clear();

  ASSERT_EQ(deltaTriples.numInserted(), 0);
  ASSERT_EQ(deltaTriples.numInserted(), 0);
  EXPECT_THAT(deltaTriples, m::NumTriplesInAllPermutations(0));
}

TEST_F(DeltaTriplesTest, insertTriplesAndDeleteTriples) {
  DeltaTriples deltaTriples(testQec->getIndex());
  auto cancellationHandle =
      std::make_shared<ad_utility::CancellationHandle<>>();

  auto mapKeys = [](auto& map) {
    return ad_utility::transform(map,
                                 [](const auto& item) { return item.first; });
  };

  ASSERT_EQ(deltaTriples.numInserted(), 0);
  ASSERT_EQ(deltaTriples.numDeleted(), 0);
  EXPECT_THAT(deltaTriples, m::NumTriplesInAllPermutations(0));
  EXPECT_THAT(
      mapKeys(deltaTriples.triplesInserted_),
      testing::UnorderedElementsAreArray(makeIdTriples(deltaTriples, {})));
  EXPECT_THAT(
      mapKeys(deltaTriples.triplesDeleted_),
      testing::UnorderedElementsAreArray(makeIdTriples(deltaTriples, {})));

  // Inserting triples.
  deltaTriples.insertTriples(
      cancellationHandle,
      makeIdTriples(deltaTriples, {"<A> <B> <C>", "<A> <B> <D>"}));
  ASSERT_EQ(deltaTriples.numInserted(), 2);
  ASSERT_EQ(deltaTriples.numDeleted(), 0);
  EXPECT_THAT(deltaTriples, m::NumTriplesInAllPermutations(2));
  EXPECT_THAT(mapKeys(deltaTriples.triplesInserted_),
              testing::UnorderedElementsAreArray(
                  makeIdTriples(deltaTriples, {"<A> <B> <C>", "<A> <B> <D>"})));
  EXPECT_THAT(
      mapKeys(deltaTriples.triplesDeleted_),
      testing::UnorderedElementsAreArray(makeIdTriples(deltaTriples, {})));

  // We only locate triples in a Block but don't resolve whether they exist.
  // Inserting triples that exist in the index works normally.
  deltaTriples.insertTriples(cancellationHandle,
                             makeIdTriples(deltaTriples, {"<A> <low> <a>"}));
  ASSERT_EQ(deltaTriples.numInserted(), 3);
  ASSERT_EQ(deltaTriples.numDeleted(), 0);
  EXPECT_THAT(deltaTriples, m::NumTriplesInAllPermutations(3));
  EXPECT_THAT(
      mapKeys(deltaTriples.triplesInserted_),
      testing::UnorderedElementsAreArray(makeIdTriples(
          deltaTriples, {"<A> <B> <C>", "<A> <B> <D>", "<A> <low> <a>"})));
  EXPECT_THAT(
      mapKeys(deltaTriples.triplesDeleted_),
      testing::UnorderedElementsAreArray(makeIdTriples(deltaTriples, {})));

  // Inserting unsorted triples works.
  deltaTriples.insertTriples(
      cancellationHandle,
      makeIdTriples(deltaTriples, {"<B> <D> <C>", "<B> <C> <D>"}));
  ASSERT_EQ(deltaTriples.numInserted(), 5);
  ASSERT_EQ(deltaTriples.numDeleted(), 0);
  EXPECT_THAT(deltaTriples, m::NumTriplesInAllPermutations(5));
  EXPECT_THAT(mapKeys(deltaTriples.triplesInserted_),
              testing::UnorderedElementsAreArray(makeIdTriples(
                  deltaTriples, {"<A> <B> <C>", "<A> <B> <D>", "<B> <D> <C>",
                                 "<B> <C> <D>", "<A> <low> <a>"})));
  EXPECT_THAT(
      mapKeys(deltaTriples.triplesDeleted_),
      testing::UnorderedElementsAreArray(makeIdTriples(deltaTriples, {})));

  // Inserting already inserted triples has no effect.
  deltaTriples.insertTriples(cancellationHandle,
                             makeIdTriples(deltaTriples, {"<A> <B> <C>"}));
  ASSERT_EQ(deltaTriples.numInserted(), 5);
  ASSERT_EQ(deltaTriples.numDeleted(), 0);
  EXPECT_THAT(deltaTriples, m::NumTriplesInAllPermutations(5));
  EXPECT_THAT(mapKeys(deltaTriples.triplesInserted_),
              testing::UnorderedElementsAreArray(makeIdTriples(
                  deltaTriples, {"<A> <B> <C>", "<A> <B> <D>", "<B> <C> <D>",
                                 "<A> <low> <a>", "<B> <D> <C>"})));
  EXPECT_THAT(
      mapKeys(deltaTriples.triplesDeleted_),
      testing::UnorderedElementsAreArray(makeIdTriples(deltaTriples, {})));

  // Deleting a previously inserted triple removes it from the inserted
  // triples and adds it to the deleted ones.
  deltaTriples.deleteTriples(cancellationHandle,
                             makeIdTriples(deltaTriples, {"<A> <B> <D>"}));
  ASSERT_EQ(deltaTriples.numInserted(), 4);
  ASSERT_EQ(deltaTriples.numDeleted(), 1);
  EXPECT_THAT(deltaTriples, m::NumTriplesInAllPermutations(5));
  EXPECT_THAT(mapKeys(deltaTriples.triplesInserted_),
              testing::UnorderedElementsAreArray(makeIdTriples(
                  deltaTriples, {"<A> <B> <C>", "<B> <C> <D>", "<A> <low> <a>",
                                 "<B> <D> <C>"})));
  EXPECT_THAT(mapKeys(deltaTriples.triplesDeleted_),
              testing::UnorderedElementsAreArray(
                  makeIdTriples(deltaTriples, {"<A> <B> <D>"})));

  // Deleting triples.
  deltaTriples.deleteTriples(
      cancellationHandle,
      makeIdTriples(deltaTriples, {"<A> <next> <B>", "<B> <next> <C>"}));
  ASSERT_EQ(deltaTriples.numInserted(), 4);
  ASSERT_EQ(deltaTriples.numDeleted(), 3);
  EXPECT_THAT(deltaTriples, m::NumTriplesInAllPermutations(7));
  EXPECT_THAT(mapKeys(deltaTriples.triplesInserted_),
              testing::UnorderedElementsAreArray(makeIdTriples(
                  deltaTriples, {"<A> <B> <C>", "<B> <C> <D>", "<A> <low> <a>",
                                 "<B> <D> <C>"})));
  EXPECT_THAT(
      mapKeys(deltaTriples.triplesDeleted_),
      testing::UnorderedElementsAreArray(makeIdTriples(
          deltaTriples, {"<A> <B> <D>", "<A> <next> <B>", "<B> <next> <C>"})));

  // Deleting non-existent triples.
  deltaTriples.deleteTriples(cancellationHandle,
                             makeIdTriples(deltaTriples, {"<A> <B> <F>"}));
  ASSERT_EQ(deltaTriples.numInserted(), 4);
  ASSERT_EQ(deltaTriples.numDeleted(), 4);
  EXPECT_THAT(deltaTriples, m::NumTriplesInAllPermutations(8));
  EXPECT_THAT(mapKeys(deltaTriples.triplesInserted_),
              testing::UnorderedElementsAreArray(makeIdTriples(
                  deltaTriples, {"<A> <B> <C>", "<B> <C> <D>", "<A> <low> <a>",
                                 "<B> <D> <C>"})));
  EXPECT_THAT(mapKeys(deltaTriples.triplesDeleted_),
              testing::UnorderedElementsAreArray(makeIdTriples(
                  deltaTriples, {"<A> <B> <D>", "<A> <B> <F>", "<A> <next> <B>",
                                 "<B> <next> <C>"})));

  // Deleting unsorted triples.
  deltaTriples.deleteTriples(
      cancellationHandle,
      makeIdTriples(deltaTriples, {"<C> <prev> <B>", "<B> <prev> <A>"}));
  ASSERT_EQ(deltaTriples.numInserted(), 4);
  ASSERT_EQ(deltaTriples.numDeleted(), 6);
  EXPECT_THAT(deltaTriples, m::NumTriplesInAllPermutations(10));
  EXPECT_THAT(mapKeys(deltaTriples.triplesInserted_),
              testing::UnorderedElementsAreArray(makeIdTriples(
                  deltaTriples, {"<A> <B> <C>", "<B> <C> <D>", "<A> <low> <a>",
                                 "<B> <D> <C>"})));
  EXPECT_THAT(mapKeys(deltaTriples.triplesDeleted_),
              testing::UnorderedElementsAreArray(makeIdTriples(
                  deltaTriples,
                  {"<A> <B> <D>", "<A> <B> <F>", "<A> <next> <B>",
                   "<B> <next> <C>", "<C> <prev> <B>", "<B> <prev> <A>"})));

  // Deleting previously deleted triples.
  deltaTriples.deleteTriples(cancellationHandle,
                             makeIdTriples(deltaTriples, {"<A> <next> <B>"}));
  ASSERT_EQ(deltaTriples.numInserted(), 4);
  ASSERT_EQ(deltaTriples.numDeleted(), 6);
  EXPECT_THAT(deltaTriples, m::NumTriplesInAllPermutations(10));
  EXPECT_THAT(mapKeys(deltaTriples.triplesInserted_),
              testing::UnorderedElementsAreArray(makeIdTriples(
                  deltaTriples, {"<A> <B> <C>", "<B> <C> <D>", "<A> <low> <a>",
                                 "<B> <D> <C>"})));
  EXPECT_THAT(mapKeys(deltaTriples.triplesDeleted_),
              testing::UnorderedElementsAreArray(makeIdTriples(
                  deltaTriples,
                  {"<A> <B> <D>", "<A> <B> <F>", "<A> <next> <B>",
                   "<B> <next> <C>", "<C> <prev> <B>", "<B> <prev> <A>"})));

  // Inserting previously deleted triple.
  deltaTriples.insertTriples(cancellationHandle,
                             makeIdTriples(deltaTriples, {"<A> <B> <F>"}));
  ASSERT_EQ(deltaTriples.numInserted(), 5);
  ASSERT_EQ(deltaTriples.numDeleted(), 5);
  EXPECT_THAT(deltaTriples, m::NumTriplesInAllPermutations(10));
  EXPECT_THAT(mapKeys(deltaTriples.triplesInserted_),
              testing::UnorderedElementsAreArray(makeIdTriples(
                  deltaTriples, {"<A> <B> <C>", "<A> <B> <F>", "<B> <C> <D>",
                                 "<A> <low> <a>", "<B> <D> <C>"})));
  EXPECT_THAT(
      mapKeys(deltaTriples.triplesDeleted_),
      testing::UnorderedElementsAreArray(makeIdTriples(
          deltaTriples, {"<A> <B> <D>", "<A> <next> <B>", "<B> <next> <C>",
                         "<C> <prev> <B>", "<B> <prev> <A>"})));
}
