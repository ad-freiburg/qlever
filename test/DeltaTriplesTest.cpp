// Copyright 2023 - 2024, University of Freiburg
//  Chair of Algorithms and Data Structures.
//  Authors:
//    2023 Hannah Bast <bast@cs.uni-freiburg.de>
//    2024 Julian Mundhahs <mundhahj@tf.uni-freiburg.de>

#include <gtest/gtest.h>

#include "./DeltaTriplesTestHelpers.h"
#include "./util/GTestHelpers.h"
#include "./util/IndexTestHelpers.h"
#include "absl/strings/str_split.h"
#include "engine/ExportQueryExecutionTrees.h"
#include "index/DeltaTriples.h"
#include "index/IndexImpl.h"
#include "index/Permutation.h"
#include "parser/RdfParser.h"

using namespace deltaTriplesTestHelpers;

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
      "<C> <prev> <B> . "
      "<anon> <x> _:blubb";

  // Query execution context with index for testing, see `IndexTestHelpers.h`.
  QueryExecutionContext* testQec = ad_utility::testing::getQec(testTurtle);

  // Make `TurtleTriple` from given Turtle input.
  std::vector<TurtleTriple> makeTurtleTriples(
      const std::vector<std::string>& turtles) {
    RdfStringParser<TurtleParser<Tokenizer>> parser;
    std::ranges::for_each(turtles, [&parser](const std::string& turtle) {
      parser.parseUtf8String(turtle);
    });
    AD_CONTRACT_CHECK(parser.getTriples().size() == turtles.size());
    return parser.getTriples();
  }

  // Make `IdTriple` from given Turtle input (the LocalVocab is not `const`
  // because we might change it).
  std::vector<IdTriple<0>> makeIdTriples(
      const Index::Vocab& vocab, LocalVocab& localVocab,
      const std::vector<std::string>& turtles) {
    auto toID = [&localVocab, &vocab](TurtleTriple triple) {
      std::array<Id, 4> ids{
          std::move(triple.subject_).toValueId(vocab, localVocab),
          std::move(TripleComponent(triple.predicate_))
              .toValueId(vocab, localVocab),
          std::move(triple.object_).toValueId(vocab, localVocab),
          std::move(triple.graphIri_).toValueId(vocab, localVocab)};
      return IdTriple<0>(ids);
    };
    return ad_utility::transform(
        makeTurtleTriples(turtles),
        [&toID](TurtleTriple triple) { return toID(std::move(triple)); });
  }
};

// Test clear after inserting or deleting a few triples.
TEST_F(DeltaTriplesTest, clear) {
  auto cancellationHandle =
      std::make_shared<ad_utility::CancellationHandle<>>();

  DeltaTriples deltaTriples(testQec->getIndex());
  auto& vocab = testQec->getIndex().getVocab();
  auto& localVocab = deltaTriples.localVocab();

  EXPECT_THAT(deltaTriples, NumTriples(0, 0, 0));

  // Insert then clear.
  deltaTriples.insertTriples(
      cancellationHandle, makeIdTriples(vocab, localVocab, {"<a> <UPP> <A>"}));

  EXPECT_THAT(deltaTriples, NumTriples(1, 0, 1));

  deltaTriples.clear();

  EXPECT_THAT(deltaTriples, NumTriples(0, 0, 0));

  // Delete, insert and then clear.
  deltaTriples.deleteTriples(
      cancellationHandle, makeIdTriples(vocab, localVocab, {"<A> <low> <a>"}));
  EXPECT_THAT(deltaTriples, NumTriples(0, 1, 1));

  deltaTriples.insertTriples(
      cancellationHandle, makeIdTriples(vocab, localVocab, {"<a> <UPP> <A>"}));

  EXPECT_THAT(deltaTriples, NumTriples(1, 1, 2));

  deltaTriples.clear();

  EXPECT_THAT(deltaTriples, NumTriples(0, 0, 0));
}

TEST_F(DeltaTriplesTest, insertTriplesAndDeleteTriples) {
  DeltaTriples deltaTriples(testQec->getIndex());
  auto& vocab = testQec->getIndex().getVocab();
  auto& localVocab = deltaTriples.localVocab();

  auto cancellationHandle =
      std::make_shared<ad_utility::CancellationHandle<>>();

  auto mapKeys = [](auto& map) {
    return ad_utility::transform(map,
                                 [](const auto& item) { return item.first; });
  };
  auto UnorderedTriplesAre = [&mapKeys, this, &vocab, &localVocab](
                                 const std::vector<std::string>& triples)
      -> testing::Matcher<const ad_utility::HashMap<
          IdTriple<0>, DeltaTriples::LocatedTripleHandles>&> {
    return testing::ResultOf(
        "mapKeys(...)", [&mapKeys](const auto map) { return mapKeys(map); },
        testing::UnorderedElementsAreArray(
            makeIdTriples(vocab, localVocab, triples)));
  };
  // A matcher that checks the state of a `DeltaTriples`:
  // - `numInserted()` and `numDeleted()`
  // - `numTriples()` for all `LocatedTriplesPerBlock`
  // - the inserted and deleted triples (unordered)
  auto StateIs = [&UnorderedTriplesAre](
                     size_t numInserted, size_t numDeleted,
                     size_t numTriplesInAllPermutations,
                     const std::vector<std::string>& inserted,
                     const std::vector<std::string>& deleted)
      -> testing::Matcher<const DeltaTriples&> {
    return AllOf(
        NumTriples(numInserted, numDeleted, numTriplesInAllPermutations),
        AD_FIELD(DeltaTriples, triplesInserted_, UnorderedTriplesAre(inserted)),
        AD_FIELD(DeltaTriples, triplesDeleted_, UnorderedTriplesAre(deleted)));
  };

  EXPECT_THAT(deltaTriples, StateIs(0, 0, 0, {}, {}));

  // Inserting triples.
  deltaTriples.insertTriples(
      cancellationHandle,
      makeIdTriples(vocab, localVocab, {"<A> <B> <C>", "<A> <B> <D>"}));
  EXPECT_THAT(deltaTriples,
              StateIs(2, 0, 2, {"<A> <B> <C>", "<A> <B> <D>"}, {}));

  // We only locate triples in a Block but don't resolve whether they exist.
  // Inserting triples that exist in the index works normally.
  deltaTriples.insertTriples(
      cancellationHandle, makeIdTriples(vocab, localVocab, {"<A> <low> <a>"}));
  EXPECT_THAT(
      deltaTriples,
      StateIs(3, 0, 3, {"<A> <B> <C>", "<A> <B> <D>", "<A> <low> <a>"}, {}));

  // Inserting unsorted triples works.
  deltaTriples.insertTriples(
      cancellationHandle,
      makeIdTriples(vocab, localVocab, {"<B> <D> <C>", "<B> <C> <D>"}));
  EXPECT_THAT(deltaTriples,
              StateIs(5, 0, 5,
                      {"<A> <B> <C>", "<A> <B> <D>", "<B> <D> <C>",
                       "<B> <C> <D>", "<A> <low> <a>"},
                      {}));

  // Inserting already inserted triples has no effect.
  deltaTriples.insertTriples(cancellationHandle,
                             makeIdTriples(vocab, localVocab, {"<A> <B> <C>"}));
  EXPECT_THAT(deltaTriples,
              StateIs(5, 0, 5,
                      {"<A> <B> <C>", "<A> <B> <D>", "<B> <C> <D>",
                       "<A> <low> <a>", "<B> <D> <C>"},
                      {}));

  // Deleting a previously inserted triple removes it from the inserted
  // triples and adds it to the deleted ones.
  deltaTriples.deleteTriples(cancellationHandle,
                             makeIdTriples(vocab, localVocab, {"<A> <B> <D>"}));
  EXPECT_THAT(deltaTriples, StateIs(4, 1, 5,
                                    {"<A> <B> <C>", "<B> <C> <D>",
                                     "<A> <low> <a>", "<B> <D> <C>"},
                                    {"<A> <B> <D>"}));

  // Deleting triples.
  deltaTriples.deleteTriples(
      cancellationHandle,
      makeIdTriples(vocab, localVocab, {"<A> <next> <B>", "<B> <next> <C>"}));
  EXPECT_THAT(
      deltaTriples,
      StateIs(4, 3, 7,
              {"<A> <B> <C>", "<B> <C> <D>", "<A> <low> <a>", "<B> <D> <C>"},
              {"<A> <B> <D>", "<A> <next> <B>", "<B> <next> <C>"}));

  // Deleting non-existent triples.
  deltaTriples.deleteTriples(cancellationHandle,
                             makeIdTriples(vocab, localVocab, {"<A> <B> <F>"}));
  EXPECT_THAT(
      deltaTriples,
      StateIs(
          4, 4, 8,
          {"<A> <B> <C>", "<B> <C> <D>", "<A> <low> <a>", "<B> <D> <C>"},
          {"<A> <B> <D>", "<A> <B> <F>", "<A> <next> <B>", "<B> <next> <C>"}));

  // Deleting unsorted triples.
  deltaTriples.deleteTriples(
      cancellationHandle,
      makeIdTriples(vocab, localVocab, {"<C> <prev> <B>", "<B> <prev> <A>"}));
  EXPECT_THAT(
      deltaTriples,
      StateIs(4, 6, 10,
              {"<A> <B> <C>", "<B> <C> <D>", "<A> <low> <a>", "<B> <D> <C>"},
              {"<A> <B> <D>", "<A> <B> <F>", "<A> <next> <B>", "<B> <next> <C>",
               "<C> <prev> <B>", "<B> <prev> <A>"}));

  // Deleting previously deleted triples.
  deltaTriples.deleteTriples(
      cancellationHandle, makeIdTriples(vocab, localVocab, {"<A> <next> <B>"}));
  EXPECT_THAT(
      deltaTriples,
      StateIs(4, 6, 10,
              {"<A> <B> <C>", "<B> <C> <D>", "<A> <low> <a>", "<B> <D> <C>"},
              {"<A> <B> <D>", "<A> <B> <F>", "<A> <next> <B>", "<B> <next> <C>",
               "<C> <prev> <B>", "<B> <prev> <A>"}));

  // Inserting previously deleted triple.
  deltaTriples.insertTriples(cancellationHandle,
                             makeIdTriples(vocab, localVocab, {"<A> <B> <F>"}));
  EXPECT_THAT(deltaTriples,
              StateIs(5, 5, 10,
                      {"<A> <B> <C>", "<A> <B> <F>", "<B> <C> <D>",
                       "<A> <low> <a>", "<B> <D> <C>"},
                      {"<A> <B> <D>", "<A> <next> <B>", "<B> <next> <C>",
                       "<C> <prev> <B>", "<B> <prev> <A>"}));
}

// Test the rewriting of local vocab entries and blank nodes.
TEST_F(DeltaTriplesTest, rewriteLocalVocabEntriesAndBlankNodes) {
  // Create a triple with a new local vocab entry and a new blank node. Use the
  // same new blank node twice (as object ID and graph ID, not important) so
  // that we can test that both occurrences are rewritten to the same new blank
  // node.
  DeltaTriples deltaTriples(testQec->getIndex());
  auto& vocab = testQec->getIndex().getVocab();
  LocalVocab localVocabOutside;
  auto triples =
      makeIdTriples(vocab, localVocabOutside, {"<A> <notInVocab> <B>"});
  AD_CORRECTNESS_CHECK(triples.size() == 1);
  triples[0].ids_[2] =
      Id::makeFromBlankNodeIndex(BlankNodeIndex::make(999'888'777));
  triples[0].ids_[3] = triples[0].ids_[2];
  auto [s1, p1, o1, g1] = triples[0].ids_;

  // Rewrite the IDs in the triple.
  deltaTriples.rewriteLocalVocabEntriesAndBlankNodes(triples);
  auto [s2, p2, o2, g2] = triples[0].ids_;

  // The subject <A> is part of the global vocabulary, so it remains unchanged.
  EXPECT_EQ(s2.getBits(), s1.getBits());

  // The predicate `<notInVocab>` is part of the local vocab, so it gets
  // rewritten, hence the `EXPECT_NE(p2, p1)`. The `EXPECT_EQ(p1, p2)` tests
  // that the strings are equal (which they should be).
  ASSERT_TRUE(p1.getDatatype() == Datatype::LocalVocabIndex);
  ASSERT_TRUE(p2.getDatatype() == Datatype::LocalVocabIndex);
  EXPECT_EQ(p1, p2);
  EXPECT_NE(p2.getBits(), p1.getBits());

  // Test that the rewritten ID is stored (and thereby kept alive) by the
  // local vocab of the `DeltaTriples` class.
  auto& localVocab = deltaTriples.localVocab_;
  auto idx = p2.getLocalVocabIndex();
  EXPECT_EQ(idx, localVocab.getIndexOrNullopt(*idx));

  // Check that the blank node is rewritten (it gets a new blank node index,
  // and hence also a new ID).
  ASSERT_TRUE(o1.getDatatype() == Datatype::BlankNodeIndex);
  ASSERT_TRUE(o2.getDatatype() == Datatype::BlankNodeIndex);
  EXPECT_NE(o2, o1);
  EXPECT_NE(o2.getBits(), o1.getBits());

  // Same for the graph blank node.
  ASSERT_TRUE(g1.getDatatype() == Datatype::BlankNodeIndex);
  ASSERT_TRUE(g2.getDatatype() == Datatype::BlankNodeIndex);
  EXPECT_NE(g2, g1);
  EXPECT_NE(g2.getBits(), g1.getBits());

  // The object and the graph ID were the same blank node, so they should
  // be rewritten to the same new ID.
  EXPECT_EQ(g1.getBits(), o1.getBits());
  EXPECT_EQ(g2.getBits(), o2.getBits());

  // If we rewrite the already written triples again, nothing should change,
  // as the `LocalVocab` of the `DeltaTriples` class is aware that it already
  // stores the corresponding values.
  deltaTriples.rewriteLocalVocabEntriesAndBlankNodes(triples);
  ASSERT_EQ(triples.size(), 1);
  auto [s3, p3, o3, g3] = triples[0].ids_;
  EXPECT_EQ(s3.getBits(), s2.getBits());
  EXPECT_EQ(p3.getBits(), p2.getBits());
  EXPECT_EQ(o3.getBits(), o2.getBits());
  EXPECT_EQ(g3.getBits(), g2.getBits());

  // If we use a local blank node that is already part of the global vocabulary,
  // nothing gets rewritten either.
  auto blank0 = Id::makeFromBlankNodeIndex(BlankNodeIndex::make(0));
  triples[0].ids_[0] = blank0;
  deltaTriples.rewriteLocalVocabEntriesAndBlankNodes(triples);
  auto s4 = triples[0].ids_[0];
  EXPECT_EQ(s4.getBits(), blank0.getBits());
}
