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
    ql::ranges::for_each(turtles, [&parser](const std::string& turtle) {
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
  // - `numInserted()` and `numDeleted()` and the derived `getCounts()`
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

  // Inserting triples. The triples being inserted must be sorted.
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

  // Insert more triples.
  deltaTriples.insertTriples(
      cancellationHandle,
      makeIdTriples(vocab, localVocab, {"<B> <C> <D>", "<B> <D> <C>"}));
  EXPECT_THAT(deltaTriples,
              StateIs(5, 0, 5,
                      {"<A> <B> <C>", "<A> <B> <D>", "<B> <C> <D>",
                       "<B> <D> <C>", "<A> <low> <a>"},
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

  // Unsorted triples are not allowed.
  if constexpr (ad_utility::areExpensiveChecksEnabled) {
    AD_EXPECT_THROW_WITH_MESSAGE(
        deltaTriples.deleteTriples(
            cancellationHandle,
            makeIdTriples(vocab, localVocab,
                          {"<C> <prev> <B>", "<B> <prev> <A>"})),
        testing::_);
  }

  // Deleting triples.
  deltaTriples.deleteTriples(
      cancellationHandle,
      makeIdTriples(vocab, localVocab, {"<B> <prev> <A>", "<C> <prev> <B>"}));
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

// _____________________________________________________________________________
TEST_F(DeltaTriplesTest, DeltaTriplesManager) {
  // Preparation.
  DeltaTriplesManager deltaTriplesManager(testQec->getIndex().getImpl());
  auto& vocab = testQec->getIndex().getVocab();
  auto cancellationHandle =
      std::make_shared<ad_utility::CancellationHandle<>>();
  std::vector<ad_utility::JThread> threads;
  static constexpr size_t numThreads = 18;
  static constexpr size_t numIterations = 21;

  // Insert and delete a well-defined set of triples, some independent and some
  // dependent on the thread index. Check that the snapshot before in the
  // middle of these updates is as expected.
  auto insertAndDelete = [&](size_t threadIdx) {
    LocalVocab localVocab;
    SharedLocatedTriplesSnapshot beforeUpdate =
        deltaTriplesManager.getCurrentSnapshot();
    for (size_t i = 0; i < numIterations; ++i) {
      // The first triple in both vectors is the same for all threads, the
      // others are exclusive to this thread via the `threadIdx`.
      auto triplesToInsert = makeIdTriples(
          vocab, localVocab,
          {"<A> <B> <C>", absl::StrCat("<A> <B> <D", threadIdx, ">"),
           absl::StrCat("<A> <B> <E", threadIdx, ">")});
      auto triplesToDelete = makeIdTriples(
          vocab, localVocab,
          {"<A> <A> <E>", absl::StrCat("<A> <B> <E", threadIdx, ">"),
           absl::StrCat("<A> <B> <F", threadIdx, ">")});
      // Insert the `triplesToInsert`.
      deltaTriplesManager.modify<void>([&](DeltaTriples& deltaTriples) {
        deltaTriples.insertTriples(cancellationHandle, triplesToInsert);
      });
      // We should have successfully completed an update, so the snapshot
      // pointer should have changed.
      EXPECT_NE(beforeUpdate, deltaTriplesManager.getCurrentSnapshot());
      // Delete the `triplesToDelete`.
      deltaTriplesManager.modify<void>([&](DeltaTriples& deltaTriples) {
        deltaTriples.deleteTriples(cancellationHandle, triplesToDelete);
      });

      // Make some checks in the middle of these updates (while the other
      // threads are likely to be in the middle of their updates as well).
      if (i == numIterations / 2) {
        {
          // None of the thread-exclusive triples should be contained in the
          // original snapshot and this should not change over time. The
          // Boolean argument specifies whether the triple was inserted (`true`)
          // or deleted (`false`).
          const auto& locatedSPO =
              beforeUpdate->getLocatedTriplesForPermutation(Permutation::SPO);
          EXPECT_FALSE(locatedSPO.isLocatedTriple(triplesToInsert.at(1), true));
          EXPECT_FALSE(
              locatedSPO.isLocatedTriple(triplesToInsert.at(1), false));
          EXPECT_FALSE(locatedSPO.isLocatedTriple(triplesToInsert.at(2), true));
          EXPECT_FALSE(
              locatedSPO.isLocatedTriple(triplesToInsert.at(2), false));
          EXPECT_FALSE(locatedSPO.isLocatedTriple(triplesToDelete.at(2), true));
          EXPECT_FALSE(
              locatedSPO.isLocatedTriple(triplesToDelete.at(2), false));
        }
        {
          // Check for several of the thread-exclusive triples that they are
          // properly contained in the current snapshot.
          //
          auto p = deltaTriplesManager.getCurrentSnapshot();
          const auto& locatedSPO =
              p->getLocatedTriplesForPermutation(Permutation::SPO);
          EXPECT_TRUE(locatedSPO.isLocatedTriple(triplesToInsert.at(1), true));
          // This triple is exclusive to the thread and is inserted and then
          // immediately deleted again. The `DeltaTriples` thus only store it
          // as deleted. It might be contained in the original input, hence we
          // cannot simply drop it.
          EXPECT_TRUE(locatedSPO.isLocatedTriple(triplesToInsert.at(2), false));
          EXPECT_TRUE(locatedSPO.isLocatedTriple(triplesToDelete.at(2), false));
        }
      }
    }
  };

  // Run the above for each of `numThreads` threads, where each thread knows
  // its index (used to create the thread-exclusive triples).
  for (size_t i = 0; i < numThreads; ++i) {
    threads.emplace_back(insertAndDelete, i);
  }
  threads.clear();

  // Check that without updates, the snapshot pointer does not change.
  auto p1 = deltaTriplesManager.getCurrentSnapshot();
  auto p2 = deltaTriplesManager.getCurrentSnapshot();
  EXPECT_EQ(p1, p2);

  // Each of the threads above inserts on thread-exclusive triple, deletes one
  // thread-exclusive triple and inserts one thread-exclusive triple that is
  // deleted right after (This triple is stored as deleted in the `DeltaTriples`
  // because it might be contained in the original input). Additionally, there
  // is one common triple inserted by all the threads and one common triple
  // that is deleted by all the threads.
  auto deltaImpl = deltaTriplesManager.deltaTriples_.rlock();
  EXPECT_THAT(*deltaImpl, NumTriples(numThreads + 1, 2 * numThreads + 1,
                                     3 * numThreads + 2));
}
