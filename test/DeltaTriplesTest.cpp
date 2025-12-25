// Copyright 2023 - 2025 The QLever Authors, in particular:
//
// 2023 - 2025 Hannah Bast <bast@cs.uni-freiburg.de>, UFR
// 2024 - 2025 Julian Mundhahs <mundhahj@tf.uni-freiburg.de>, UFR
// 2024 - 2025 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <absl/strings/str_split.h>
#include <gtest/gtest.h>

#include "./DeltaTriplesTestHelpers.h"
#include "./util/GTestHelpers.h"
#include "./util/IndexTestHelpers.h"
#include "engine/ExportQueryExecutionTrees.h"
#include "index/DeltaTriples.h"
#include "index/IndexImpl.h"
#include "index/Permutation.h"
#include "parser/RdfParser.h"
#include "parser/Tokenizer.h"

namespace {
using namespace deltaTriplesTestHelpers;

constexpr auto encodedIriManager = []() -> const EncodedIriManager* {
  static EncodedIriManager encodedIriManager_;
  return &encodedIriManager_;
};

constexpr auto iri = [](std::string_view s) -> TripleComponent {
  return TripleComponent{TripleComponent::Iri::fromIriref(s)};
};

constexpr auto lit = [](std::string s) -> TripleComponent {
  return TripleComponent{TripleComponent::Literal::fromStringRepresentation(s)};
};
}  // namespace

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
    RdfStringParser<TurtleParser<Tokenizer>> parser{encodedIriManager()};
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
          std::move(triple.subject_)
              .toValueId(vocab, localVocab, *encodedIriManager()),
          std::move(TripleComponent(triple.predicate_))
              .toValueId(vocab, localVocab, *encodedIriManager()),
          std::move(triple.object_)
              .toValueId(vocab, localVocab, *encodedIriManager()),
          std::move(triple.graphIri_)
              .toValueId(vocab, localVocab, *encodedIriManager())};
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
  deltaTriples.insertInternalTriplesForTesting(
      cancellationHandle,
      makeIdTriples(vocab, localVocab,
                    {"<internal-a> <internal-UPP> <internal-A>"}));

  EXPECT_THAT(deltaTriples, NumTriples(1, 0, 1, 1, 0));

  deltaTriples.clear();

  EXPECT_THAT(deltaTriples, NumTriples(0, 0, 0, 0, 0));

  // Delete, insert and then clear.
  deltaTriples.deleteTriples(
      cancellationHandle, makeIdTriples(vocab, localVocab, {"<A> <low> <a>"}));
  deltaTriples.deleteInternalTriplesForTesting(
      cancellationHandle,
      makeIdTriples(vocab, localVocab,
                    {"<internal-A> <internal-low> <internal-a>"}));
  EXPECT_THAT(deltaTriples, NumTriples(0, 1, 1, 0, 1));

  deltaTriples.insertTriples(
      cancellationHandle, makeIdTriples(vocab, localVocab, {"<a> <UPP> <A>"}));
  deltaTriples.insertInternalTriplesForTesting(
      cancellationHandle,
      makeIdTriples(vocab, localVocab,
                    {"<internal-a> <internal-UPP> <internal-A>"}));

  EXPECT_THAT(deltaTriples, NumTriples(1, 1, 2, 1, 1));

  deltaTriples.clear();

  EXPECT_THAT(deltaTriples, NumTriples(0, 0, 0, 0, 0));
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
                                 [[maybe_unused]] auto isInternal,
                                 const std::vector<std::string>& triples)
      -> testing::Matcher<const ad_utility::HashMap<
          IdTriple<0>,
          typename DeltaTriples::TriplesToHandles<
              decltype(isInternal)::value>::LocatedTripleHandles>&> {
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
                     size_t numInternalInserted, size_t numInternalDeleted,
                     const std::vector<std::string>& inserted,
                     const std::vector<std::string>& deleted,
                     const std::vector<std::string>& internalInserted,
                     const std::vector<std::string>& internalDeleted)
      -> testing::Matcher<const DeltaTriples&> {
    using ::testing::AllOf;
    using TriplesNormal = DeltaTriples::TriplesToHandles<false>;
    using TriplesInternal = DeltaTriples::TriplesToHandles<true>;
    return AllOf(
        NumTriples(numInserted, numDeleted, numTriplesInAllPermutations,
                   numInternalInserted, numInternalDeleted),
        AD_FIELD(
            DeltaTriples, triplesToHandlesNormal_,
            AllOf(AD_FIELD(TriplesNormal, triplesInserted_,
                           UnorderedTriplesAre(std::false_type{}, inserted)),
                  AD_FIELD(TriplesNormal, triplesDeleted_,
                           UnorderedTriplesAre(std::false_type{}, deleted)))),
        AD_FIELD(DeltaTriples, triplesToHandlesInternal_,
                 AllOf(AD_FIELD(TriplesInternal, triplesInserted_,
                                UnorderedTriplesAre(std::true_type{},
                                                    internalInserted)),
                       AD_FIELD(TriplesInternal, triplesDeleted_,
                                UnorderedTriplesAre(std::true_type{},
                                                    internalDeleted)))));
  };

  EXPECT_THAT(deltaTriples, StateIs(0, 0, 0, 0, 0, {}, {}, {}, {}));

  // Inserting triples. The triples being inserted must be sorted.
  deltaTriples.insertTriples(
      cancellationHandle,
      makeIdTriples(vocab, localVocab, {"<A> <B> <C>", "<A> <B> <D>"}));
  EXPECT_THAT(
      deltaTriples,
      StateIs(2, 0, 2, 0, 0, {"<A> <B> <C>", "<A> <B> <D>"}, {}, {}, {}));

  // We only locate triples in a Block but don't resolve whether they exist.
  // Inserting triples that exist in the index works normally.
  deltaTriples.insertTriples(
      cancellationHandle, makeIdTriples(vocab, localVocab, {"<A> <low> <a>"}));
  EXPECT_THAT(
      deltaTriples,
      StateIs(3, 0, 3, 0, 0, {"<A> <B> <C>", "<A> <B> <D>", "<A> <low> <a>"},
              {}, {}, {}));

  // Insert more triples.
  deltaTriples.insertTriples(
      cancellationHandle,
      makeIdTriples(vocab, localVocab, {"<B> <C> <D>", "<B> <D> <C>"}));
  EXPECT_THAT(deltaTriples,
              StateIs(5, 0, 5, 0, 0,
                      {"<A> <B> <C>", "<A> <B> <D>", "<B> <C> <D>",
                       "<B> <D> <C>", "<A> <low> <a>"},
                      {}, {}, {}));

  // Inserting already inserted triples has no effect.
  deltaTriples.insertTriples(cancellationHandle,
                             makeIdTriples(vocab, localVocab, {"<A> <B> <C>"}));
  EXPECT_THAT(deltaTriples,
              StateIs(5, 0, 5, 0, 0,
                      {"<A> <B> <C>", "<A> <B> <D>", "<B> <C> <D>",
                       "<A> <low> <a>", "<B> <D> <C>"},
                      {}, {}, {}));

  // Deleting a previously inserted triple removes it from the inserted
  // triples and adds it to the deleted ones.
  deltaTriples.deleteTriples(cancellationHandle,
                             makeIdTriples(vocab, localVocab, {"<A> <B> <D>"}));
  EXPECT_THAT(deltaTriples, StateIs(4, 1, 5, 0, 0,
                                    {"<A> <B> <C>", "<B> <C> <D>",
                                     "<A> <low> <a>", "<B> <D> <C>"},
                                    {"<A> <B> <D>"}, {}, {}));

  // Deleting triples.
  deltaTriples.deleteTriples(
      cancellationHandle,
      makeIdTriples(vocab, localVocab, {"<A> <next> <B>", "<B> <next> <C>"}));
  EXPECT_THAT(
      deltaTriples,
      StateIs(4, 3, 7, 0, 0,
              {"<A> <B> <C>", "<B> <C> <D>", "<A> <low> <a>", "<B> <D> <C>"},
              {"<A> <B> <D>", "<A> <next> <B>", "<B> <next> <C>"}, {}, {}));

  // Deleting non-existent triples.
  deltaTriples.deleteTriples(cancellationHandle,
                             makeIdTriples(vocab, localVocab, {"<A> <B> <F>"}));
  EXPECT_THAT(
      deltaTriples,
      StateIs(
          4, 4, 8, 0, 0,
          {"<A> <B> <C>", "<B> <C> <D>", "<A> <low> <a>", "<B> <D> <C>"},
          {"<A> <B> <D>", "<A> <B> <F>", "<A> <next> <B>", "<B> <next> <C>"},
          {}, {}));

  // Unsorted triples are not allowed, but the assertion that checks this is
  // 1. an `AD_EXPENSIVE_CHECK`.
  // 2. Only enabled in C++20 mode.
#ifndef QLEVER_CPP_17
  if constexpr (ad_utility::areExpensiveChecksEnabled) {
    AD_EXPECT_THROW_WITH_MESSAGE(
        deltaTriples.deleteTriples(
            cancellationHandle,
            makeIdTriples(vocab, localVocab,
                          {"<C> <prev> <B>", "<B> <prev> <A>"})),
        testing::_);
  }
#endif

  // Deleting triples.
  deltaTriples.deleteTriples(
      cancellationHandle,
      makeIdTriples(vocab, localVocab, {"<B> <prev> <A>", "<C> <prev> <B>"}));
  EXPECT_THAT(
      deltaTriples,
      StateIs(4, 6, 10, 0, 0,
              {"<A> <B> <C>", "<B> <C> <D>", "<A> <low> <a>", "<B> <D> <C>"},
              {"<A> <B> <D>", "<A> <B> <F>", "<A> <next> <B>", "<B> <next> <C>",
               "<C> <prev> <B>", "<B> <prev> <A>"},
              {}, {}));

  // Deleting previously deleted triples.
  deltaTriples.deleteTriples(
      cancellationHandle, makeIdTriples(vocab, localVocab, {"<A> <next> <B>"}));
  EXPECT_THAT(
      deltaTriples,
      StateIs(4, 6, 10, 0, 0,
              {"<A> <B> <C>", "<B> <C> <D>", "<A> <low> <a>", "<B> <D> <C>"},
              {"<A> <B> <D>", "<A> <B> <F>", "<A> <next> <B>", "<B> <next> <C>",
               "<C> <prev> <B>", "<B> <prev> <A>"},
              {}, {}));

  // Inserting previously deleted triple.
  deltaTriples.insertTriples(cancellationHandle,
                             makeIdTriples(vocab, localVocab, {"<A> <B> <F>"}));
  EXPECT_THAT(deltaTriples,
              StateIs(5, 5, 10, 0, 0,
                      {"<A> <B> <C>", "<A> <B> <F>", "<B> <C> <D>",
                       "<A> <low> <a>", "<B> <D> <C>"},
                      {"<A> <B> <D>", "<A> <next> <B>", "<B> <next> <C>",
                       "<C> <prev> <B>", "<B> <prev> <A>"},
                      {}, {}));

  // Insert new internal triple.
  deltaTriples.insertInternalTriplesForTesting(
      cancellationHandle,
      makeIdTriples(vocab, localVocab,
                    {"<internal-A> <internal-B> <internal-F>"}));
  EXPECT_THAT(deltaTriples,
              StateIs(5, 5, 10, 1, 0,
                      {"<A> <B> <C>", "<A> <B> <F>", "<B> <C> <D>",
                       "<A> <low> <a>", "<B> <D> <C>"},
                      {"<A> <B> <D>", "<A> <next> <B>", "<B> <next> <C>",
                       "<C> <prev> <B>", "<B> <prev> <A>"},
                      {"<internal-A> <internal-B> <internal-F>"}, {}));

  // Remove "existing" internal triple.
  deltaTriples.deleteInternalTriplesForTesting(
      cancellationHandle,
      makeIdTriples(vocab, localVocab,
                    {"<internal-C> <internal-D> <internal-E>"}));
  EXPECT_THAT(deltaTriples,
              StateIs(5, 5, 10, 1, 1,
                      {"<A> <B> <C>", "<A> <B> <F>", "<B> <C> <D>",
                       "<A> <low> <a>", "<B> <D> <C>"},
                      {"<A> <B> <D>", "<A> <next> <B>", "<B> <next> <C>",
                       "<C> <prev> <B>", "<B> <prev> <A>"},
                      {"<internal-A> <internal-B> <internal-F>"},
                      {"<internal-C> <internal-D> <internal-E>"}));

  // Remove previously inserted internal triple.
  deltaTriples.deleteInternalTriplesForTesting(
      cancellationHandle,
      makeIdTriples(vocab, localVocab,
                    {"<internal-A> <internal-B> <internal-F>"}));
  EXPECT_THAT(deltaTriples,
              StateIs(5, 5, 10, 0, 2,
                      {"<A> <B> <C>", "<A> <B> <F>", "<B> <C> <D>",
                       "<A> <low> <a>", "<B> <D> <C>"},
                      {"<A> <B> <D>", "<A> <next> <B>", "<B> <next> <C>",
                       "<C> <prev> <B>", "<B> <prev> <A>"},
                      {},
                      {"<internal-A> <internal-B> <internal-F>",
                       "<internal-C> <internal-D> <internal-E>"}));

  // Remove previously removes internal triple again.
  deltaTriples.deleteInternalTriplesForTesting(
      cancellationHandle,
      makeIdTriples(vocab, localVocab,
                    {"<internal-C> <internal-D> <internal-E>"}));
  EXPECT_THAT(deltaTriples,
              StateIs(5, 5, 10, 0, 2,
                      {"<A> <B> <C>", "<A> <B> <F>", "<B> <C> <D>",
                       "<A> <low> <a>", "<B> <D> <C>"},
                      {"<A> <B> <D>", "<A> <next> <B>", "<B> <next> <C>",
                       "<C> <prev> <B>", "<B> <prev> <A>"},
                      {},
                      {"<internal-A> <internal-B> <internal-F>",
                       "<internal-C> <internal-D> <internal-E>"}));

  // Inserting previously deleted internal triple.
  deltaTriples.insertInternalTriplesForTesting(
      cancellationHandle,
      makeIdTriples(vocab, localVocab,
                    {"<internal-C> <internal-D> <internal-E>"}));
  EXPECT_THAT(deltaTriples,
              StateIs(5, 5, 10, 1, 1,
                      {"<A> <B> <C>", "<A> <B> <F>", "<B> <C> <D>",
                       "<A> <low> <a>", "<B> <D> <C>"},
                      {"<A> <B> <D>", "<A> <next> <B>", "<B> <next> <C>",
                       "<C> <prev> <B>", "<B> <prev> <A>"},
                      {"<internal-C> <internal-D> <internal-E>"},
                      {"<internal-A> <internal-B> <internal-F>"}));

  deltaTriples.clear();
  // Test internal language filter triples are inserted correctly.
  auto toId = [this, &vocab, &localVocab](TripleComponent& component) {
    return std::move(component).toValueId(
        vocab, localVocab, testQec->getIndex().encodedIriManager());
  };

  Id graphId = qlever::specialIds().at(DEFAULT_GRAPH_IRI);
  auto keysMatch =
      [&toId,
       graphId](std::vector<std::array<TripleComponent, 3>> tripleComponents) {
        std::vector<::testing::internal::KeyMatcher<
            ::testing::internal::EqMatcher<IdTriple<0>>>>
            keys;
        for (auto& [subject, predicate, object] : tripleComponents) {
          keys.push_back(::testing::Key(::testing::Eq(IdTriple<0>{
              {toId(subject), toId(predicate), toId(object), graphId}})));
        }
        return ::testing::UnorderedElementsAreArray(keys);
      };
  auto TriplesAre =
      [&keysMatch](std::vector<std::array<TripleComponent, 3>> inserted,
                   std::vector<std::array<TripleComponent, 3>> deleted,
                   std::vector<std::array<TripleComponent, 3>> internalInserted,
                   std::vector<std::array<TripleComponent, 3>> internalDeleted)
      -> testing::Matcher<const DeltaTriples&> {
    using ::testing::AllOf;
    using TriplesNormal = DeltaTriples::TriplesToHandles<false>;
    using TriplesInternal = DeltaTriples::TriplesToHandles<true>;
    return AllOf(
        AD_FIELD(DeltaTriples, triplesToHandlesNormal_,
                 AllOf(AD_FIELD(TriplesNormal, triplesInserted_,
                                keysMatch(std::move(inserted))),
                       AD_FIELD(TriplesNormal, triplesDeleted_,
                                keysMatch(std::move(deleted))))),
        AD_FIELD(DeltaTriples, triplesToHandlesInternal_,
                 AllOf(AD_FIELD(TriplesInternal, triplesInserted_,
                                keysMatch(std::move(internalInserted))),
                       AD_FIELD(TriplesInternal, triplesDeleted_,
                                keysMatch(std::move(internalDeleted))))));
  };

  deltaTriples.insertTriples(
      cancellationHandle,
      makeIdTriples(
          vocab, localVocab,
          {"<a> <b> 1", "<a> <b> \"abc\"", "<a> <b> \"abc\"@de",
           "<a> <b> \"abc\"@en",
           "<a> <b> \"abc\"^^<http://example.com/datatype>", "<a> <b> <abc>",
           "<a> <other> \"def\"@de", "<a> <other> \"def\"@es"}));
  auto a = iri("<a>");
  auto b = iri("<b>");
  EXPECT_THAT(deltaTriples,
              TriplesAre({{a, b, TripleComponent{1}},
                          {a, b, lit("\"abc\"")},
                          {a, b, lit("\"abc\"@de")},
                          {a, b, lit("\"abc\"@en")},
                          {a, b, lit("\"abc\"^^<http://example.com/datatype>")},
                          {a, b, iri("<abc>")},
                          {a, iri("<other>"), lit("\"def\"@de")},
                          {a, iri("<other>"), lit("\"def\"@es")}},
                         {},
                         {{a, iri("@de@<b>"), lit("\"abc\"@de")},
                          {a, iri("@en@<b>"), lit("\"abc\"@en")},
                          {a, iri("@de@<other>"), lit("\"def\"@de")},
                          {a, iri("@es@<other>"), lit("\"def\"@es")}},
                         {}));

  deltaTriples.deleteTriples(
      cancellationHandle,
      makeIdTriples(
          vocab, localVocab,
          {"<a> <b> 1", "<a> <b> \"abc\"", "<a> <b> \"abc\"@de",
           "<a> <b> \"abc\"@en",
           "<a> <b> \"abc\"^^<http://example.com/datatype>", "<a> <b> <abc>",
           "<a> <other> \"def\"@de", "<a> <other> \"def\"@es"}));
  EXPECT_THAT(deltaTriples,
              TriplesAre({},
                         {{a, b, TripleComponent{1}},
                          {a, b, lit("\"abc\"")},
                          {a, b, lit("\"abc\"@de")},
                          {a, b, lit("\"abc\"@en")},
                          {a, b, lit("\"abc\"^^<http://example.com/datatype>")},
                          {a, b, iri("<abc>")},
                          {a, iri("<other>"), lit("\"def\"@de")},
                          {a, iri("<other>"), lit("\"def\"@es")}},
                         {},
                         {{a, iri("@de@<b>"), lit("\"abc\"@de")},
                          {a, iri("@en@<b>"), lit("\"abc\"@en")},
                          {a, iri("@de@<other>"), lit("\"def\"@de")},
                          {a, iri("@es@<other>"), lit("\"def\"@es")}}));
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
  triples[0].ids()[2] =
      Id::makeFromBlankNodeIndex(BlankNodeIndex::make(999'888'777));
  triples[0].ids()[3] = triples[0].ids()[2];
  auto [s1, p1, o1, g1] = triples[0].ids();

  // Rewrite the IDs in the triple.
  deltaTriples.rewriteLocalVocabEntriesAndBlankNodes(triples);
  auto [s2, p2, o2, g2] = triples[0].ids();

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
  auto [s3, p3, o3, g3] = triples[0].ids();
  EXPECT_EQ(s3.getBits(), s2.getBits());
  EXPECT_EQ(p3.getBits(), p2.getBits());
  EXPECT_EQ(o3.getBits(), o2.getBits());
  EXPECT_EQ(g3.getBits(), g2.getBits());

  // If we use a local blank node that is already part of the global vocabulary,
  // nothing gets rewritten either.
  auto blank0 = Id::makeFromBlankNodeIndex(BlankNodeIndex::make(0));
  triples[0].ids()[0] = blank0;
  deltaTriples.rewriteLocalVocabEntriesAndBlankNodes(triples);
  auto s4 = triples[0].ids()[0];
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

// _____________________________________________________________________________
TEST_F(DeltaTriplesTest, restoreFromNonExistingFile) {
  DeltaTriples deltaTriples{testQec->getIndex()};
  deltaTriples.setPersists("filethatdoesnotexist");
  EXPECT_NO_THROW(deltaTriples.readFromDisk());
  EXPECT_EQ(deltaTriples.numDeleted(), 0);
  EXPECT_EQ(deltaTriples.numInserted(), 0);
  EXPECT_EQ(deltaTriples.numInternalDeleted(), 0);
  EXPECT_EQ(deltaTriples.numInternalInserted(), 0);
}

// _____________________________________________________________________________
TEST_F(DeltaTriplesTest, storeAndRestoreFromEmptySet) {
  DeltaTriples deltaTriples{testQec->getIndex()};
  auto tmpFile =
      std::filesystem::temp_directory_path() / "testEmptyDeltaTriples";
  // Make sure no artifacts from previous crashed runs exists.
  std::filesystem::remove(tmpFile);
  absl::Cleanup cleanup{[&tmpFile]() { std::filesystem::remove(tmpFile); }};
  deltaTriples.setPersists(tmpFile);
  // Write "empty" file
  EXPECT_NO_THROW(deltaTriples.writeToDisk());

  // Check if file contents match
  std::array<char, 55> expectedContent{
      // Magic bytes
      'Q',
      'L',
      'E',
      'V',
      'E',
      'R',
      '.',
      'U',
      'P',
      'D',
      'A',
      'T',
      'E',
      // Version
      1,
      0,
      // Size of `BlankNodeBlocks`.
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      // LocalVocab size
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      // Amount of continuous triple ranges (currently 2, insert + delete)
      2,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      // Amount of ids for deleted triples (currently #triples * 4)
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      // Amount of ids for inserted triples (currently #triples * 4)
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
  };

  std::array<char, expectedContent.size()> actualContent{};

  std::ifstream tmpFileStream{tmpFile, std::ios::binary};
  tmpFileStream.read(actualContent.data(), actualContent.size());
  EXPECT_TRUE(tmpFileStream.good());
  EXPECT_EQ(tmpFileStream.peek(), std::char_traits<char>::eof());
  tmpFileStream.close();

  ASSERT_EQ(expectedContent, actualContent);

  // Check if restoring from empty file works
  EXPECT_NO_THROW(deltaTriples.readFromDisk());
  EXPECT_EQ(deltaTriples.numDeleted(), 0);
  EXPECT_EQ(deltaTriples.numInserted(), 0);
}

// _____________________________________________________________________________
TEST_F(DeltaTriplesTest, storeAndRestoreData) {
  using namespace ::testing;
  using ad_utility::triple_component::LiteralOrIri;
  auto tmpFile = std::filesystem::temp_directory_path() / "testDeltaTriples";
  // Make sure no file like this exists
  std::filesystem::remove(tmpFile);
  absl::Cleanup cleanup{[&tmpFile]() { std::filesystem::remove(tmpFile); }};
  {
    DeltaTriples deltaTriples{testQec->getIndex()};
    deltaTriples.setPersists(tmpFile);
    deltaTriples.readFromDisk();

    auto cancellationHandle =
        std::make_shared<ad_utility::CancellationHandle<>>();
    LocalVocabEntry entry1{LiteralOrIri::fromStringRepresentation("<test>")};
    deltaTriples.insertTriples(
        cancellationHandle,
        {IdTriple<>{{Id::makeFromInt(1), Id::makeFromLocalVocabIndex(&entry1),
                     Id::makeFromBool(true)}}});
    LocalVocabEntry entry2{LiteralOrIri::fromStringRepresentation("<other>")};
    deltaTriples.deleteTriples(
        cancellationHandle,
        {IdTriple<>{{Id::makeFromInt(2), Id::makeFromLocalVocabIndex(&entry2),
                     Id::makeFromBool(false)}}});

    deltaTriples.writeToDisk();
  }
  {
    DeltaTriples deltaTriples{testQec->getIndex()};
    deltaTriples.setPersists(tmpFile);
    deltaTriples.readFromDisk();

    EXPECT_EQ(deltaTriples.numDeleted(), 1);
    EXPECT_EQ(deltaTriples.numInserted(), 1);
    // Currently we don't store internal delta triples in this format. In the
    // future we might regenerate them from the regular delta triples, or change
    // the format so they are also stored on disk.
    EXPECT_EQ(deltaTriples.numInternalDeleted(), 0);
    EXPECT_EQ(deltaTriples.numInternalInserted(), 0);

    EXPECT_THAT(deltaTriples.localVocab().getAllWordsForTesting(),
                ::testing::UnorderedElementsAre(
                    AD_PROPERTY(LocalVocabEntry, toStringRepresentation,
                                ::testing::Eq("<test>")),
                    AD_PROPERTY(LocalVocabEntry, toStringRepresentation,
                                ::testing::Eq("<other>"))));

    std::vector<IdTriple<>> insertedTriples;
    ql::ranges::copy(
        deltaTriples.triplesToHandlesNormal_.triplesInserted_ | ql::views::keys,
        std::back_inserter(insertedTriples));
    EXPECT_THAT(
        insertedTriples,
        ::testing::ElementsAre(::testing::Eq(IdTriple<>{
            {Id::makeFromInt(1),
             Id::makeFromLocalVocabIndex(
                 deltaTriples.localVocab()
                     .getIndexOrNullopt(LocalVocabEntry{
                         LiteralOrIri::fromStringRepresentation("<test>")})
                     .value()),
             Id::makeFromBool(true)}})));
    std::vector<IdTriple<>> deletedTriples;
    ql::ranges::copy(
        deltaTriples.triplesToHandlesNormal_.triplesDeleted_ | ql::views::keys,
        std::back_inserter(deletedTriples));
    EXPECT_THAT(
        deletedTriples,
        ::testing::ElementsAre(::testing::Eq(IdTriple<>{
            {Id::makeFromInt(2),
             Id::makeFromLocalVocabIndex(
                 deltaTriples.localVocab()
                     .getIndexOrNullopt(LocalVocabEntry{
                         LiteralOrIri::fromStringRepresentation("<other>")})
                     .value()),
             Id::makeFromBool(false)}})));
  }
}
