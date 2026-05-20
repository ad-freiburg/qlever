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
#include "./ValueIdTestHelpers.h"
#include "./util/GTestHelpers.h"
#include "./util/IndexTestHelpers.h"
#include "./util/RuntimeParametersTestHelpers.h"
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
      const IndexImpl& index, LocalVocab& localVocab,
      const std::vector<std::string>& turtles) {
    auto toID = [&localVocab, &index](TurtleTriple triple) {
      // The RdfStringParser returns temporary internal IDs for the default
      // graph. Detect this and overwrite with the Iri which gets looked up for
      // the correct ID.
      if (triple.graphIri_ == qlever::specialIds().at(DEFAULT_GRAPH_IRI)) {
        triple.graphIri_ = TripleComponent(
            TripleComponent::Iri::fromIriref(DEFAULT_GRAPH_IRI));
      }
      std::array<Id, 4> ids{
          std::move(triple.subject_).toValueId(index, localVocab),
          std::move(TripleComponent(triple.predicate_))
              .toValueId(index, localVocab),
          std::move(triple.object_).toValueId(index, localVocab),
          std::move(triple.graphIri_).toValueId(index, localVocab)};
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
  auto& index = testQec->getIndex();
  auto& localVocab = deltaTriples.localVocab();

  EXPECT_THAT(deltaTriples, NumTriples(0, 0, 0));

  // Insert then clear.
  deltaTriples.insertTriples(
      cancellationHandle, makeIdTriples(index, localVocab, {"<a> <UPP> <A>"}));
  deltaTriples.insertInternalTriplesForTesting(
      cancellationHandle,
      makeIdTriples(index, localVocab,
                    {"<internal-a> <internal-UPP> <internal-A>"}));
  deltaTriples.consolidateAll();

  EXPECT_THAT(deltaTriples, NumTriples(1, 0, 1, 1, 0));

  deltaTriples.clear();

  EXPECT_THAT(deltaTriples, NumTriples(0, 0, 0, 0, 0));

  // Delete, insert and then clear.
  deltaTriples.deleteTriples(
      cancellationHandle, makeIdTriples(index, localVocab, {"<A> <low> <a>"}));
  deltaTriples.deleteInternalTriplesForTesting(
      cancellationHandle,
      makeIdTriples(index, localVocab,
                    {"<internal-A> <internal-low> <internal-a>"}));
  deltaTriples.consolidateAll();
  EXPECT_THAT(deltaTriples, NumTriples(0, 1, 1, 0, 1));

  deltaTriples.insertTriples(
      cancellationHandle, makeIdTriples(index, localVocab, {"<a> <UPP> <A>"}));
  deltaTriples.insertInternalTriplesForTesting(
      cancellationHandle,
      makeIdTriples(index, localVocab,
                    {"<internal-a> <internal-UPP> <internal-A>"}));
  deltaTriples.consolidateAll();

  EXPECT_THAT(deltaTriples, NumTriples(1, 1, 2, 1, 1));

  deltaTriples.clear();

  EXPECT_THAT(deltaTriples, NumTriples(0, 0, 0, 0, 0));
}

TEST_F(DeltaTriplesTest, insertTriplesAndDeleteTriples) {
  DeltaTriples deltaTriples(testQec->getIndex());
  auto& index = testQec->getIndex();
  auto& localVocab = deltaTriples.localVocab();

  auto cancellationHandle =
      std::make_shared<ad_utility::CancellationHandle<>>();

  auto TriplesAreStr = [this, &index,
                        &localVocab](const std::vector<std::string>& triples) {
    return testing::UnorderedElementsAreArray(
        makeIdTriples(index, localVocab, triples));
  };
  // A matcher that checks the state of a `DeltaTriples`:
  // - `numInserted()` and `numDeleted()` and the derived `getCounts()`
  // - `numTriples()` for all `LocatedTriplesPerBlock`
  // - the inserted and deleted triples (unordered)
  auto StateIs = [this, &TriplesAreStr](
                     size_t numInserted, size_t numDeleted,
                     size_t numTriplesInAllPermutations,
                     size_t numInternalInserted, size_t numInternalDeleted,
                     const std::vector<std::string>& inserted,
                     const std::vector<std::string>& deleted,
                     const std::vector<std::string>& internalInserted,
                     const std::vector<std::string>& internalDeleted)
      -> testing::Matcher<const DeltaTriples&> {
    using ::testing::AllOf;
    using TriplesNormal = DeltaTriples::TriplesSets<false>;
    using TriplesInternal = DeltaTriples::TriplesSets<true>;
    return AllOf(
        NumTriples(numInserted, numDeleted, numTriplesInAllPermutations,
                   numInternalInserted, numInternalDeleted),
        AD_FIELD(DeltaTriples, triplesSetsNormal_,
                 AllOf(AD_FIELD(TriplesNormal, triplesInserted_,
                                TriplesAreStr(inserted)),
                       AD_FIELD(TriplesNormal, triplesDeleted_,
                                TriplesAreStr(deleted)))),
        AD_FIELD(DeltaTriples, triplesSetsInternal_,
                 AllOf(AD_FIELD(TriplesInternal, triplesInserted_,
                                TriplesAreStr(internalInserted)),
                       AD_FIELD(TriplesInternal, triplesDeleted_,
                                TriplesAreStr(internalDeleted)))));
  };

  EXPECT_THAT(deltaTriples, StateIs(0, 0, 0, 0, 0, {}, {}, {}, {}));

  // Inserting triples. The triples being inserted must be sorted.
  deltaTriples.insertTriples(
      cancellationHandle,
      makeIdTriples(index, localVocab, {"<A> <B> <C>", "<A> <B> <D>"}));
  deltaTriples.consolidateAll();
  EXPECT_THAT(
      deltaTriples,
      StateIs(2, 0, 2, 0, 0, {"<A> <B> <C>", "<A> <B> <D>"}, {}, {}, {}));

  // We only locate triples in a Block but don't resolve whether they exist.
  // Inserting triples that exist in the index works normally.
  deltaTriples.insertTriples(
      cancellationHandle, makeIdTriples(index, localVocab, {"<A> <low> <a>"}));
  deltaTriples.consolidateAll();
  EXPECT_THAT(
      deltaTriples,
      StateIs(3, 0, 3, 0, 0, {"<A> <B> <C>", "<A> <B> <D>", "<A> <low> <a>"},
              {}, {}, {}));

  // Insert more triples.
  deltaTriples.insertTriples(
      cancellationHandle,
      makeIdTriples(index, localVocab, {"<B> <C> <D>", "<B> <D> <C>"}));
  deltaTriples.consolidateAll();
  EXPECT_THAT(deltaTriples,
              StateIs(5, 0, 5, 0, 0,
                      {"<A> <B> <C>", "<A> <B> <D>", "<B> <C> <D>",
                       "<B> <D> <C>", "<A> <low> <a>"},
                      {}, {}, {}));

  // Inserting already inserted triples has no effect.
  deltaTriples.insertTriples(cancellationHandle,
                             makeIdTriples(index, localVocab, {"<A> <B> <C>"}));
  deltaTriples.consolidateAll();
  EXPECT_THAT(deltaTriples,
              StateIs(5, 0, 5, 0, 0,
                      {"<A> <B> <C>", "<A> <B> <D>", "<B> <C> <D>",
                       "<A> <low> <a>", "<B> <D> <C>"},
                      {}, {}, {}));

  // Deleting a previously inserted triple removes it from the inserted
  // triples and adds it to the deleted ones.
  deltaTriples.deleteTriples(cancellationHandle,
                             makeIdTriples(index, localVocab, {"<A> <B> <D>"}));
  deltaTriples.consolidateAll();
  EXPECT_THAT(deltaTriples, StateIs(4, 1, 5, 0, 0,
                                    {"<A> <B> <C>", "<B> <C> <D>",
                                     "<A> <low> <a>", "<B> <D> <C>"},
                                    {"<A> <B> <D>"}, {}, {}));

  // Deleting triples.
  deltaTriples.deleteTriples(
      cancellationHandle,
      makeIdTriples(index, localVocab, {"<A> <next> <B>", "<B> <next> <C>"}));
  deltaTriples.consolidateAll();
  EXPECT_THAT(
      deltaTriples,
      StateIs(4, 3, 7, 0, 0,
              {"<A> <B> <C>", "<B> <C> <D>", "<A> <low> <a>", "<B> <D> <C>"},
              {"<A> <B> <D>", "<A> <next> <B>", "<B> <next> <C>"}, {}, {}));

  // Deleting non-existent triples.
  deltaTriples.deleteTriples(cancellationHandle,
                             makeIdTriples(index, localVocab, {"<A> <B> <F>"}));
  deltaTriples.consolidateAll();
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
            makeIdTriples(index, localVocab,
                          {"<C> <prev> <B>", "<B> <prev> <A>"})),
        testing::_);
  }
#endif

  // Deleting triples.
  deltaTriples.deleteTriples(
      cancellationHandle,
      makeIdTriples(index, localVocab, {"<B> <prev> <A>", "<C> <prev> <B>"}));
  deltaTriples.consolidateAll();
  EXPECT_THAT(
      deltaTriples,
      StateIs(4, 6, 10, 0, 0,
              {"<A> <B> <C>", "<B> <C> <D>", "<A> <low> <a>", "<B> <D> <C>"},
              {"<A> <B> <D>", "<A> <B> <F>", "<A> <next> <B>", "<B> <next> <C>",
               "<C> <prev> <B>", "<B> <prev> <A>"},
              {}, {}));

  // Deleting previously deleted triples.
  deltaTriples.deleteTriples(
      cancellationHandle, makeIdTriples(index, localVocab, {"<A> <next> <B>"}));
  deltaTriples.consolidateAll();
  EXPECT_THAT(
      deltaTriples,
      StateIs(4, 6, 10, 0, 0,
              {"<A> <B> <C>", "<B> <C> <D>", "<A> <low> <a>", "<B> <D> <C>"},
              {"<A> <B> <D>", "<A> <B> <F>", "<A> <next> <B>", "<B> <next> <C>",
               "<C> <prev> <B>", "<B> <prev> <A>"},
              {}, {}));

  // Inserting previously deleted triple.
  deltaTriples.insertTriples(cancellationHandle,
                             makeIdTriples(index, localVocab, {"<A> <B> <F>"}));
  deltaTriples.consolidateAll();
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
      makeIdTriples(index, localVocab,
                    {"<internal-A> <internal-B> <internal-F>"}));
  deltaTriples.consolidateAll();
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
      makeIdTriples(index, localVocab,
                    {"<internal-C> <internal-D> <internal-E>"}));
  deltaTriples.consolidateAll();
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
      makeIdTriples(index, localVocab,
                    {"<internal-A> <internal-B> <internal-F>"}));
  deltaTriples.consolidateAll();
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
      makeIdTriples(index, localVocab,
                    {"<internal-C> <internal-D> <internal-E>"}));
  deltaTriples.consolidateAll();
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
      makeIdTriples(index, localVocab,
                    {"<internal-C> <internal-D> <internal-E>"}));
  deltaTriples.consolidateAll();
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
  auto toId = [&index, &localVocab](TripleComponent& component) {
    return std::move(component).toValueId(index, localVocab);
  };

  Id graphId = [&index]() {
    auto graphOpt =
        TripleComponent(TripleComponent::Iri::fromIriref(DEFAULT_GRAPH_IRI))
            .toValueId(index);
    AD_CORRECTNESS_CHECK(graphOpt.has_value());
    return graphOpt.value();
  }();
  auto keysMatch =
      [&toId,
       graphId](std::vector<std::array<TripleComponent, 3>> tripleComponents) {
        std::vector<::testing::internal::EqMatcher<IdTriple<0>>> keys;
        for (auto& [subject, predicate, object] : tripleComponents) {
          keys.push_back(::testing::Eq(IdTriple<0>{
              {toId(subject), toId(predicate), toId(object), graphId}}));
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
    using TriplesNormal = DeltaTriples::TriplesSets<false>;
    using TriplesInternal = DeltaTriples::TriplesSets<true>;
    return AllOf(
        AD_FIELD(DeltaTriples, triplesSetsNormal_,
                 AllOf(AD_FIELD(TriplesNormal, triplesInserted_,
                                keysMatch(std::move(inserted))),
                       AD_FIELD(TriplesNormal, triplesDeleted_,
                                keysMatch(std::move(deleted))))),
        AD_FIELD(DeltaTriples, triplesSetsInternal_,
                 AllOf(AD_FIELD(TriplesInternal, triplesInserted_,
                                keysMatch(std::move(internalInserted))),
                       AD_FIELD(TriplesInternal, triplesDeleted_,
                                keysMatch(std::move(internalDeleted))))));
  };

  deltaTriples.insertTriples(
      cancellationHandle,
      makeIdTriples(index, localVocab,
                    {"<a> <b> 1", "<a> <b> \"abc\"", "<a> <b> \"abc\"@de",
                     "<a> <b> \"abc\"@en",
                     "<a> <b> \"abc\"^^<http://example.com/datatype>",
                     "<a> <b> <abc>", "<a> <other> \"def\"@de",
                     "<a> <other> \"def\"@es", "<other> <a> \"def\"@es"}));
  deltaTriples.consolidateAll();
  auto a = iri("<a>");
  auto b = iri("<b>");
  auto lp = iri(LANGUAGE_PREDICATE);
  auto de = TripleComponent{ad_utility::convertLangtagToEntityUri("de")};
  auto en = TripleComponent{ad_utility::convertLangtagToEntityUri("en")};
  auto es = TripleComponent{ad_utility::convertLangtagToEntityUri("es")};
  EXPECT_THAT(deltaTriples,
              TriplesAre({{a, b, TripleComponent{1}},
                          {a, b, lit("\"abc\"")},
                          {a, b, lit("\"abc\"@de")},
                          {a, b, lit("\"abc\"@en")},
                          {a, b, lit("\"abc\"^^<http://example.com/datatype>")},
                          {a, b, iri("<abc>")},
                          {a, iri("<other>"), lit("\"def\"@de")},
                          {a, iri("<other>"), lit("\"def\"@es")},
                          {iri("<other>"), a, lit("\"def\"@es")}},
                         {},
                         {{lit("\"abc\"@de"), lp, de},
                          {lit("\"abc\"@en"), lp, en},
                          {lit("\"def\"@de"), lp, de},
                          {lit("\"def\"@es"), lp, es},
                          {a, iri("@de@<b>"), lit("\"abc\"@de")},
                          {a, iri("@en@<b>"), lit("\"abc\"@en")},
                          {a, iri("@de@<other>"), lit("\"def\"@de")},
                          {a, iri("@es@<other>"), lit("\"def\"@es")},
                          {iri("<other>"), iri("@es@<a>"), lit("\"def\"@es")}},
                         {}));

  deltaTriples.deleteTriples(
      cancellationHandle,
      makeIdTriples(index, localVocab,
                    {"<a> <b> 1", "<a> <b> \"abc\"", "<a> <b> \"abc\"@de",
                     "<a> <b> \"abc\"@en",
                     "<a> <b> \"abc\"^^<http://example.com/datatype>",
                     "<a> <b> <abc>", "<a> <other> \"def\"@de",
                     "<a> <other> \"def\"@es", "<other> <a> \"def\"@es"}));
  deltaTriples.consolidateAll();
  EXPECT_THAT(
      deltaTriples,
      TriplesAre({},
                 {{a, b, TripleComponent{1}},
                  {a, b, lit("\"abc\"")},
                  {a, b, lit("\"abc\"@de")},
                  {a, b, lit("\"abc\"@en")},
                  {a, b, lit("\"abc\"^^<http://example.com/datatype>")},
                  {a, b, iri("<abc>")},
                  {a, iri("<other>"), lit("\"def\"@de")},
                  {a, iri("<other>"), lit("\"def\"@es")},
                  {iri("<other>"), a, lit("\"def\"@es")}},
                 {{lit("\"abc\"@de"), lp, de},
                  {lit("\"abc\"@en"), lp, en},
                  {lit("\"def\"@de"), lp, de},
                  {lit("\"def\"@es"), lp, es}},
                 {{a, iri("@de@<b>"), lit("\"abc\"@de")},
                  {a, iri("@en@<b>"), lit("\"abc\"@en")},
                  {a, iri("@de@<other>"), lit("\"def\"@de")},
                  {a, iri("@es@<other>"), lit("\"def\"@es")},
                  {iri("<other>"), iri("@es@<a>"), lit("\"def\"@es")}}));
}

// Test the rewriting of local vocab entries and blank nodes.
TEST_F(DeltaTriplesTest, rewriteLocalVocabEntriesAndBlankNodes) {
  // Create a triple with a new local vocab entry and a new blank node. Use the
  // same new blank node twice (as object ID and graph ID, not important) so
  // that we can test that both occurrences are rewritten to the same new blank
  // node.
  DeltaTriples deltaTriples(testQec->getIndex());
  auto& index = testQec->getIndex();
  LocalVocab localVocabOutside;
  auto triples =
      makeIdTriples(index, localVocabOutside, {"<A> <notInVocab> <B>"});
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
  auto& index = testQec->getIndex();
  DeltaTriplesManager deltaTriplesManager(index);
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
    LocatedTriplesSharedState beforeUpdate =
        deltaTriplesManager.getCurrentLocatedTriplesSharedState();
    for (size_t i = 0; i < numIterations; ++i) {
      // The first triple in both vectors is the same for all threads, the
      // others are exclusive to this thread via the `threadIdx`.
      auto triplesToInsert = makeIdTriples(
          index, localVocab,
          {"<A> <B> <C>", absl::StrCat("<A> <B> <D", threadIdx, ">"),
           absl::StrCat("<A> <B> <E", threadIdx, ">")});
      auto triplesToDelete = makeIdTriples(
          index, localVocab,
          {"<A> <A> <E>", absl::StrCat("<A> <B> <E", threadIdx, ">"),
           absl::StrCat("<A> <B> <F", threadIdx, ">")});
      // Insert the `triplesToInsert`.
      deltaTriplesManager.modify<void>([&](DeltaTriples& deltaTriples) {
        deltaTriples.insertTriples(cancellationHandle, triplesToInsert);
        deltaTriples.consolidateAll();
      });
      // We should have successfully completed an update, so the snapshot
      // pointer should have changed.
      EXPECT_NE(beforeUpdate,
                deltaTriplesManager.getCurrentLocatedTriplesSharedState());
      // Delete the `triplesToDelete`.
      deltaTriplesManager.modify<void>([&](DeltaTriples& deltaTriples) {
        deltaTriples.deleteTriples(cancellationHandle, triplesToDelete);
        deltaTriples.consolidateAll();
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
              beforeUpdate->getLocatedTriplesForPermutation<false>(
                  Permutation::SPO);
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
          auto p = deltaTriplesManager.getCurrentLocatedTriplesSharedState();
          const auto& locatedSPO =
              p->getLocatedTriplesForPermutation<false>(Permutation::SPO);
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
  auto p1 = deltaTriplesManager.getCurrentLocatedTriplesSharedState();
  auto p2 = deltaTriplesManager.getCurrentLocatedTriplesSharedState();
  EXPECT_EQ(p1, p2);

  // Each of the threads above inserts one thread-exclusive triple, deletes one
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
TEST_F(DeltaTriplesTest, LocatedTriplesSharedState) {
  auto Snapshot = [](size_t index, size_t numTriples)
      -> testing::Matcher<const LocatedTriplesSharedState> {
    auto m =
        AD_PROPERTY(LocatedTriplesPerBlock, numTriplesForTesting, numTriples);
    return testing::Pointee(testing::AllOf(
        AD_FIELD(LocatedTriplesState, index_, testing::Eq(index)),
        AD_FIELD(LocatedTriplesState, locatedTriplesPerBlock_,
                 testing::ElementsAre(m, m, m, m, m, m))));
  };
  DeltaTriples deltaTriples(testQec->getIndex());
  auto& index = testQec->getIndex();
  auto cancellationHandle =
      std::make_shared<ad_utility::CancellationHandle<>>();

  // Do one transparent and two copied snapshots.
  LocatedTriplesSharedState transparentSnapshotBeforeUpdate =
      deltaTriples.getLocatedTriplesSharedStateReference();
  LocatedTriplesSharedState copiedSnapshotBeforeUpdate =
      deltaTriples.getLocatedTriplesSharedStateCopy();
  LocatedTriplesSharedState copiedSnapshotBeforeUpdate2 =
      deltaTriples.getLocatedTriplesSharedStateCopy();

  // All snapshots have the same index and triples.
  EXPECT_THAT(transparentSnapshotBeforeUpdate, Snapshot(0, 0));
  EXPECT_THAT(copiedSnapshotBeforeUpdate, Snapshot(0, 0));
  EXPECT_THAT(copiedSnapshotBeforeUpdate2, Snapshot(0, 0));

  // Modifying the delta triples increases the index_.
  LocalVocab localVocab;
  auto triplesToInsert = makeIdTriples(index, localVocab, {"<A> <B> <C>"});
  deltaTriples.insertTriples(cancellationHandle, std::move(triplesToInsert));
  deltaTriples.consolidateAll();

  // Another transparent and copied snapshot.
  LocatedTriplesSharedState transparentSnapshotAfterUpdate =
      deltaTriples.getLocatedTriplesSharedStateReference();
  LocatedTriplesSharedState copiedSnapshotAfterUpdate =
      deltaTriples.getLocatedTriplesSharedStateCopy();

  // The two new snapshots are identical and up-to-date.
  EXPECT_THAT(transparentSnapshotAfterUpdate, Snapshot(1, 1));
  EXPECT_THAT(copiedSnapshotAfterUpdate, Snapshot(1, 1));
  // The transparent snapshot mirrors the underlying state of the
  // `DeltaTriples`, so the transparent snapshots from before the update now has
  // the state after the update.
  EXPECT_THAT(transparentSnapshotBeforeUpdate, Snapshot(1, 1));
  // The copied snapshot before the update is unchanged.
  EXPECT_THAT(copiedSnapshotBeforeUpdate, Snapshot(0, 0));
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
  auto defaultGraph =
      TripleComponent(TripleComponent::Iri::fromIriref(DEFAULT_GRAPH_IRI))
          .toValueId(testQec->getIndex().getImpl())
          .value();
  const auto& localVocabContext = testQec->getLocalVocabContext();
  {
    DeltaTriples deltaTriples{testQec->getIndex()};
    deltaTriples.setPersists(tmpFile);
    deltaTriples.readFromDisk();

    auto cancellationHandle =
        std::make_shared<ad_utility::CancellationHandle<>>();
    LocalVocabEntry entry1 =
        LocalVocabEntry::fromStringRepresentation("<test>", localVocabContext);
    deltaTriples.insertTriples(
        cancellationHandle,
        {IdTriple<>{{Id::makeFromInt(1), Id::makeFromLocalVocabIndex(&entry1),
                     Id::makeFromBool(true), defaultGraph}}});
    LocalVocabEntry entry2 =
        LocalVocabEntry::fromStringRepresentation("<other>", localVocabContext);
    deltaTriples.deleteTriples(
        cancellationHandle,
        {IdTriple<>{{Id::makeFromInt(2), Id::makeFromLocalVocabIndex(&entry2),
                     Id::makeFromBool(false), defaultGraph}}});

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
                                ::testing::Eq("<other>")),
                    AD_PROPERTY(LocalVocabEntry, toStringRepresentation,
                                ::testing::Eq(LANGUAGE_PREDICATE))));

    std::vector<IdTriple<>> insertedTriples;
    ql::ranges::copy(deltaTriples.triplesSetsNormal_.triplesInserted_,
                     std::back_inserter(insertedTriples));
    EXPECT_THAT(insertedTriples,
                ::testing::ElementsAre(::testing::Eq(IdTriple<>{
                    {Id::makeFromInt(1),
                     Id::makeFromLocalVocabIndex(
                         deltaTriples.localVocab()
                             .getIndexOrNullopt(
                                 LocalVocabEntry::fromStringRepresentation(
                                     "<test>", localVocabContext))
                             .value()),
                     Id::makeFromBool(true), defaultGraph}})));
    std::vector<IdTriple<>> deletedTriples;
    ql::ranges::copy(deltaTriples.triplesSetsNormal_.triplesDeleted_,
                     std::back_inserter(deletedTriples));
    EXPECT_THAT(deletedTriples,
                ::testing::ElementsAre(::testing::Eq(IdTriple<>{
                    {Id::makeFromInt(2),
                     Id::makeFromLocalVocabIndex(
                         deltaTriples.localVocab()
                             .getIndexOrNullopt(
                                 LocalVocabEntry::fromStringRepresentation(
                                     "<other>", localVocabContext))
                             .value()),
                     Id::makeFromBool(false), defaultGraph}})));
  }
}

// _____________________________________________________________________________
TEST_F(DeltaTriplesTest, copyLocalVocab) {
  using namespace ::testing;
  using ad_utility::triple_component::LiteralOrIri;
  const auto& localVocabContext = testQec->getLocalVocabContext();
  DeltaTriples deltaTriples{testQec->getIndex()};

  std::string iri1 = "<test>";
  std::string iri2 = "<other>";

  auto cancellationHandle =
      std::make_shared<ad_utility::CancellationHandle<>>();
  LocalVocabEntry entry1 =
      LocalVocabEntry::fromStringRepresentation(iri1, localVocabContext);
  deltaTriples.insertTriples(
      cancellationHandle,
      {IdTriple<>{{Id::makeFromInt(1), Id::makeFromLocalVocabIndex(&entry1),
                   Id::makeFromBlankNodeIndex(BlankNodeIndex::make(1337))}}});
  LocalVocabEntry entry2 =
      LocalVocabEntry::fromStringRepresentation(iri2, localVocabContext);
  deltaTriples.deleteTriples(
      cancellationHandle,
      {IdTriple<>{{Id::makeFromInt(2), Id::makeFromLocalVocabIndex(&entry2),
                   Id::makeFromBool(false)}}});

  auto [indices, ownedBlocks] = deltaTriples.copyLocalVocab();

  using namespace ::testing;

  EXPECT_THAT(indices,
              UnorderedElementsAre(
                  Pointee(AD_PROPERTY(LocalVocabEntry, toStringRepresentation,
                                      Eq(iri1))),
                  Pointee(AD_PROPERTY(LocalVocabEntry, toStringRepresentation,
                                      Eq(iri2))),
                  Pointee(AD_PROPERTY(LocalVocabEntry, toStringRepresentation,
                                      Eq(LANGUAGE_PREDICATE)))));

  using OBE =
      ad_utility::BlankNodeManager::LocalBlankNodeManager::OwnedBlocksEntry;
  // Blank Nodes are assigned at random, so all we can check is that there is
  // exactly one block allocated.
  EXPECT_THAT(ownedBlocks, ElementsAre(AD_FIELD(OBE, blockIndices_,
                                                ElementsAre(A<uint64_t>()))));
}

// _____________________________________________________________________________
TEST_F(DeltaTriplesTest, getCurrentLocatedTriplesSharedStateWithVocab) {
  using namespace ::testing;
  using ad_utility::triple_component::LiteralOrIri;
  const auto& index = testQec->getIndex();
  DeltaTriplesManager deltaTriplesManager(index);

  std::string iri1 = "<test>";
  LocalVocabEntry entry1 =
      LocalVocabEntry::fromStringRepresentation(iri1, index);
  IdTriple<> triple1{{Id::makeFromInt(1), Id::makeFromLocalVocabIndex(&entry1),
                      Id::makeFromBool(true)}};
  std::string iri2 = "<other>";
  LocalVocabEntry entry2 =
      LocalVocabEntry::fromStringRepresentation(iri2, index);
  IdTriple<> triple2{{Id::makeFromInt(2), Id::makeFromLocalVocabIndex(&entry2),
                      Id::makeFromBool(false)}};
  deltaTriplesManager.modify<void>(
      [&triple1, &triple2](DeltaTriples& deltaTriples) {
        auto cancellationHandle =
            std::make_shared<ad_utility::CancellationHandle<>>();
        deltaTriples.insertTriples(cancellationHandle, {triple1});
        deltaTriples.deleteTriples(cancellationHandle, {triple2});
        deltaTriples.consolidateAll();
      });

  auto [sharedState, indices, ownedBlocks] =
      deltaTriplesManager.getCurrentLocatedTriplesSharedStateWithVocab();

  using namespace ::testing;

  const auto& locatedSPO =
      sharedState->getLocatedTriplesForPermutation<false>(Permutation::SPO);
  EXPECT_TRUE(locatedSPO.isLocatedTriple(triple1, true));
  EXPECT_TRUE(locatedSPO.isLocatedTriple(triple2, false));

  EXPECT_THAT(indices,
              UnorderedElementsAre(
                  Pointee(AD_PROPERTY(LocalVocabEntry, toStringRepresentation,
                                      Eq(iri1))),
                  Pointee(AD_PROPERTY(LocalVocabEntry, toStringRepresentation,
                                      Eq(iri2))),
                  Pointee(AD_PROPERTY(LocalVocabEntry, toStringRepresentation,
                                      Eq(LANGUAGE_PREDICATE)))));

  EXPECT_THAT(ownedBlocks, ElementsAre());
}

// _____________________________________________________________________________
TEST_F(DeltaTriplesTest, vacuum) {
  auto cancellationHandle =
      std::make_shared<ad_utility::CancellationHandle<>>();
  DeltaTriples deltaTriples(testQec->getIndex());
  auto& index = testQec->getIndex().getImpl();
  LocalVocab localVocab;

  // Insertions of triples in the index.
  deltaTriples.insertTriples(
      cancellationHandle,
      makeIdTriples(index, localVocab, {"<a> <upp> <A>", "<b> <upp> <B>"}));
  // Deletions of triples not in the index.
  deltaTriples.deleteTriples(cancellationHandle,
                             makeIdTriples(index, localVocab, {"<X> <Y> <Z>"}));
  // Insertions of triples not in the index.
  deltaTriples.insertTriples(
      cancellationHandle,
      makeIdTriples(index, localVocab, {"<a> <upp> <newval>"}));
  // Deletions of triples in the index.
  deltaTriples.deleteTriples(
      cancellationHandle, makeIdTriples(index, localVocab, {"<a> <next> <b>"}));
  deltaTriples.consolidateAll();

  EXPECT_THAT(deltaTriples, NumTriples(3, 2, 5));

  auto cleanup =
      setRuntimeParameterForTest<&RuntimeParameters::vacuumMinimumBlockSize_>(
          0ul);

  auto result = deltaTriples.vacuum(cancellationHandle);

  EXPECT_THAT(deltaTriples, NumTriples(1, 1, 2));
  EXPECT_EQ(result["external"]["insertionsRemoved"], 2);
  EXPECT_EQ(result["external"]["deletionsRemoved"], 1);
  EXPECT_EQ(result["external"]["insertionsKept"], 1);
  EXPECT_EQ(result["external"]["deletionsKept"], 1);
  // Had no updates to begin with.
  EXPECT_EQ(result["internal"]["totalRemoved"], 0);
  EXPECT_EQ(result["internal"]["totalKept"], 0);
}

// _____________________________________________________________________________
TEST_F(DeltaTriplesTest, remapId) {
  auto I = &Id::makeFromInt;
  auto V = &makeVocabId;
  auto B = &makeBlankNodeId;
  qlever::indexRebuilder::IndexRebuildMapping idMapping;
  Id entryId = makeLocalVocabId(10101010);
  auto remap = [&idMapping](Id id) {
    DeltaTriples::remapId(idMapping, id);
    return id;
  };

  EXPECT_EQ(remap(I(69)), I(69));
  EXPECT_EQ(remap(entryId), entryId);
  idMapping.localVocabMapping_.emplace(entryId.getBits(), I(42));
  EXPECT_EQ(remap(entryId), I(42));

  idMapping.minBlankNodeIndex_ = 0;
  EXPECT_EQ(remap(B(1337)), B(1337));
  idMapping.blankNodeBlocks_.push_back(1);
  idMapping.minBlankNodeIndex_ = 330;
  EXPECT_EQ(remap(B(1337)), B(337));
  EXPECT_EQ(remap(B(37)), B(37));

  EXPECT_EQ(remap(V(10)), V(10));
  idMapping.insertionPositions_.push_back(VocabIndex::make(5));
  EXPECT_EQ(remap(V(10)), V(11));
  EXPECT_EQ(remap(V(5)), V(6));
  EXPECT_EQ(remap(V(4)), V(4));
}

#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17

namespace {
qlever::indexRebuilder::IndexRebuildMapping simulateRebuild(
    const std::vector<LocalVocabIndex>& originalVocab,
    const std::vector<
        ad_utility::BlankNodeManager::LocalBlankNodeManager::OwnedBlocksEntry>&
        blankNodeBlocks,
    uint64_t minBlankNodeIndex) {
  qlever::indexRebuilder::IndexRebuildMapping idMapping;
  Id firstNewEntry =
      Id::fromBits(originalVocab.at(0)->positionInVocab().upperBound_.get());
  Id secondNewEntry =
      Id::fromBits(originalVocab.at(1)->positionInVocab().upperBound_.get());
  Id thirdNewEntry =
      Id::fromBits(originalVocab.at(2)->positionInVocab().upperBound_.get());

  idMapping.insertionPositions_.push_back(firstNewEntry.getVocabIndex());
  idMapping.insertionPositions_.push_back(secondNewEntry.getVocabIndex());
  idMapping.insertionPositions_.push_back(thirdNewEntry.getVocabIndex());
  ql::ranges::sort(idMapping.insertionPositions_);
  idMapping.localVocabMapping_.emplace(
      Id::makeFromLocalVocabIndex(originalVocab.at(0)).getBits(),
      firstNewEntry);
  idMapping.localVocabMapping_.emplace(
      Id::makeFromLocalVocabIndex(originalVocab.at(1)).getBits(),
      secondNewEntry);
  idMapping.localVocabMapping_.emplace(
      Id::makeFromLocalVocabIndex(originalVocab.at(2)).getBits(),
      thirdNewEntry);
  idMapping.blankNodeBlocks_.emplace_back(
      blankNodeBlocks.at(0).blockIndices_.at(0));
  idMapping.minBlankNodeIndex_ = minBlankNodeIndex;
  return idMapping;
}
}  // namespace

// _____________________________________________________________________________
TEST_F(DeltaTriplesTest, addFromSnapshotDiff) {
  auto cancellationHandle =
      std::make_shared<ad_utility::CancellationHandle<>>();
  DeltaTriples deltaTriples(testQec->getIndex());
  auto& index = testQec->getIndex();
  auto getId = ad_utility::testing::makeGetId(index);
  LocalVocab localVocab;

  Id x = getId("<x>");
  Id anon = getId("<anon>");
  Id graph = getId(std::string{DEFAULT_GRAPH_IRI});

  deltaTriples.insertTriples(
      cancellationHandle,
      makeIdTriples(index, localVocab,
                    {"<a> <upp> <newval>", "<anon> <x> 42", "<C> <next> <D>"}));
  deltaTriples.insertTriples(
      cancellationHandle,
      {IdTriple<0>{std::array{x, anon, makeBlankNodeId(1337), graph}}});
  deltaTriples.deleteTriples(
      cancellationHandle, makeIdTriples(index, localVocab, {"<a> <next> <b>"}));
  deltaTriples.consolidateAll();

  auto originalSnapshot = deltaTriples.getLocatedTriplesSharedStateCopy();
  auto [originalVocab, blankNodeBlocks] = deltaTriples.copyLocalVocab();
  ASSERT_EQ(originalVocab.size(), 3);
  ASSERT_EQ(blankNodeBlocks.size(), 1);
  ASSERT_EQ(blankNodeBlocks.at(0).blockIndices_.size(), 1);

  // Simulate delta triples being inserted after the rebuild.
  deltaTriples.insertTriples(
      cancellationHandle,
      makeIdTriples(index, localVocab, {"<anon> <x> \"neverseenbefore\"@en"}));
  deltaTriples.insertTriples(
      cancellationHandle,
      {IdTriple<0>{std::array{anon, x, makeBlankNodeId(1338), graph}}});
  deltaTriples.deleteTriples(
      cancellationHandle, makeIdTriples(index, localVocab, {"<C> <next> <D>"}));
  deltaTriples.consolidateAll();
  auto newSnapshot = deltaTriples.getLocatedTriplesSharedStateCopy();

  // Technically you'd use a rebuilt index for this, but for testing the
  // existing one suffices.
  DeltaTriples newDeltaTriples(testQec->getIndex());
  ad_utility::timer::TimeTracer tracer{"testAddFromSnapshotDiff"};
  qlever::indexRebuilder::IndexRebuildMapping idMapping = simulateRebuild(
      originalVocab, blankNodeBlocks, index.getBlankNodeManager()->minIndex_);

  newDeltaTriples.addFromSnapshotDiff(*originalSnapshot, *newSnapshot,
                                      idMapping, std::move(cancellationHandle),
                                      tracer);
  newDeltaTriples.consolidateAll();

  EXPECT_THAT(newDeltaTriples, NumTriples(2, 1, 3, 2, 0));
  auto locatedTriples =
      newDeltaTriples.getLocatedTriplesForPermutation(Permutation::SPO);
  std::vector<IdTriple<0>> insertedTriples;
  auto numLocatedTriples = locatedTriples.numTriplesForTesting();
  insertedTriples.reserve(numLocatedTriples);

  for (size_t counter = 0; insertedTriples.size() < numLocatedTriples;
       counter++) {
    auto updates = locatedTriples.getUpdatesIfPresent(counter);
    if (updates.has_value()) {
      for (const auto& locatedTriple : updates.value()) {
        insertedTriples.push_back(locatedTriple.triple_);
      }
    }

    ASSERT_LT(counter, 1000)
        << "This is to prevent an infinite loop in case of a bug.";
  }

  // Account for offset introduced by index rebuild.
  auto add = [](Id id, size_t offset) {
    AD_CONTRACT_CHECK(id.getDatatype() == Datatype::VocabIndex);
    return Id::makeFromVocabIndex(
        VocabIndex::make(id.getVocabIndex().get() + offset));
  };

  Id newX = add(x, 3);
  Id newGraph = add(graph, 1);
  Id newNext = add(getId("<next>"), 3);

  ASSERT_EQ(insertedTriples.size(), 3);
  EXPECT_THAT(insertedTriples.at(0).ids(),
              ::testing::ElementsAre(
                  anon, newX,
                  AD_PROPERTY(ValueId, getDatatype,
                              ::testing::Eq(Datatype::LocalVocabIndex)),
                  newGraph));
  EXPECT_THAT(insertedTriples.at(1).ids(),
              ::testing::ElementsAre(
                  anon, newX,
                  ::testing::ResultOf(
                      [](Id id) {
                        return id.getDatatype() == Datatype::BlankNodeIndex
                                   ? id.getBlankNodeIndex().get()
                                   : 0;
                      },
                      ::testing::Gt(2)),
                  newGraph));
  EXPECT_THAT(
      insertedTriples.at(2).ids(),
      ::testing::ElementsAre(getId("<C>"), newNext,
                             AD_PROPERTY(ValueId, getDatatype,
                                         ::testing::Eq(Datatype::VocabIndex)),
                             newGraph));
}
#endif
