// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "./util/GTestHelpers.h"
#include "./util/IndexTestHelpers.h"
#include "global/Constants.h"
#include "index/AuxIndexBuilder.h"
#include "index/DeltaTriples.h"
#include "index/ExportIds.h"
#include "index/IndexImpl.h"
#include "parser/RdfParser.h"
#include "parser/Tokenizer.h"
#include "util/MemorySize/MemorySize.h"

namespace {
using namespace ad_utility::memory_literals;

// Parse the given `turtle` into sorted, deduplicated `IdTriple`s (which is what
// `DeltaTriples` requires of its callers).
std::vector<IdTriple<0>> makeSortedIdTriples(const IndexImpl& index,
                                             LocalVocab& localVocab,
                                             const std::string& turtle) {
  RdfStringParser<TurtleParser<Tokenizer>> parser{&index.encodedIriManager()};
  parser.parseUtf8String(turtle);
  auto triples = ad_utility::transform(
      parser.getTriples(), [&index, &localVocab](TurtleTriple triple) {
        if (triple.graphIri_ == qlever::specialIds().at(DEFAULT_GRAPH_IRI)) {
          triple.graphIri_ = TripleComponent{
              TripleComponent::Iri::fromIriref(DEFAULT_GRAPH_IRI)};
        }
        std::array<Id, 4> ids{
            std::move(triple.subject_).toValueId(index, localVocab),
            TripleComponent{triple.predicate_}.toValueId(index, localVocab),
            std::move(triple.object_).toValueId(index, localVocab),
            std::move(triple.graphIri_).toValueId(index, localVocab)};
        return IdTriple<0>{ids};
      });
  ql::ranges::sort(triples);
  triples.erase(std::unique(triples.begin(), triples.end()), triples.end());
  return triples;
}

// Fixture that builds an index, applies updates, moves them into an auxiliary
// index, and then scans the index.
class AuxIndexScanTest : public ::testing::Test {
 protected:
  static constexpr const char* testTurtle =
      "<a> <p> <A> . <b> <p> <B> . <c> <p> <C> . <d> <p> <D> .";

  std::unique_ptr<Index> index_ = std::make_unique<Index>(makeIndex());
  std::vector<std::string> auxBasenames_;

  static Index makeIndex() {
    std::string basename = gtestCurrentTestName();
    for (size_t generation : AuxIndex::generationsOnDisk(basename)) {
      AuxIndex::deleteFromDisk(AuxIndex::makeBasename(basename, generation));
    }
    ad_utility::testing::TestIndexConfig config{std::string{testTurtle}};
    // A tiny block size, so that the permutations have several blocks and the
    // triples of the auxiliary index are distributed over them.
    config.blocksizePermutations = 16_B;
    return ad_utility::testing::makeTestIndex(basename, std::move(config));
  }

  ~AuxIndexScanTest() override {
    for (const auto& basename : auxBasenames_) {
      AuxIndex::deleteFromDisk(basename);
    }
  }

  IndexImpl& impl() { return index_->getImpl(); }
  const IndexImpl& impl() const { return index_->getImpl(); }

  // Apply the given insertions and deletions to the delta triples.
  void update(const std::string& toInsert, const std::string& toDelete = "") {
    auto handle = std::make_shared<ad_utility::CancellationHandle<>>();
    index_->deltaTriplesManager().modify<void>(
        [this, &toInsert, &toDelete, &handle](DeltaTriples& deltaTriples) {
          LocalVocab localVocab;
          if (!toDelete.empty()) {
            deltaTriples.deleteTriples<DeltaTriples::Consolidate::No>(
                handle, makeSortedIdTriples(impl(), localVocab, toDelete));
          }
          if (!toInsert.empty()) {
            deltaTriples.insertTriples<DeltaTriples::Consolidate::No>(
                handle, makeSortedIdTriples(impl(), localVocab, toInsert));
          }
          deltaTriples.consolidateAll();
        },
        false, true);
  }

  // Move the current delta triples into a new generation of the auxiliary
  // index, publish it, and clear the delta triples, so that from now on the
  // triples are only in the auxiliary index.
  void moveDeltaTriplesToAuxIndex() {
    auto handle = std::make_shared<ad_utility::CancellationHandle<>>();
    auto [state, entries, blocks] =
        index_->deltaTriplesManager()
            .getCurrentLocatedTriplesSharedStateWithVocab();
    auto result = qlever::buildAuxIndex(impl(), *state, 10_MB, handle);
    auxBasenames_.push_back(result.basename_);

    auto auxIndex = std::make_shared<AuxIndex>(impl().allocator());
    const auto& locale = impl().configurationJson().at("locale");
    auxIndex->loadFromDisk(result.basename_, std::string{locale.at("language")},
                           std::string{locale.at("country")},
                           bool{locale.at("ignore-punctuation")});
    index_->deltaTriplesManager().clear();
    impl().setAuxIndexForTesting(std::move(auxIndex));
  }

  // Scan the given permutation for all triples and return them in a
  // human-readable form (`subject predicate object`, in the order of the
  // permutation).
  std::vector<std::string> scanAll(Permutation::Enum permutationEnum) {
    const auto& permutation = impl().getPermutation(permutationEnum);
    auto state =
        index_->deltaTriplesManager().getCurrentLocatedTriplesSharedState();
    auto scanSpecAndBlocks = permutation.getScanSpecAndBlocks(
        ScanSpecification{std::nullopt, std::nullopt, std::nullopt}, *state);
    auto handle = std::make_shared<ad_utility::CancellationHandle<>>();
    IdTable table = permutation.scan(scanSpecAndBlocks, {}, handle, *state);
    LocalVocab emptyLocalVocab;
    auto toString = [this, &emptyLocalVocab](Id id) -> std::string {
      auto word = ql::exportIds::idToLiteralOrIri(impl(), id, emptyLocalVocab);
      return word.has_value() ? word.value().toStringRepresentation()
                              : absl::StrCat("id:", id.getBits());
    };
    std::vector<std::string> result;
    for (size_t row = 0; row < table.numRows(); ++row) {
      result.push_back(absl::StrCat(toString(table(row, 0)), " ",
                                    toString(table(row, 1)), " ",
                                    toString(table(row, 2))));
    }
    return result;
  }
};

// ____________________________________________________________________________
TEST_F(AuxIndexScanTest, insertionsAndDeletionsFromTheAuxIndexAreMergedIn) {
  // The index contains `<a> <p> <A>` ... `<d> <p> <D>`. Delete two of them and
  // insert two triples, one of which sorts in the middle of the existing ones
  // and one of which is larger than all of them (so that it ends up in the
  // block after the last one).
  update("<b> <p> <Z> . <z> <p> <Z> .", "<a> <p> <A> . <c> <p> <C> .");
  auto expected = scanAll(Permutation::SPO);
  EXPECT_THAT(expected, ::testing::ElementsAre("<b> <p> <B>", "<b> <p> <Z>",
                                               "<d> <p> <D>", "<z> <p> <Z>"));

  // After moving the updates into the auxiliary index, a scan has to return
  // exactly the same result, now with the triples coming from disk.
  moveDeltaTriplesToAuxIndex();
  EXPECT_EQ(scanAll(Permutation::SPO), expected);
  // The delta triples are empty now, so the result really comes from the
  // auxiliary index.
  EXPECT_EQ(index_->deltaTriplesManager()
                .getCurrentLocatedTriplesSharedState()
                ->counts_.value()
                .triplesInserted_,
            0);

  // The same in the other permutations.
  EXPECT_THAT(scanAll(Permutation::OPS),
              ::testing::ElementsAre("<B> <p> <b>", "<D> <p> <d>",
                                     "<Z> <p> <b>", "<Z> <p> <z>"));
  EXPECT_THAT(scanAll(Permutation::PSO),
              ::testing::ElementsAre("<p> <b> <B>", "<p> <b> <Z>",
                                     "<p> <d> <D>", "<p> <z> <Z>"));
}

// ____________________________________________________________________________
TEST_F(AuxIndexScanTest, deltaTriplesTakePrecedenceOverTheAuxIndex) {
  update("<b> <p> <Z> . <z> <p> <Z> .", "<a> <p> <A> .");
  moveDeltaTriplesToAuxIndex();
  ASSERT_THAT(
      scanAll(Permutation::SPO),
      ::testing::ElementsAre("<b> <p> <B>", "<b> <p> <Z>", "<c> <p> <C>",
                             "<d> <p> <D>", "<z> <p> <Z>"));

  // Now delete a triple that the auxiliary index inserted, re-insert a triple
  // that it deleted, and delete a triple of the main index. All three have to
  // take precedence over the auxiliary index.
  update("<a> <p> <A> .", "<b> <p> <Z> . <d> <p> <D> .");
  EXPECT_THAT(scanAll(Permutation::SPO),
              ::testing::ElementsAre("<a> <p> <A>", "<b> <p> <B>",
                                     "<c> <p> <C>", "<z> <p> <Z>"));
}

// ____________________________________________________________________________
TEST_F(AuxIndexScanTest, scanWithFixedSubjectAndPredicate) {
  update("<b> <p> <Z> . <z> <p> <Z> .", "<a> <p> <A> .");
  moveDeltaTriplesToAuxIndex();

  auto getId = ad_utility::testing::makeGetId(*index_);
  auto state =
      index_->deltaTriplesManager().getCurrentLocatedTriplesSharedState();
  const auto& permutation = impl().getPermutation(Permutation::SPO);
  auto handle = std::make_shared<ad_utility::CancellationHandle<>>();

  // A scan for `<b> <p> ?o` has to return the object of the main index and the
  // one that the auxiliary index inserted.
  auto scanSpec = ScanSpecification{getId("<b>"), getId("<p>"), std::nullopt};
  auto scanSpecAndBlocks = permutation.getScanSpecAndBlocks(scanSpec, *state);
  IdTable table = permutation.scan(scanSpecAndBlocks, {}, handle, *state);
  ASSERT_EQ(table.numColumns(), 1);
  EXPECT_THAT(table.getColumn(0),
              ::testing::ElementsAre(getId("<B>"), getId("<Z>")));

  // A scan for `<a> <p> ?o` has to be empty, because the auxiliary index
  // deleted the only such triple.
  scanSpec = ScanSpecification{getId("<a>"), getId("<p>"), std::nullopt};
  scanSpecAndBlocks = permutation.getScanSpecAndBlocks(scanSpec, *state);
  table = permutation.scan(scanSpecAndBlocks, {}, handle, *state);
  EXPECT_EQ(table.numRows(), 0);

  // A scan for the subject that only exists in the auxiliary index.
  scanSpec = ScanSpecification{getId("<z>"), getId("<p>"), std::nullopt};
  scanSpecAndBlocks = permutation.getScanSpecAndBlocks(scanSpec, *state);
  table = permutation.scan(scanSpecAndBlocks, {}, handle, *state);
  EXPECT_THAT(table.getColumn(0), ::testing::ElementsAre(getId("<Z>")));
}

// ____________________________________________________________________________
TEST_F(AuxIndexScanTest, scanSizesAccountForTheAuxIndex) {
  update("<b> <p> <Z> . <z> <p> <Z> .", "<a> <p> <A> .");
  moveDeltaTriplesToAuxIndex();
  auto state =
      index_->deltaTriplesManager().getCurrentLocatedTriplesSharedState();
  const auto& permutation = impl().getPermutation(Permutation::SPO);
  auto scanSpecAndBlocks = permutation.getScanSpecAndBlocks(
      ScanSpecification{std::nullopt, std::nullopt, std::nullopt}, *state);
  // Four triples in the main index, one deleted and two inserted.
  EXPECT_EQ(permutation.getResultSizeOfScan(scanSpecAndBlocks, *state), 5);
  auto [lower, upper] =
      permutation.getSizeEstimateForScan(scanSpecAndBlocks, *state);
  EXPECT_LE(lower, 5);
  EXPECT_GE(upper, 5);
}

}  // namespace
