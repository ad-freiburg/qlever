// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <absl/cleanup/cleanup.h>
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

// A triple of an auxiliary index, as a human-readable representation of its
// four components plus the information whether it is inserted or deleted. This
// is what the tests below compare.
struct ReadableUpdateTriple {
  std::array<std::string, 4> quad_;
  bool insertOrDelete_;

  bool operator==(const ReadableUpdateTriple& other) const {
    return quad_ == other.quad_ && insertOrDelete_ == other.insertOrDelete_;
  }
  friend std::ostream& operator<<(std::ostream& os,
                                  const ReadableUpdateTriple& triple) {
    os << (triple.insertOrDelete_ ? "+ " : "- ");
    for (const auto& component : triple.quad_) {
      os << component << ' ';
    }
    return os;
  }
};

// Parse the given `turtle` into `IdTriple`s, adding new words to `localVocab`.
// The triples are sorted and deduplicated, which `DeltaTriples` requires of its
// callers (see `ExecuteUpdate::sortAndRemoveDuplicates`).
std::vector<IdTriple<0>> makeIdTriples(const IndexImpl& index,
                                       LocalVocab& localVocab,
                                       const std::string& turtle) {
  RdfStringParser<TurtleParser<Tokenizer>> parser{&index.encodedIriManager()};
  parser.parseUtf8String(turtle);
  return ad_utility::transform(
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
}

// See above.
std::vector<IdTriple<0>> makeSortedIdTriples(const IndexImpl& index,
                                             LocalVocab& localVocab,
                                             const std::string& turtle) {
  auto triples = makeIdTriples(index, localVocab, turtle);
  ql::ranges::sort(triples);
  triples.erase(std::unique(triples.begin(), triples.end()), triples.end());
  return triples;
}

// Read all triples of the given permutation of `auxIndex` and convert them into
// their readable representation. The words are looked up in the vocabularies of
// `index` and of `auxIndex`.
std::vector<ReadableUpdateTriple> readAuxPermutation(
    const IndexImpl& index, const AuxIndex& auxIndex,
    Permutation::Enum permutationEnum, bool isInternal) {
  auto handle = std::make_shared<ad_utility::CancellationHandle<>>();
  auto [reader, blocks] =
      auxIndex.scanFull(permutationEnum, isInternal, handle);

  // Convert a single `Id` into a readable string. The `Id`s of the auxiliary
  // vocabulary are resolved there, all others in the main index.
  auto toString = [&index, &auxIndex](Id id) -> std::string {
    if (id.getDatatype() == Datatype::AuxVocabIndex) {
      return absl::StrCat("aux:",
                          std::string(auxIndex.vocab()[id.getAuxVocabIndex()]));
    }
    LocalVocab emptyLocalVocab;
    auto word = ql::exportIds::idToLiteralOrIri(index, id, emptyLocalVocab);
    return word.has_value() ? word.value().toStringRepresentation()
                            : absl::StrCat("id:", id.getBits());
  };

  std::vector<ReadableUpdateTriple> result;
  for (const IdTable& block : blocks) {
    for (size_t row = 0; row < block.numRows(); ++row) {
      result.push_back({{toString(block(row, 0)), toString(block(row, 1)),
                         toString(block(row, 2)), toString(block(row, 3))},
                        block(row, AuxIndex::insertOrDeleteColumn) ==
                            AuxIndex::insertedId()});
    }
  }
  return result;
}

// Fixture that sets up an index, applies updates to it, and builds auxiliary
// indices from them.
class AuxIndexBuilderTest : public ::testing::Test {
 protected:
  static constexpr const char* testTurtle =
      "<a> <p> <A> . <b> <p> <B> . <c> <p> <C> .";

  // The index is owned by the test (not taken from the cache of `getQec`),
  // because building an auxiliary index writes files next to it, and a leftover
  // auxiliary index would be picked up by a later test that uses the same base
  // name.
  std::unique_ptr<Index> index_ =
      std::make_unique<Index>(makeIndex(std::nullopt));
  // The base names of all auxiliary indices that were built, so that their
  // files can be removed again.
  std::vector<std::string> auxBasenames_;

  // Create the test index, with the given vocabulary type if it is set.
  static Index makeIndex(
      std::optional<ad_utility::VocabularyType> vocabularyType) {
    // Remove the auxiliary indices of a previous run of this test, else the
    // freshly built index would pick them up (see
    // `IndexImpl::loadNewestAuxIndexFromDisk`).
    std::string basename = gtestCurrentTestName();
    for (size_t generation : AuxIndex::generationsOnDisk(basename)) {
      AuxIndex::deleteFromDisk(AuxIndex::makeBasename(basename, generation));
    }
    ad_utility::testing::TestIndexConfig config{std::string{testTurtle}};
    config.vocabularyType = vocabularyType;
    // The WKT literals of the geo-split test are longer than the default parser
    // buffer.
    config.parserBufferSize = 10_kB;
    return ad_utility::testing::makeTestIndex(basename, std::move(config));
  }

  ~AuxIndexBuilderTest() override {
    for (const auto& basename : auxBasenames_) {
      AuxIndex::deleteFromDisk(basename);
    }
  }

  IndexImpl& impl() { return index_->getImpl(); }
  const IndexImpl& impl() const { return index_->getImpl(); }

  // Apply the given insertions and deletions to the delta triples of the index.
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

  // Build a new generation of the auxiliary index from the current delta
  // triples and load it.
  std::pair<qlever::AuxIndexBuildResult, std::unique_ptr<AuxIndex>> build() {
    auto handle = std::make_shared<ad_utility::CancellationHandle<>>();
    auto [state, localVocabEntries, ownedBlocks] =
        index_->deltaTriplesManager()
            .getCurrentLocatedTriplesSharedStateWithVocab();
    auto result = qlever::buildAuxIndex(impl(), *state, 10_MB, handle);
    auxBasenames_.push_back(result.basename_);

    auto auxIndex = std::make_unique<AuxIndex>(impl().allocator());
    const auto& locale = impl().configurationJson().at("locale");
    auxIndex->loadFromDisk(result.basename_, std::string{locale.at("language")},
                           std::string{locale.at("country")},
                           bool{locale.at("ignore-punctuation")});
    return {std::move(result), std::move(auxIndex)};
  }
};

// ____________________________________________________________________________
TEST_F(AuxIndexBuilderTest, buildFromInsertionsAndDeletions) {
  // `<d>` and `<D>` are new words, all the others are part of the vocabulary of
  // the main index.
  update("<d> <p> <D> .", "<a> <p> <A> .");
  auto [result, auxIndex] = build();

  EXPECT_EQ(result.metadata_.generation_, 0);
  EXPECT_EQ(result.metadata_.numInserted_, 1);
  EXPECT_EQ(result.metadata_.numDeleted_, 1);
  // The two new words of the inserted triple.
  EXPECT_EQ(result.metadata_.numVocabWords_, 2);
  EXPECT_EQ(auxIndex->vocab().numWords(), 2);
  EXPECT_EQ(auxIndex->vocab()[AuxVocabIndex::make(0)], "<d>");
  EXPECT_EQ(auxIndex->vocab()[AuxVocabIndex::make(1)], "<D>");

  // All six permutations hold both triples, each with its correct flag.
  auto defaultGraph = std::string{DEFAULT_GRAPH_IRI};
  EXPECT_THAT(
      readAuxPermutation(impl(), *auxIndex, Permutation::SPO, false),
      ::testing::UnorderedElementsAre(
          ReadableUpdateTriple{{"<a>", "<p>", "<A>", defaultGraph}, false},
          ReadableUpdateTriple{{"aux:<d>", "<p>", "aux:<D>", defaultGraph},
                               true}));
  EXPECT_THAT(
      readAuxPermutation(impl(), *auxIndex, Permutation::OPS, false),
      ::testing::UnorderedElementsAre(
          ReadableUpdateTriple{{"<A>", "<p>", "<a>", defaultGraph}, false},
          ReadableUpdateTriple{{"aux:<D>", "<p>", "aux:<d>", defaultGraph},
                               true}));
  EXPECT_THAT(
      readAuxPermutation(impl(), *auxIndex, Permutation::PSO, false),
      ::testing::UnorderedElementsAre(
          ReadableUpdateTriple{{"<p>", "<a>", "<A>", defaultGraph}, false},
          ReadableUpdateTriple{{"<p>", "aux:<d>", "aux:<D>", defaultGraph},
                               true}));
}

// ____________________________________________________________________________
TEST_F(AuxIndexBuilderTest, rebuildMergesWithPreviousGeneration) {
  update("<d> <p> <D> .", "<a> <p> <A> .");
  auto [firstResult, firstAux] = build();
  ASSERT_EQ(firstResult.metadata_.generation_, 0);

  std::string firstAuxVocabWord{firstAux->vocab()[AuxVocabIndex::make(0)]};
  // Make the first generation the current auxiliary index of the index, exactly
  // as the publication of a new generation does, and clear the delta triples
  // that it now covers.
  impl().setAuxIndexForTesting(std::move(firstAux));
  index_->deltaTriplesManager().clear();

  // The second round of updates: a new word `<e>`, a deletion of a triple that
  // the first generation inserted, and a re-insertion of a triple that the
  // first generation deleted. The latter two are exactly the cases where the
  // delta triples have to win over the auxiliary index.
  update("<e> <p> <a> . <a> <p> <A> .", "<d> <p> <D> .");
  auto [secondResult, secondAux] = build();

  EXPECT_EQ(secondResult.metadata_.generation_, 1);
  // The surviving triples are: `<e> <p> <a>` and `<a> <p> <A>` as insertions
  // and
  // `<d> <p> <D>` as a deletion.
  EXPECT_EQ(secondResult.metadata_.numInserted_, 2);
  EXPECT_EQ(secondResult.metadata_.numDeleted_, 1);
  // `<D>` and `<d>` are carried over from the first generation, `<e>` is new.
  EXPECT_EQ(secondResult.metadata_.numVocabWords_, 3);

  auto defaultGraph = std::string{DEFAULT_GRAPH_IRI};
  EXPECT_THAT(
      readAuxPermutation(impl(), *secondAux, Permutation::SPO, false),
      ::testing::UnorderedElementsAre(
          ReadableUpdateTriple{{"<a>", "<p>", "<A>", defaultGraph}, true},
          ReadableUpdateTriple{{"aux:<d>", "<p>", "aux:<D>", defaultGraph},
                               false},
          ReadableUpdateTriple{{"aux:<e>", "<p>", "<a>", defaultGraph}, true}));

  // The words that were carried over got new `Id`s, and the mapping translates
  // the `Id`s of the first generation into them.
  auto oldIdOfLowercaseD = Id::makeFromAuxVocabIndex(AuxVocabIndex::make(0));
  ASSERT_EQ(firstAuxVocabWord, "<d>");
  auto newId = secondResult.idMapping_.map(oldIdOfLowercaseD);
  ASSERT_TRUE(newId.has_value());
  EXPECT_EQ(secondAux->vocab()[newId.value().getAuxVocabIndex()], "<d>");
}

// ____________________________________________________________________________
TEST_F(AuxIndexBuilderTest, buildWithGeoSplitVocabulary) {
  // Rebuild the index with a geo-split vocabulary, so that the auxiliary
  // vocabulary splits its words the same way (see `AuxVocabulary`).
  index_ = std::make_unique<Index>(
      makeIndex(ad_utility::VocabularyType::OnDiskCompressedGeoSplit));
  ASSERT_TRUE(impl().getVocab().isGeoInfoAvailable());

  // Insert two new ordinary IRIs and two new WKT literals, which end up in
  // different sub-vocabularies of the auxiliary vocabulary. Note that a WKT
  // literal that is a `POINT` would be converted into an `Id` of type
  // `Datatype::GeoPoint` and would therefore not need a vocabulary entry at
  // all.
  auto wkt = [](std::string_view content) {
    return absl::StrCat("\"", content, "\"^^<", GEO_WKT_LITERAL, ">");
  };
  update(absl::StrCat("<d> <p> ", wkt("LINESTRING(1 1, 2 2)"), " . <e> <p> ",
                      wkt("POLYGON((1 1, 2 2, 3 1, 1 1))"), " ."));
  auto [result, auxIndex] = build();

  EXPECT_EQ(result.metadata_.numInserted_, 2);
  // Two new IRIs and two new WKT literals.
  EXPECT_EQ(result.metadata_.numVocabWords_, 4);

  // The two IRIs are in the first sub-vocabulary, the two WKT literals in the
  // second one, whose `Id`s carry the marker bit.
  auto markerBit = uint64_t{1} << 59;
  EXPECT_EQ(auxIndex->vocab()[AuxVocabIndex::make(0)], "<d>");
  EXPECT_EQ(auxIndex->vocab()[AuxVocabIndex::make(1)], "<e>");
  EXPECT_EQ(auxIndex->vocab()[AuxVocabIndex::make(markerBit)],
            wkt("LINESTRING(1 1, 2 2)"));
  EXPECT_EQ(auxIndex->vocab()[AuxVocabIndex::make(markerBit | 1)],
            wkt("POLYGON((1 1, 2 2, 3 1, 1 1))"));
  // The geometry information of the new WKT literals is precomputed, just as it
  // is in the main index.
  EXPECT_TRUE(auxIndex->vocab().vocab().isGeoInfoAvailable());
  EXPECT_TRUE(auxIndex->vocab()
                  .vocab()
                  .getGeoInfo(AuxVocabIndex::make(markerBit))
                  .has_value());

  auto defaultGraph = std::string{DEFAULT_GRAPH_IRI};
  EXPECT_THAT(
      readAuxPermutation(impl(), *auxIndex, Permutation::SPO, false),
      ::testing::UnorderedElementsAre(
          ReadableUpdateTriple{
              {"aux:<d>", "<p>",
               absl::StrCat("aux:", wkt("LINESTRING(1 1, 2 2)")), defaultGraph},
              true},
          ReadableUpdateTriple{
              {"aux:<e>", "<p>",
               absl::StrCat("aux:", wkt("POLYGON((1 1, 2 2, 3 1, 1 1))")),
               defaultGraph},
              true}));
}

// ____________________________________________________________________________
TEST_F(AuxIndexBuilderTest, buildWithoutAnyUpdates) {
  auto [result, auxIndex] = build();
  EXPECT_EQ(result.metadata_.numInserted_, 0);
  EXPECT_EQ(result.metadata_.numDeleted_, 0);
  EXPECT_EQ(result.metadata_.numVocabWords_, 0);
  EXPECT_TRUE(auxIndex->isEmpty());
  EXPECT_THAT(readAuxPermutation(impl(), *auxIndex, Permutation::SPO, false),
              ::testing::IsEmpty());
}

}  // namespace
