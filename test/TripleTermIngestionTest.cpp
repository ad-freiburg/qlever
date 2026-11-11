// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Prashanth Premakumar <prashanthp0703@gmail.com>, UFR

// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

// Tests for giving RDF 1.2 triple terms consistent IDs during index building:
// batch-local IDs in the `ItemMapManager`, global IDs after the vocabulary
// merge (`IndexImpl::mergeTripleTerms`), and the outer triples that refer to
// the terms.

#include <gtest/gtest.h>

#include <fstream>
#include <string>
#include <vector>

#include "engine/IndexScan.h"
#include "engine/Join.h"
#include "engine/QueryExecutionTree.h"
#include "engine/TripleTermScan.h"
#include "global/FileSuffixConstants.h"
#include "index/IndexBuilderTypes.h"
#include "index/IndexImpl.h"
#include "parser/SparqlTriple.h"
#include "util/File.h"
#include "util/GTestHelpers.h"
#include "util/IndexTestHelpers.h"
#include "util/TripleComponentTestHelpers.h"
#include "util/json.h"

namespace {
using ad_utility::testing::getQec;
using ad_utility::testing::iri;
using ad_utility::testing::makeGetId;
using Builder = qlever::tripleTermPrototype::TripleTermIndexBuilder;

// The running example of the design note. With the two triples per batch of
// the test indices, the two mentions of `alice p bob` are in different
// partial vocabularies, and so are their components.
const std::string reports =
    "<http://ex/reportA> <http://ex/reifies> "
    "<<( <http://ex/alice> <http://ex/p> <http://ex/bob> )>> .\n"
    "<http://ex/reportA> <http://ex/date> \"2026\" .\n"
    "<http://ex/reportB> <http://ex/reifies> "
    "<<( <http://ex/alice> <http://ex/p> <http://ex/bob> )>> .\n"
    "<http://ex/reportC> <http://ex/reifies> "
    "<<( <http://ex/alice> <http://ex/p> <http://ex/charlie> )>> .\n";

Id term(uint64_t index) {
  return Id::makeFromTripleTermIndex(TripleTermIndex::make(index));
}

// Build an index from `turtle` and return the `Index` that built it. The
// triple-term dictionary is not persisted yet, so it is only available on that
// object, and the test helpers (`getQec`, `makeTestIndex`) cannot be used for
// it. The index files are written to `basename`, so the index can additionally
// be loaded from disk.
Index buildIndex(const std::string& basename, const std::string& turtle,
                 size_t triplesPerBatch,
                 std::vector<std::string> encodedPrefixes = {}) {
  std::string filename = basename + ".ttl";
  {
    std::ofstream f{filename};
    f << turtle;
  }
  Index index = ad_utility::testing::makeIndexWithTestSettings();
  index.setNumTriplesPerBatch(triplesPerBatch);
  index.setOnDiskBase(basename);
  if (!encodedPrefixes.empty()) {
    index.getImpl().setPrefixesForEncodedValues(std::move(encodedPrefixes));
  }
  index.createFromFiles({qlever::InputFileSpecification{
                            filename, qlever::Filetype::Turtle, std::nullopt}},
                        1);
  return index;
}

// All the terms of `builder`, in the order of their `TripleTermIndex`.
std::vector<Builder::Triple> allTerms(const Builder& builder) {
  std::vector<Builder::Triple> result;
  for (size_t i = 0; i < builder.size(); ++i) {
    result.push_back(builder.resolve(TripleTermIndex::make(i)));
  }
  return result;
}
}  // namespace

// _____________________________________________________________________________
TEST(TripleTermIngestion, ItemMapManagerInternsTermsPerBatch) {
  TripleComponentComparator comparator;
  ItemMapManager map{0, &comparator};
  auto c = [](std::string_view s) {
    return PossiblyExternalizedTripleComponent{iri(s)};
  };
  TripleTermComponents bob{c("<a>"), c("<p>"), c("<b>")};
  Id first = map.getTripleTermId(bob);
  EXPECT_EQ(first, term(0));
  EXPECT_EQ(map.getTripleTermId(bob), first);
  EXPECT_EQ(map.getTripleTermId({c("<a>"), c("<p>"), c("<c>")}), term(1));

  // The components are ordinary words of the partial vocabulary: they have the
  // same local IDs as the same IRIs in an ordinary triple.
  ASSERT_EQ(map.map_.tripleTerms_.size(), 2);
  const TripleTermIds expected{map.getId(c("<a>")), map.getId(c("<p>")),
                               map.getId(c("<b>"))};
  EXPECT_EQ(map.map_.tripleTerms_[0], expected);

  // A new partial vocabulary starts numbering its terms from zero again.
  map.clear();
  EXPECT_TRUE(map.map_.tripleTerms_.empty());
  EXPECT_EQ(map.getTripleTermId({c("<a>"), c("<p>"), c("<c>")}), term(0));
}

// _____________________________________________________________________________
TEST(TripleTermIngestion, OuterTriplesReferToSharedTerms) {
  auto* qec = getQec(reports);
  auto getId = makeGetId(qec->getIndex());
  auto scan = [qec](SparqlTripleSimple triple, Permutation::Enum permutation) {
    auto tree = ad_utility::makeExecutionTree<IndexScan>(qec, permutation,
                                                         std::move(triple));
    return std::pair{tree->getResult(), tree->getVariableColumns()};
  };

  // `?report <reifies> ?term`
  auto [result, columns] =
      scan({Variable{"?report"}, iri("<http://ex/reifies>"), Variable{"?term"}},
           Permutation::PSO);
  const auto& table = result->idTableView();
  ASSERT_EQ(table.numRows(), 3);
  auto reportCol = columns.at(Variable{"?report"}).columnIndex_;
  auto termCol = columns.at(Variable{"?term"}).columnIndex_;
  ad_utility::HashMap<Id, Id> termOfReport;
  for (size_t row = 0; row < table.numRows(); ++row) {
    termOfReport[table(row, reportCol)] = table(row, termCol);
  }
  Id a = termOfReport.at(getId("<http://ex/reportA>"));
  Id b = termOfReport.at(getId("<http://ex/reportB>"));
  Id c = termOfReport.at(getId("<http://ex/reportC>"));
  // Both mentions of `alice p bob` get the same ID, although they were in
  // different partial vocabularies. The IDs are numbered in the sorted order of
  // the components, and `bob` sorts before `charlie`.
  EXPECT_EQ(a, b);
  EXPECT_EQ(a, term(0));
  EXPECT_EQ(c, term(1));

  // Mentioning a term does not assert it: there is no triple `alice p ?x`.
  auto aliceTriples =
      scan({iri("<http://ex/alice>"), Variable{"?p"}, Variable{"?o"}},
           Permutation::SPO)
          .first;
  EXPECT_EQ(aliceTriples->idTableView().numRows(), 0);
}

// _____________________________________________________________________________
TEST(TripleTermIngestion, DictionaryHasGlobalComponentIds) {
  const std::string basename = "tripleTermIngestionDictionary";
  Index builder = buildIndex(basename, reports, 2);
  const auto& terms = builder.getImpl().tripleTerms();
  ASSERT_EQ(terms.size(), 2);

  // The components are the IDs of the loaded index, not partial IDs.
  Index loaded{ad_utility::makeUnlimitedAllocator<Id>()};
  loaded.createFromOnDiskIndex(basename, false);
  auto getId = makeGetId(loaded);
  Id alice = getId("<http://ex/alice>");
  Id p = getId("<http://ex/p>");
  const std::vector<Builder::Triple> expected{
      {alice, p, getId("<http://ex/bob>")},
      {alice, p, getId("<http://ex/charlie>")}};
  EXPECT_EQ(allTerms(terms), expected);
  // Loading restores exactly the same terms and IDs.
  EXPECT_EQ(allTerms(loaded.getImpl().tripleTerms()), expected);

  // The POIS scan finds the subject of `?person <p> <bob>` and its term.
  const std::vector<Builder::Match> matches{{alice, TripleTermIndex::make(0)}};
  EXPECT_EQ(terms.scan(p, getId("<http://ex/bob>")), matches);
}

// _____________________________________________________________________________
TEST(TripleTermIngestion, IdsDoNotDependOnThePartitioning) {
  // Many small partial vocabularies vs. a single one.
  Index small = buildIndex("tripleTermIngestionSmallBatches", reports, 1);
  Index large = buildIndex("tripleTermIngestionLargeBatch", reports, 1000);
  ASSERT_EQ(small.getImpl().tripleTerms().size(), 2);
  EXPECT_EQ(allTerms(small.getImpl().tripleTerms()),
            allTerms(large.getImpl().tripleTerms()));
}

// _____________________________________________________________________________
TEST(TripleTermIngestion, EncodedComponentsAreGlobalIds) {
  // IRIs of the form `<prefix><number>` are encoded into the `Id`, also when
  // they are the components of a triple term.
  const std::string turtle =
      "<http://enc/1> <http://ex/reifies> "
      "<<( <http://enc/2> <http://ex/p> <http://enc/3> )>> .\n";
  Index index =
      buildIndex("tripleTermIngestionEncoded", turtle, 2, {"http://enc/"});
  const auto& terms = index.getImpl().tripleTerms();
  ASSERT_EQ(terms.size(), 1);
  auto components = terms.resolve(TripleTermIndex::make(0));
  const auto& manager = index.getImpl().encodedIriManager();
  EXPECT_EQ(components[0], manager.encode("<http://enc/2>").value());
  EXPECT_EQ(components[1].getDatatype(), Datatype::VocabIndex);
  EXPECT_EQ(components[2], manager.encode("<http://enc/3>").value());
}

// _____________________________________________________________________________
TEST(TripleTermIngestion, InputWithoutTriplesTermsHasNoTerms) {
  Index index = buildIndex("tripleTermIngestionNoTerms",
                           "<http://ex/a> <http://ex/p> <http://ex/b> .\n", 2);
  EXPECT_EQ(index.getImpl().tripleTerms().size(), 0);
}

// _____________________________________________________________________________
TEST(TripleTermIngestion, LoadedIndexHasTheTermsOfTheBuild) {
  // `getQec` builds the index and then loads it from disk into another
  // `Index`, so the terms must have been persisted.
  auto* qec = getQec(reports);
  auto getId = makeGetId(qec->getIndex());
  Id alice = getId("<http://ex/alice>");
  Id p = getId("<http://ex/p>");
  const std::vector<Builder::Triple> expected{
      {alice, p, getId("<http://ex/bob>")},
      {alice, p, getId("<http://ex/charlie>")}};
  EXPECT_EQ(allTerms(qec->getIndex().getImpl().tripleTerms()), expected);
}

// _____________________________________________________________________________
TEST(TripleTermIngestion, LoadedTermsJoinWithTheOuterTriples) {
  // `?report <reifies> ?term . ?term = <<( ?person <p> <bob> )>>`, assembled
  // from operations by hand (there is no SPARQL syntax yet), but both inputs
  // come from the loaded index: the outer triples from an ordinary `IndexScan`
  // and the term bindings from the persisted triple terms.
  auto* qec = getQec(reports);
  auto getId = makeGetId(qec->getIndex());
  Variable report{"?report"};
  Variable person{"?person"};
  Variable termVar{"?term"};
  // POS with a fixed predicate is sorted by the object, i.e. by `?term`.
  auto outer = ad_utility::makeExecutionTree<IndexScan>(
      qec, Permutation::POS,
      SparqlTripleSimple{report, iri("<http://ex/reifies>"), termVar});
  auto inner = qlever::tripleTermPrototype::makeTripleTermScan(
      qec, qec->getIndex().getImpl().tripleTerms(), getId("<http://ex/p>"),
      getId("<http://ex/bob>"), person, termVar);
  auto outerCol = outer->getVariableColumns().at(termVar).columnIndex_;
  auto innerCol = inner->getVariableColumns().at(termVar).columnIndex_;
  auto join = ad_utility::makeExecutionTree<Join>(qec, outer, inner, outerCol,
                                                  innerCol);
  auto result = join->getResult();
  const auto& table = result->idTableView();
  const auto& columns = join->getVariableColumns();
  std::vector<std::pair<Id, Id>> rows;
  for (size_t row = 0; row < table.numRows(); ++row) {
    rows.emplace_back(table(row, columns.at(report).columnIndex_),
                      table(row, columns.at(person).columnIndex_));
  }
  ql::ranges::sort(rows);
  std::vector<std::pair<Id, Id>> expected{
      {getId("<http://ex/reportA>"), getId("<http://ex/alice>")},
      {getId("<http://ex/reportB>"), getId("<http://ex/alice>")}};
  ql::ranges::sort(expected);
  EXPECT_EQ(rows, expected);
}

// _____________________________________________________________________________
TEST(TripleTermIngestion, MissingTermsFileIsAnError) {
  const std::string basename = "tripleTermIngestionMissingFile";
  buildIndex(basename, reports, 2);
  ad_utility::deleteFile(absl::StrCat(basename, TRIPLE_TERMS_FILE_SUFFIX));
  Index loaded{ad_utility::makeUnlimitedAllocator<Id>()};
  AD_EXPECT_THROW_WITH_MESSAGE(loaded.createFromOnDiskIndex(basename, false),
                               ::testing::HasSubstr("rebuild the index"));
}

// _____________________________________________________________________________
TEST(TripleTermIngestion, IndexFromBeforeTripleTermsHasNoTerms) {
  // An index built before triple terms were supported has neither the file nor
  // the configuration entry. Simulate it by removing both.
  const std::string basename = "tripleTermIngestionOldIndex";
  buildIndex(basename, "<http://ex/a> <http://ex/p> <http://ex/b> .\n", 2);
  ad_utility::deleteFile(absl::StrCat(basename, TRIPLE_TERMS_FILE_SUFFIX));
  std::string configFile = absl::StrCat(basename, CONFIGURATION_FILE);
  auto config = fileToJson<nlohmann::json>(configFile);
  ASSERT_TRUE(config.contains("num-triple-terms"));
  config.erase("num-triple-terms");
  {
    std::ofstream f{configFile};
    f << config.dump();
  }
  Index loaded{ad_utility::makeUnlimitedAllocator<Id>()};
  loaded.createFromOnDiskIndex(basename, false);
  EXPECT_EQ(loaded.getImpl().tripleTerms().size(), 0);
}
