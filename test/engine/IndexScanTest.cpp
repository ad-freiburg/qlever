//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gtest/gtest.h>

#include <memory>

#include "../test/PrefilterExpressionTestHelpers.h"
#include "../util/GTestHelpers.h"
#include "../util/IdTableHelpers.h"
#include "../util/IndexTestHelpers.h"
#include "../util/TripleComponentTestHelpers.h"
#include "engine/IndexScan.h"
#include "index/IndexImpl.h"
#include "parser/ParsedQuery.h"

using namespace ad_utility::testing;
using namespace std::chrono_literals;
using ad_utility::source_location;

namespace {
using Tc = TripleComponent;
using Var = Variable;
using LazyResult = Result::LazyResult;

using IndexPair = std::pair<size_t, size_t>;

constexpr auto encodedIriManager = []() -> const EncodedIriManager* {
  static EncodedIriManager encodedIriManager_;
  return &encodedIriManager_;
};

// NOTE: All the following helper functions always use the `PSO` permutation to
// set up index scans unless explicitly stated otherwise.

// Test that the `partialLazyScanResult` when being materialized to a single
// `IdTable` yields a subset of the full result of the `fullScan`. The `subset`
// is specified via the `expectedRows`, for example {{1, 3}, {7, 8}} means that
// the partialLazyScanResult shall contain the rows number `1, 2, 7` of the full
// scan (upper bounds are not included).
void testLazyScan(
    CompressedRelationReader::IdTableGeneratorInputRange partialLazyScanResult,
    IndexScan& fullScan, const std::vector<IndexPair>& expectedRows,
    const LimitOffsetClause& limitOffset = {},
    source_location l = AD_CURRENT_SOURCE_LOC()) {
  auto t = generateLocationTrace(l);
  auto alloc = ad_utility::makeUnlimitedAllocator<Id>();
  IdTable lazyScanRes{0, alloc};
  size_t numBlocks = 0;
  for (const auto& block : partialLazyScanResult) {
    if (lazyScanRes.empty()) {
      lazyScanRes.setNumColumns(block.numColumns());
    }
    lazyScanRes.insertAtEnd(block);
    ++numBlocks;
  }

  if (limitOffset.isUnconstrained()) {
    EXPECT_EQ(numBlocks, partialLazyScanResult.details().numBlocksRead_);
    // The number of read elements might be a bit larger than the final result
    // size, because the first and/or last block might be incomplete, meaning
    // that they have to be completely read, but only partially contribute to
    // the result.
    EXPECT_LE(lazyScanRes.size(),
              partialLazyScanResult.details().numElementsRead_);
  }

  auto resFullScan = fullScan.getResult()->idTable().clone();
  IdTable expected{resFullScan.numColumns(), alloc};

  if (limitOffset.isUnconstrained()) {
    for (auto [lower, upper] : expectedRows) {
      for (auto index : ql::views::iota(lower, upper)) {
        expected.push_back(resFullScan.at(index));
      }
    }
  } else {
    // as soon as a limit clause is applied, we currently ignore the block
    // filter, thus the result of the lazy and the materialized scan become the
    // same.
    expected = resFullScan.clone();
  }

  if (limitOffset.isUnconstrained()) {
    EXPECT_EQ(lazyScanRes, expected);
  } else {
    // If the join on blocks could already determine that there are no matching
    // blocks, then the lazy scan will be empty even with a limit present.
    EXPECT_TRUE((lazyScanRes.empty() && expectedRows.empty()) ||
                lazyScanRes == expected);
  }
}

// Test that when two scans are set up (specified by `tripleLeft` and
// `tripleRight`) on the given knowledge graph), and each scan is lazily
// executed and only contains the blocks that are needed to join both scans,
// then the resulting lazy partial scans only contain the subset of the
// respective full scans as specified by `leftRows` and `rightRows`. For the
// specification of the subset see above.
void testLazyScanForJoinOfTwoScans(
    const std::string& kgTurtle, const SparqlTripleSimple& tripleLeft,
    const SparqlTripleSimple& tripleRight,
    const std::vector<IndexPair>& leftRows,
    const std::vector<IndexPair>& rightRows,
    ad_utility::MemorySize blocksizePermutations = 16_B,
    source_location l = AD_CURRENT_SOURCE_LOC()) {
  auto t = generateLocationTrace(l);
  // As soon as there is a LIMIT clause present, we cannot use the prefiltered
  // blocks.
  std::vector<LimitOffsetClause> limits{{}, {12, 3}, {2, 3}};
  for (const auto& limit : limits) {
    TestIndexConfig config{kgTurtle};
    config.blocksizePermutations = blocksizePermutations;
    auto qec = getQec(std::move(config));
    IndexScan s1{qec, Permutation::PSO, tripleLeft};
    s1.applyLimitOffset(limit);
    IndexScan s2{qec, Permutation::PSO, tripleRight};
    auto implForSwitch = [](IndexScan& l, IndexScan& r, const auto& expectedL,
                            const auto& expectedR,
                            const LimitOffsetClause& limitL,
                            const LimitOffsetClause& limitR) {
      auto [scan1, scan2] = (IndexScan::lazyScanForJoinOfTwoScans(l, r));

      testLazyScan(std::move(scan1), l, expectedL, limitL);
      testLazyScan(std::move(scan2), r, expectedR, limitR);
    };
    implForSwitch(s1, s2, leftRows, rightRows, limit, {});
    implForSwitch(s2, s1, rightRows, leftRows, {}, limit);
  }
}

// Test that setting up the lazy partial scans between `tripleLeft` and
// `tripleRight` on the given `kg` throws an exception.
void testLazyScanThrows(const std::string& kg,
                        const SparqlTripleSimple& tripleLeft,
                        const SparqlTripleSimple& tripleRight,
                        source_location l = AD_CURRENT_SOURCE_LOC()) {
  auto t = generateLocationTrace(l);
  auto qec = getQec(kg);
  IndexScan s1{qec, Permutation::PSO, tripleLeft};
  IndexScan s2{qec, Permutation::PSO, tripleRight};
  EXPECT_ANY_THROW(IndexScan::lazyScanForJoinOfTwoScans(s1, s2));
}

// Test that a lazy partial scan for a join of the `scanTriple` with a
// materialized join column result that is specified by the `columnEntries`
// yields only the subsets specified by the `expectedRows`.
void testLazyScanForJoinWithColumn(
    const std::string& kg, const SparqlTripleSimple& scanTriple,
    std::vector<TripleComponent> columnEntries,
    const std::vector<IndexPair>& expectedRows,
    source_location l = AD_CURRENT_SOURCE_LOC()) {
  auto t = generateLocationTrace(l);
  auto qec = getQec(kg);
  IndexScan scan{qec, Permutation::PSO, scanTriple};
  std::vector<Id> column;
  for (const auto& entry : columnEntries) {
    column.push_back(
        entry.toValueId(qec->getIndex().getVocab(), *encodedIriManager())
            .value());
  }

  auto lazyScan = scan.lazyScanForJoinOfColumnWithScan(column);
  testLazyScan(std::move(lazyScan), scan, expectedRows);
}

// Test the same scenario as the previous function, but assumes that the
// setting up of the lazy scan fails with an exception.
void testLazyScanWithColumnThrows(
    const std::string& kg, const SparqlTripleSimple& scanTriple,
    const std::vector<TripleComponent>& columnEntries,
    source_location l = AD_CURRENT_SOURCE_LOC()) {
  auto t = generateLocationTrace(l);
  auto qec = getQec(kg);
  IndexScan s1{qec, Permutation::PSO, scanTriple};
  std::vector<Id> column;
  for (const auto& entry : columnEntries) {
    column.push_back(
        entry.toValueId(qec->getIndex().getVocab(), *encodedIriManager())
            .value());
  }

  // We need this to suppress the warning about a [[nodiscard]] return value
  // being unused.
  auto makeScan = [&column, &s1]() {
    [[maybe_unused]] auto scan = s1.lazyScanForJoinOfColumnWithScan(column);
  };
  EXPECT_ANY_THROW(makeScan());
}

//______________________________________________________________________________
// Check that the `IndexScan` computes correct prefiltered `IdTable`s w.r.t.
// the applied `PrefilterExpression` given a `<PrefilterExpression, Variable>`
// pair was successfully set. In addition, if
// `std::optional<IndexScan::PrefilterVariablePair> pr2` contains a value, its
// additional `PrefilterExpression` is also applied (if possible). This
// corresponds logically to a conjunction over two `PrefilterExpression`s
// applied. For convenience we assert this for the IdTable column on which the
// `PrefilterExpression` was applied.
const auto testSetAndMakeScanWithPrefilterExpr =
    [](const std::string& kg, const SparqlTripleSimple& triple,
       const Permutation::Enum permutation,
       IndexScan::PrefilterVariablePair pr1,
       const std::vector<ValueId>& expectedIdsOnFilterColumn,
       bool prefilterCanBeSet,
       std::optional<IndexScan::PrefilterVariablePair> pr2 = std::nullopt,
       source_location l = AD_CURRENT_SOURCE_LOC()) {
      auto t = generateLocationTrace(l);
      IndexScan scan{getQec(kg), permutation, triple};
      auto variable = pr1.second;
      auto optUpdatedQet = scan.setPrefilterGetUpdatedQueryExecutionTree(
          makeFilterExpression::filterHelper::makePrefilterVec(std::move(pr1)));
      if (pr2.has_value() && optUpdatedQet.has_value()) {
        // Testing with a second `PrefilterExpression`s only makes sense if the
        // first `PrefilterExpression` was successfully applied.
        ASSERT_TRUE(optUpdatedQet.has_value());
        optUpdatedQet =
            optUpdatedQet.value()
                ->setPrefilterGetUpdatedQueryExecutionTree(
                    makeFilterExpression::filterHelper::makePrefilterVec(
                        std::move(pr2.value())))
                .value_or(optUpdatedQet.value());
      }
      if (optUpdatedQet.has_value()) {
        auto updatedQet = optUpdatedQet.value();
        ASSERT_TRUE(prefilterCanBeSet);
        // Check that the prefiltering procedure yields the correct result given
        // that the <PrefilterExpression, Variable> pair is correctly assigned
        // to the IndexScan.
        IdTable idTableFiltered = updatedQet->getRootOperation()
                                      ->computeResultOnlyForTesting()
                                      .idTable()
                                      .clone();
        auto isColumnIdSpan =
            idTableFiltered.getColumn(updatedQet->getVariableColumn(variable));
        ASSERT_EQ(
            (std::vector<Id>{isColumnIdSpan.begin(), isColumnIdSpan.end()}),
            expectedIdsOnFilterColumn);
      } else {
        // Check our prediction that the prefilter with the given
        // <PrefilterExpression, Variable> pair is not applicable (no updated
        // QueryExecutionTree is returned).
        ASSERT_FALSE(prefilterCanBeSet);
      }
    };

}  // namespace

TEST(IndexScan, lazyScanForJoinOfTwoScans) {
  SparqlTripleSimple xpy{Tc{Var{"?x"}}, iri("<p>"), Tc{Var{"?y"}}};
  SparqlTripleSimple xqz{Tc{Var{"?x"}}, iri("<q>"), Tc{Var{"?z"}}};
  {
    // In the tests we have a blocksize of two triples per block, and a new
    // block is started for a new relation. That explains the spacing of the
    // following example knowledge graphs.
    std::string kg =
        "<a> <p> <A>. <a> <p> <A2>. "
        "<a> <p> <A3> . <b> <p> <B>. "
        "<b> <p> <B2> ."
        "<b> <q> <xb>. <b> <q> <xb2> .";

    // When joining the <p> and <x> relations, we only need to read the last two
    // blocks of the <p> relation, as <a> never appears as a subject in <x>.
    // This means that the lazy partial scan can skip the first two triples.
    testLazyScanForJoinOfTwoScans(kg, xpy, xqz, {{2, 5}}, {{0, 2}});
  }
  {
    std::string kg =
        "<a> <p2> <A>. <a> <p2> <A2>. "
        "<a> <p2> <A3> . <b> <p2> <B>. "
        "<b> <q2> <xb>. <b> <q2> <xb2> .";
    // No triple for relation <p> (which doesn't even appear in the knowledge
    // graph), so both lazy scans are empty.
    testLazyScanForJoinOfTwoScans(kg, xpy, xqz, {}, {});
  }
  {
    // No triple for relation <x> (which does appear in the knowledge graph, but
    // not as a predicate), so both lazy scans are empty.
    std::string kg =
        "<a> <p> <A>. <a> <p> <A2>. "
        "<a> <p> <A3> . <b> <p> <B>. "
        "<b> <x2> <x>. <b> <x2> <xb2> .";
    testLazyScanForJoinOfTwoScans(kg, xpy, xqz, {}, {});
  }
  SparqlTripleSimple bpx{Tc{iri("<b>")}, iri("<p>"), Tc{Var{"?x"}}};
  {
    std::string kg =
        "<a> <p> <a1>. <a> <p> <a2>. "
        "<a> <p> <a3> . <b> <p> <x1>. "
        "<b> <p> <x2> . <b> <p> <x3>. "
        "<b> <p> <x4> . <b> <p> <x7>. "
        "<x2> <q> <xb>. <x5> <q> <xb2> ."
        "<x5> <q> <xb>. <x9> <q> <xb2> ."
        "<x91> <q> <xb>. <x93> <q> <xb2> .";
    testLazyScanForJoinOfTwoScans(kg, bpx, xqz, {{1, 5}}, {{0, 4}});
  }
  {
    // In this example we use 3 triples per block (24 bytes) and the `<p>`
    // permutation is standing in a single block together with the previous
    // `<o>` relation. The lazy scans are however still aware that the relevant
    // part of the block (`<b> <p> ?x`) only  goes from `<x80>` through `<x90>`,
    // so it is not necessary to scan the first block of the `<q>` relation
    // which only has subjects <= `<x5>`.
    std::string kg =
        "<a> <o> <a1>. <b> <p> <x80>. <b> <p> <x90>. "
        "<x2> <q> <xb>. <x5> <q> <xb2> . <x5> <q> <xb>. "
        "<x9> <q> <xb2> . <x91> <q> <xb>. <x93> <q> <xb2> .";
    testLazyScanForJoinOfTwoScans(kg, bpx, xqz, {{0, 2}}, {{3, 6}}, 24_B);
  }
  {
    std::string kg =
        "<a> <p> <a1>. <a> <p> <a2>. "
        "<a> <p> <a3> . <b> <p> <x1>. "
        "<x2> <q> <xb>. <x5> <q> <xb2> ."
        "<x5> <q> <xb>. <x9> <q> <xb2> ."
        "<x91> <q> <xb>. <x93> <q> <xb2> .";
    // Scan for a fixed subject that appears in the kg but not as the subject of
    // the <p> predicate.
    SparqlTripleSimple xb2px{Tc{iri("<xb2>")}, iri("<p>"), Tc{Var{"?x"}}};
    testLazyScanForJoinOfTwoScans(kg, bpx, xqz, {}, {});
  }
  {
    std::string kg =
        "<a> <p> <a1>. <a> <p> <a2>. "
        "<a> <p> <a3> . <b> <p> <x1>. "
        "<x2> <q> <xb>. <x5> <q> <xb2> ."
        "<x5> <q> <xb>. <x9> <q> <xb2> ."
        "<x91> <q> <xb>. <x93> <q> <xb2> .";
    // Scan for a fixed subject that is not even in the knowledge graph.
    SparqlTripleSimple xb2px{Tc{iri("<notInKg>")}, iri("<p>"), Tc{Var{"?x"}}};
    testLazyScanForJoinOfTwoScans(kg, bpx, xqz, {}, {});
  }

  // Corner cases
  {
    std::string kg = "<a> <b> <c> .";
    SparqlTripleSimple xyz{Tc{Var{"?x"}}, Var{"?y"}, Tc{Var{"?z"}}};
    testLazyScanThrows(kg, xyz, xqz);
    testLazyScanThrows(kg, xyz, xqz);
    testLazyScanThrows(kg, xyz, xyz);
    testLazyScanThrows(kg, xqz, xyz);

    // The first variable must be matching (subject variable is ?a vs ?x)
    SparqlTripleSimple abc{Tc{Var{"?a"}}, iri("<b>"), Tc{Var{"?c"}}};
    testLazyScanThrows(kg, abc, xqz);

    // If both scans have two variables, then the second variable must not
    // match.
    testLazyScanThrows(kg, abc, abc);
  }
}

TEST(IndexScan, lazyScanForJoinOfColumnWithScanTwoVariables) {
  SparqlTripleSimple xpy{Tc{Var{"?x"}}, iri("<p>"), Tc{Var{"?y"}}};
  // In the tests we have a blocksize of two triples per block, and a new
  // block is started for a new relation. That explains the spacing of the
  // following example knowledge graphs.
  std::string kg =
      "<a> <p> <A>. <a> <p> <A2>. "
      "<a> <p> <A3> . <b> <p> <B>. "
      "<b> <p> <B2> ."
      "<b> <q> <xb>. <b> <q> <xb2> .";
  {
    std::vector<TripleComponent> column{iri("<a>"), iri("<b>"), iri("<q>"),
                                        iri("<xb>")};
    // We need to scan all the blocks that contain the `<p>` predicate.
    testLazyScanForJoinWithColumn(kg, xpy, column, {{0, 5}});
  }
  {
    std::vector<TripleComponent> column{iri("<b>"), iri("<q>"), iri("<xb>")};
    // The first block only contains <a> which doesn't appear in the first
    // block.
    testLazyScanForJoinWithColumn(kg, xpy, column, {{2, 5}});
  }
  {
    std::vector<TripleComponent> column{iri("<a>"), iri("<q>"), iri("<xb>")};
    // The first block only contains <a> which only appears in the first two
    // blocks.
    testLazyScanForJoinWithColumn(kg, xpy, column, {{0, 4}});
  }
  {
    std::vector<TripleComponent> column{iri("<a>"), iri("<q>"), iri("<xb>")};
    // <f> does not appear as a predicate, so the result is empty.
    SparqlTripleSimple efg{Tc{Var{"?e"}}, iri("<f>"), Tc{Var{"?g"}}};
    testLazyScanForJoinWithColumn(kg, efg, column, {});
  }
}

TEST(IndexScan, lazyScanForJoinOfColumnWithScanOneVariable) {
  SparqlTripleSimple bpy{Tc{iri("<b>")}, iri("<p>"), Tc{Var{"?x"}}};
  std::string kg =
      "<a> <p> <s0>. <a> <p> <s7>. "
      "<a> <p> <s99> . <b> <p> <s0>. "
      "<b> <p> <s2> . <b> <p> <s3>. "
      "<b> <p> <s6> . <b> <p> <s9>. "
      "<b> <q> <s3>. <b> <q> <s5> .";
  {
    // The subject (<b>) and predicate (<b>) are fixed, so the object is the
    // join column
    std::vector<TripleComponent> column{iri("<s0>"), iri("<s7>"), iri("<s99>")};
    // We don't need to scan the middle block that only has <s2> and <s3>
    testLazyScanForJoinWithColumn(kg, bpy, column, {{0, 1}, {3, 5}});
  }
}

TEST(IndexScan, lazyScanForJoinOfColumnWithScanCornerCases) {
  SparqlTripleSimple threeVars{Tc{Var{"?x"}}, Var{"?b"}, Tc{Var{"?y"}}};
  std::string kg =
      "<a> <p> <A>. <a> <p> <A2>. "
      "<a> <p> <A3> . <b> <p> <B>. "
      "<b> <p> <B2> ."
      "<b> <q> <xb>. <b> <q> <xb2> .";

  // Full index scan (three variables).
  std::vector<TripleComponent> column{iri("<a>"), iri("<b>"), iri("<q>"),
                                      iri("<xb>")};
  // only `<q>` matches (we join on the predicate), so we only get the last
  // block.
  testLazyScanForJoinWithColumn(kg, threeVars, column, {{5, 7}});

  // The join column must be sorted.
  if constexpr (ad_utility::areExpensiveChecksEnabled) {
    std::vector<TripleComponent> unsortedColumn{iri("<a>"), iri("<b>"),
                                                iri("<a>")};
    SparqlTripleSimple xpy{Tc{Var{"?x"}}, iri("<p>"), Tc{Var{"?y"}}};
    testLazyScanWithColumnThrows(kg, xpy, unsortedColumn);
  }
}

TEST(IndexScan, additionalColumn) {
  auto qec = getQec("<x> <y> <z>.");
  using V = Variable;
  SparqlTripleSimple triple{V{"?x"}, iri("<y>"), V{"?z"}};
  triple.additionalScanColumns_.emplace_back(
      ADDITIONAL_COLUMN_INDEX_SUBJECT_PATTERN, V{"?xpattern"});
  triple.additionalScanColumns_.emplace_back(
      ADDITIONAL_COLUMN_INDEX_OBJECT_PATTERN, V{"?ypattern"});
  auto scan = IndexScan{qec, Permutation::PSO, triple};
  ASSERT_EQ(scan.getResultWidth(), 4);
  auto col = makeAlwaysDefinedColumn;
  VariableToColumnMap expected = {{V{"?x"}, col(0)},
                                  {V{"?z"}, col(1)},
                                  {V("?xpattern"), col(2)},
                                  {V("?ypattern"), col(3)}};
  ASSERT_THAT(scan.getExternallyVisibleVariableColumns(),
              ::testing::UnorderedElementsAreArray(expected));
  ASSERT_THAT(scan.getCacheKey(),
              ::testing::ContainsRegex("Additional Columns: 4 5"));
  auto res = scan.computeResultOnlyForTesting();
  auto getId = makeGetId(qec->getIndex());
  auto I = IntId;
  // <x> is the only subject, so it has pattern 0, <z> doesn't appear as a
  // subject, so it has no pattern.
  auto exp = makeIdTableFromVector(
      {{getId("<x>"), getId("<z>"), I(0), I(Pattern::NoPattern)}});
  EXPECT_THAT(res.idTable(), ::testing::ElementsAreArray(exp));
}

// Test that the graphs by which an `IndexScan` is to be filtered is correctly
// reflected in its cache key and its `ScanSpecification`.
TEST(IndexScan, namedGraphs) {
  auto qec = getQec("<x> <y> <z>.");
  using V = Variable;
  SparqlTripleSimple triple{V{"?x"}, iri("<y>"), V{"?z"}};
  ad_utility::HashSet<TripleComponent> graphs{
      TripleComponent::Iri::fromIriref("<graph1>"),
      TripleComponent::Iri::fromIriref("<graph2>")};
  auto scan = IndexScan{qec, Permutation::PSO, triple,
                        IndexScan::Graphs::Whitelist(graphs)};
  using namespace testing;
  EXPECT_EQ(scan.graphsToFilter(), IndexScan::Graphs::Whitelist(graphs));
  // HashSet order is non-deterministic.
  EXPECT_THAT(scan.getCacheKey(),
              AnyOf(HasSubstr("GRAPHS: Whitelist <graph1> <graph2>"),
                    HasSubstr("GRAPHS: Whitelist <graph2> <graph1>")));
  EXPECT_THAT(scan.getScanSpecificationTc().graphFilter(),
              Eq(IndexScan::Graphs::Whitelist(graphs)));

  auto scanNoGraphs = IndexScan{qec, Permutation::PSO, triple};
  EXPECT_EQ(scanNoGraphs.graphsToFilter(), IndexScan::Graphs::All());
  EXPECT_THAT(scanNoGraphs.getCacheKey(), HasSubstr("GRAPHS: ALL"));
  EXPECT_THAT(scanNoGraphs.getScanSpecificationTc().graphFilter(),
              Eq(IndexScan::Graphs::All()));

  TripleComponent defaultGraph{iri(DEFAULT_GRAPH_IRI)};

  auto scanNamedGraphs = IndexScan{qec, Permutation::PSO, triple,
                                   IndexScan::Graphs::Blacklist(defaultGraph)};
  EXPECT_EQ(scanNamedGraphs.graphsToFilter(),
            IndexScan::Graphs::Blacklist(defaultGraph));
  EXPECT_THAT(scanNamedGraphs.getCacheKey(),
              HasSubstr("GRAPHS: Blacklist "
                        "<http://qlever.cs.uni-freiburg.de/builtin-functions/"
                        "default-graph>"));
  EXPECT_THAT(scanNamedGraphs.getScanSpecificationTc().graphFilter(),
              Eq(IndexScan::Graphs::Blacklist(defaultGraph)));
}

TEST(IndexScan, getResultSizeOfScan) {
  auto qec = getQec("<x> <p> <s1>, <s2>. <x> <p2> <s1>.");
  auto getId = makeGetId(qec->getIndex());
  [[maybe_unused]] auto x = getId("<x>");
  [[maybe_unused]] auto p = getId("<p>");
  [[maybe_unused]] auto s1 = getId("<s1>");
  [[maybe_unused]] auto s2 = getId("<s2>");
  [[maybe_unused]] auto p2 = getId("<p2>");
  using V = Variable;
  using I = TripleComponent::Iri;

  {
    SparqlTripleSimple scanTriple{V{"?x"}, V("?y"), V{"?z"}};
    IndexScan scan{qec, Permutation::Enum::PSO, scanTriple};
    EXPECT_EQ(scan.getSizeEstimate(), 3);
  }
  {
    SparqlTripleSimple scanTriple{V{"?x"}, I::fromIriref("<p>"), V{"?y"}};
    IndexScan scan{qec, Permutation::Enum::PSO, scanTriple};
    EXPECT_EQ(scan.getSizeEstimate(), 2);
  }
  {
    SparqlTripleSimple scanTriple{I::fromIriref("<x>"), I::fromIriref("<p>"),
                                  V{"?y"}};
    IndexScan scan{qec, Permutation::Enum::PSO, scanTriple};
    EXPECT_EQ(scan.getSizeEstimate(), 2);
  }
  {
    SparqlTripleSimple scanTriple{V("?x"), I::fromIriref("<p>"),
                                  I::fromIriref("<s1>")};
    IndexScan scan{qec, Permutation::Enum::POS, scanTriple};
    EXPECT_EQ(scan.getSizeEstimate(), 1);
  }
  // 0 variables
  {
    SparqlTripleSimple scanTriple{I::fromIriref("<x>"), I::fromIriref("<p>"),
                                  I::fromIriref("<s1>")};
    IndexScan scan{qec, Permutation::Enum::POS, scanTriple};
    EXPECT_EQ(scan.getSizeEstimate(), 1);
    EXPECT_ANY_THROW(scan.getMultiplicity(0));
    auto res = scan.computeResultOnlyForTesting();
    ASSERT_EQ(res.idTable().numRows(), 1);
    ASSERT_EQ(res.idTable().numColumns(), 0);
  }
  {
    SparqlTripleSimple scanTriple{I::fromIriref("<x2>"), I::fromIriref("<p>"),
                                  I::fromIriref("<s1>")};
    IndexScan scan{qec, Permutation::Enum::POS, scanTriple};
    EXPECT_EQ(scan.getSizeEstimate(), 1);
    EXPECT_EQ(scan.getExactSize(), 0);
  }
  {
    SparqlTripleSimple scanTriple{I::fromIriref("<x>"), I::fromIriref("<p>"),
                                  I::fromIriref("<p>")};
    IndexScan scan{qec, Permutation::Enum::POS, scanTriple};
    EXPECT_EQ(scan.getSizeEstimate(), 0);
    EXPECT_ANY_THROW(scan.getMultiplicity(0));
    auto res = scan.computeResultOnlyForTesting();
    ASSERT_EQ(res.idTable().numRows(), 0);
    ASSERT_EQ(res.idTable().numColumns(), 0);
  }
}

namespace {
// Return a `QueryExecutionContext` that is used in some of the tests below,
auto getQecWithoutPatterns = []() {
  TestIndexConfig config{"<x> <p> <s1>, <s2>. <x> <p2> <s1>."};
  config.usePatterns = false;
  return getQec(std::move(config));
};
}  // namespace
// _____________________________________________________________________________
TEST(IndexScan, computeResultCanBeConsumedLazily) {
  using V = Variable;
  auto qec = getQecWithoutPatterns();
  auto getId = makeGetId(qec->getIndex());
  auto x = getId("<x>");
  auto p = getId("<p>");
  auto s1 = getId("<s1>");
  auto s2 = getId("<s2>");
  auto p2 = getId("<p2>");
  SparqlTripleSimple scanTriple{V{"?x"}, V{"?y"}, V{"?z"}};
  IndexScan scan{qec, Permutation::Enum::POS, scanTriple};

  Result result = scan.computeResultOnlyForTesting(true);

  ASSERT_FALSE(result.isFullyMaterialized());

  IdTable resultTable{3, ad_utility::makeUnlimitedAllocator<Id>()};

  for (Result::IdTableVocabPair& pair : result.idTables()) {
    resultTable.insertAtEnd(pair.idTable_);
  }

  EXPECT_EQ(resultTable,
            makeIdTableFromVector({{p, s1, x}, {p, s2, x}, {p2, s1, x}}));
}

// _____________________________________________________________________________
TEST(IndexScan, computeResultReturnsEmptyGeneratorIfScanIsEmpty) {
  using V = Variable;
  using I = TripleComponent::Iri;
  auto qec = getQecWithoutPatterns();
  SparqlTripleSimple scanTriple{V{"?x"}, I::fromIriref("<abcdef>"), V{"?z"}};
  IndexScan scan{qec, Permutation::Enum::POS, scanTriple};

  Result result = scan.computeResultOnlyForTesting(true);

  ASSERT_FALSE(result.isFullyMaterialized());

  for ([[maybe_unused]] Result::IdTableVocabPair& pair : result.idTables()) {
    ADD_FAILURE() << "Generator should be empty" << std::endl;
  }
}

// _____________________________________________________________________________
TEST(IndexScan, unlikelyToFitInCacheCalculatesSizeCorrectly) {
  using ad_utility::MemorySize;
  using V = Variable;
  using I = TripleComponent::Iri;
  using enum Permutation::Enum;
  auto qec = getQecWithoutPatterns();
  auto x = I::fromIriref("<x>");
  auto p = I::fromIriref("<p>");
  auto p2 = I::fromIriref("<p2>");

  auto expectMaximumCacheableSize = [&](const IndexScan& scan, size_t numRows,
                                        size_t numCols,
                                        source_location l =
                                            AD_CURRENT_SOURCE_LOC()) {
    auto locationTrace = generateLocationTrace(l);

    EXPECT_TRUE(scan.unlikelyToFitInCache(MemorySize::bytes(0)));
    size_t byteCount = numRows * numCols * sizeof(Id);
    EXPECT_TRUE(scan.unlikelyToFitInCache(MemorySize::bytes(byteCount - 1)));
    EXPECT_FALSE(scan.unlikelyToFitInCache(MemorySize::bytes(byteCount)));
  };

  {
    IndexScan scan{qec, POS, {V{"?x"}, V{"?y"}, V{"?z"}}};
    expectMaximumCacheableSize(scan, 3, 3);
  }

  {
    IndexScan scan{qec, SPO, {x, V{"?y"}, V{"?z"}}};
    expectMaximumCacheableSize(scan, 3, 2);
  }

  {
    IndexScan scan{qec, POS, {V{"?x"}, p, V{"?z"}}};
    expectMaximumCacheableSize(scan, 2, 2);
  }

  {
    IndexScan scan{qec, SPO, {x, p2, V{"?z"}}};
    expectMaximumCacheableSize(scan, 1, 1);
  }
}

// _____________________________________________________________________________
TEST(IndexScan, getSizeEstimateAndExactSizeWithAppliedPrefilter) {
  using namespace makeFilterExpression;
  using namespace filterHelper;
  using I = TripleComponent::Iri;

  std::string kg =
      "<a> <price_tag> 10.00 . <b> <price_tag> 12.00 . <b> <price_tag> "
      "12.001 . <b> <price_tag> 21.99 . <b> <price_tag> 24.33 . <b> "
      "<price_tag> 147.32 . <b> <price_tag> 189.99 . <b> <price_tag> 194.67 "
      ".";
  auto qec = getQec(kg);

  auto assertEstimatedAndExactSize = [](IndexScan& indexScan,
                                        IndexScan::PrefilterVariablePair pair,
                                        const size_t estimateSize,
                                        const size_t exactSize) {
    auto optUpdatedQet = indexScan.setPrefilterGetUpdatedQueryExecutionTree(
        makePrefilterVec(std::move(pair)));
    ASSERT_TRUE(optUpdatedQet.has_value());
    auto updatedQet = optUpdatedQet.value();
    ASSERT_EQ(updatedQet->getSizeEstimate(), estimateSize);
    std::shared_ptr<IndexScan> scanPtr =
        std::dynamic_pointer_cast<IndexScan>(updatedQet->getRootOperation());
    ASSERT_EQ(scanPtr->getExactSize(), exactSize);
  };

  {
    SparqlTripleSimple triple{Tc{Variable{"?b"}}, iri("<price_tag>"),
                              Tc{Variable{"?price"}}};
    IndexScan scan{qec, Permutation::POS, triple};
    assertEstimatedAndExactSize(
        scan,
        pr(andExpr(gt(IntId(0)), lt(DoubleId(234.35))), Variable{"?price"}), 8,
        8);
    assertEstimatedAndExactSize(
        scan,
        pr(andExpr(gt(DoubleId(12.00)), lt(IntId(190))), Variable{"?price"}), 6,
        6);
    assertEstimatedAndExactSize(
        scan, pr(le(DoubleId(21.99)), Variable{"?price"}), 4, 4);
  }

  {
    SparqlTripleSimple triple{I::fromIriref("<b>"), iri("<price_tag>"),
                              Variable{"?price"}};
    IndexScan scan{qec, Permutation::PSO, triple};
    assertEstimatedAndExactSize(
        scan, pr(le(DoubleId(21.99)), Variable{"?price"}), 3, 3);
    assertEstimatedAndExactSize(
        scan, pr(eq(DoubleId(24.33)), Variable{"?price"}), 3, 3);
    assertEstimatedAndExactSize(
        scan, pr(orExpr(le(IntId(12)), gt(IntId(22))), Variable{"?price"}), 5,
        5);
  }
}

// _____________________________________________________________________________
TEST(IndexScan, verifyThatPrefilteredIndexScanResultIsNotCacheable) {
  using namespace makeFilterExpression;
  using namespace filterHelper;
  using V = Variable;
  auto qec = getQec("<x> <y> <z>.");
  SparqlTripleSimple triple{V{"?x"}, iri("<y>"), V{"?z"}};
  auto scan = IndexScan{qec, Permutation::PSO, triple};
  auto prefilterPairs =
      makePrefilterVec(pr(lt(IntId(10)), V{"?a"}), pr(gt(IntId(5)), V{"?b"}),
                       pr(lt(IntId(5)), V{"?x"}));
  auto qet =
      ad_utility::makeExecutionTree<IndexScan>(qec, Permutation::PSO, triple);
  EXPECT_TRUE(qet->getRootOperation()->canResultBeCached());
  auto updatedQet =
      qet->setPrefilterGetUpdatedQueryExecutionTree(std::move(prefilterPairs));
  // We have a corresponding column for ?x (at ColumnIndex 1), which is also the
  // first sorted variable column. Thus, we expect that the PrefilterExpression
  // (< 5, ?x) is applied for this `IndexScan`, resulting in prefiltered
  // `BlockMetadataRanges`. For `IndexScan`s containing prefiltered
  // `BlockMetadataRanges`, we expect that its result is not cacheable.
  EXPECT_TRUE(updatedQet.has_value());
  EXPECT_FALSE(updatedQet.value()->getRootOperation()->canResultBeCached());

  // Assert that we don't set a <PrefilterExpression, ColumnIndex> pair for the
  // second Variable.
  prefilterPairs = makePrefilterVec(pr(lt(IntId(10)), V{"?a"}),
                                    pr(gt(DoubleId(22)), V{"?z"}),
                                    pr(gt(IntId(10)), V{"?b"}));
  EXPECT_TRUE(qet->getRootOperation()->canResultBeCached());
  updatedQet =
      qet->setPrefilterGetUpdatedQueryExecutionTree(std::move(prefilterPairs));
  // No `PrefilterExpression` should be applied for this `IndexScan`, we don't
  // expect an updated QueryExecutionTree. The `IndexScan` should remain
  // unchanged, containing no prefiltered `BlockMetadataRanges`. Thus, it should
  // be still cacheable.
  EXPECT_TRUE(!updatedQet.has_value());
  EXPECT_TRUE(qet->getRootOperation()->canResultBeCached());
}

// _____________________________________________________________________________
TEST(IndexScan, checkEvaluationWithPrefiltering) {
  using namespace makeFilterExpression;
  using namespace filterHelper;
  auto I = ad_utility::testing::IntId;
  std::string kg =
      "<P1> <price_tag> 10 . <P2> <price_tag> 12 . <P3> <price_tag> "
      "18 . <P4> <price_tag> 22 . <P5> <price_tag> 25 . <P6> "
      "<price_tag> 147 . <P7> <price_tag> 174 . <P8> <price_tag> 174 "
      ". <P9> <price_tag> 189 . <P10> <price_tag> 194 .";
  SparqlTripleSimple triple{Tc{Variable{"?x"}}, iri("<price_tag>"),
                            Tc{Variable{"?price"}}};

  // For the following tests, the <PrefilterExpression, Variable> pair is set
  // and applied for the respective IndexScan.
  testSetAndMakeScanWithPrefilterExpr(kg, triple, Permutation::POS,
                                      pr(ge(IntId(10)), Variable{"?price"}),
                                      {I(10), I(12), I(18), I(22), I(25),
                                       I(147), I(174), I(174), I(189), I(194)},
                                      true);
  testSetAndMakeScanWithPrefilterExpr(
      kg, triple, Permutation::POS,
      pr(lt(DoubleId(147.32)), Variable{"?price"}),
      {I(10), I(12), I(18), I(22), I(25), I(147)}, true);
  testSetAndMakeScanWithPrefilterExpr(
      kg, triple, Permutation::POS,
      pr(andExpr(gt(DoubleId(12.00)), le(IntId(174))), Variable{"?price"}),
      {I(18), I(22), I(25), I(147), I(174), I(174)}, true);
  testSetAndMakeScanWithPrefilterExpr(
      kg, triple, Permutation::POS,
      pr(andExpr(gt(DoubleId(12.00)), le(IntId(174))), Variable{"?price"}),
      {I(18), I(22), I(25), I(147)}, true,
      pr(lt(IntId(174)), Variable{"?price"}));
  testSetAndMakeScanWithPrefilterExpr(
      kg, triple, Permutation::POS,
      pr(andExpr(gt(DoubleId(12.00)), lt(IntId(174))), Variable{"?price"}), {},
      true, pr(eq(IntId(174)), Variable{"?price"}));
  testSetAndMakeScanWithPrefilterExpr(
      kg, triple, Permutation::POS,
      pr(andExpr(gt(DoubleId(12.00)), le(IntId(174))), Variable{"?price"}),
      {I(174), I(174)}, true, pr(eq(IntId(174)), Variable{"?price"}));
  testSetAndMakeScanWithPrefilterExpr(
      kg, triple, Permutation::POS,
      pr(andExpr(gt(DoubleId(12.00)), le(IntId(174))), Variable{"?price"}),
      {I(25), I(147)}, true,
      pr(andExpr(gt(IntId(22)), lt(IntId(174))), Variable{"?price"}));
  // The second `PrefilterExpression` is not applicable, since its variable
  // `?some_var` doesn't match the sorted `IndexScan` columns.
  testSetAndMakeScanWithPrefilterExpr(
      kg, triple, Permutation::POS,
      pr(andExpr(gt(DoubleId(12.00)), le(IntId(174))), Variable{"?price"}),
      {I(18), I(22), I(25), I(147), I(174), I(174)}, true,
      pr(andExpr(gt(IntId(22)), lt(IntId(174))), Variable{"?some_var"}));

  // For the following test, the Variable value doesn't match any of the scan
  // triple Variable values. We expect that the prefilter is not applicable (=>
  // set bool flag to false).
  testSetAndMakeScanWithPrefilterExpr(
      kg, triple, Permutation::POS,
      pr(andExpr(gt(DoubleId(12.00)), le(IntId(174))), Variable{"?y"}), {},
      false);

  // For the following tests, the first sorted column given the permutation
  // doesn't match with the corresponding column for the Variable of the
  // <PrefilterExpression, Variable> pair. We expect that the provided prefilter
  // is not applicable (and can't be set).
  testSetAndMakeScanWithPrefilterExpr(
      kg, triple, Permutation::PSO,
      pr(andExpr(gt(DoubleId(12.00)), le(IntId(174))), Variable{"?price"}), {},
      false);
  testSetAndMakeScanWithPrefilterExpr(
      kg, triple, Permutation::POS,
      pr(andExpr(gt(VocabId(0)), lt(VocabId(100))), Variable{"?x"}), {}, false);

  // This knowledge graph yields an incomplete first and last block.
  std::string kgFirstAndLastIncomplete =
      "<a> <price_tag> 10 . <b> <price_tag> 12 . <b> <price_tag> "
      "18 . <b> <price_tag> 22 . <b> <price_tag> 25 . <b> "
      "<price_tag> 147 . <b> <price_tag> 189 . <c> <price_tag> 194 "
      ".";
  // The following test verifies that the prefilter procedure is successfully
  // applicable under the condition that the first and last block are
  // potentially incomplete.
  testSetAndMakeScanWithPrefilterExpr(
      kgFirstAndLastIncomplete, triple, Permutation::POS,
      pr(orExpr(gt(IntId(100)), le(IntId(10))), Variable{"?price"}),
      {I(10), I(12), I(25), I(147), I(189), I(194)}, true);
  testSetAndMakeScanWithPrefilterExpr(
      kgFirstAndLastIncomplete, triple, Permutation::POS,
      pr(andExpr(gt(IntId(10)), lt(IntId(194))), Variable{"?price"}),
      {I(10), I(12), I(18), I(22), I(25), I(147), I(189), I(194)}, true);
}

class IndexScanWithLazyJoin : public ::testing::TestWithParam<bool> {
 protected:
  QueryExecutionContext* qec_ = nullptr;

  void SetUp() override {
    std::string kg =
        "<a> <p> <A> . <a> <p> <A2> . "
        "<b> <p> <B> . <b> <p> <B2> . "
        "<c> <p> <C> . <c> <p> <C2> . "
        "<b> <q> <xb> . <b> <q> <xb2> .";
    qec_ = getQec(std::move(kg));
  }

  // Convert a TripleComponent to a ValueId.
  Id toValueId(const TripleComponent& tc) const {
    return tc.toValueId(qec_->getIndex().getVocab(), *encodedIriManager())
        .value();
  }

  // Create an id table with a single column from a vector of
  // `TripleComponent`s.
  IdTable makeIdTable(std::vector<TripleComponent> entries) const {
    IdTable result{1, makeAllocator()};
    result.reserve(entries.size());
    for (const TripleComponent& entry : entries) {
      result.emplace_back();
      result.back()[0] = toValueId(entry);
    }
    return result;
  }

  // Convert a vector of a tuple of triples to an id table.
  IdTable tableFromTriples(
      std::vector<std::array<TripleComponent, 2>> triples) const {
    IdTable result{2, makeAllocator()};
    result.reserve(triples.size());
    for (const auto& triple : triples) {
      result.emplace_back();
      result.back()[0] = toValueId(triple.at(0));
      result.back()[1] = toValueId(triple.at(1));
    }
    return result;
  }

  // Create a common `IndexScan` instance.
  IndexScan makeScan() const {
    SparqlTripleSimple xpy{Tc{Var{"?x"}}, iri("<p>"), Tc{Var{"?y"}}};
    // We need to scan all the blocks that contain the `<p>` predicate.
    return IndexScan{qec_, Permutation::PSO, xpy};
  }

  // Consume range `first` first and store it in a vector, then do the same
  // with `second`.
  static std::pair<std::vector<Result::IdTableVocabPair>,
                   std::vector<Result::IdTableVocabPair>>
  consumeSequentially(Result::LazyResult first, Result::LazyResult second,
                      auto postCondition) {
    std::vector<Result::IdTableVocabPair> firstResult;
    std::vector<Result::IdTableVocabPair> secondResult;

    for (Result::IdTableVocabPair& element : first) {
      firstResult.push_back(std::move(element));
      std::invoke(postCondition);
    }
    for (Result::IdTableVocabPair& element : second) {
      secondResult.push_back(std::move(element));
      std::invoke(postCondition);
    }
    return {std::move(firstResult), std::move(secondResult)};
  }

  // Consume the ranges and store the results in vectors using the
  // parameterized strategy.
  template <typename T = ad_utility::Noop>
  static std::pair<std::vector<Result::IdTableVocabPair>,
                   std::vector<Result::IdTableVocabPair>>
  consumeRanges(std::pair<Result::LazyResult, Result::LazyResult> rangePair,
                T postCondition = ad_utility::noop) {
    std::vector<Result::IdTableVocabPair> joinSideResults;
    std::vector<Result::IdTableVocabPair> scanResults;

    bool rightFirst = GetParam();
    if (rightFirst) {
      std::tie(scanResults, joinSideResults) = consumeSequentially(
          std::move(rangePair.second), std::move(rangePair.first),
          std::move(postCondition));
    } else {
      std::tie(joinSideResults, scanResults) = consumeSequentially(
          std::move(rangePair.first), std::move(rangePair.second),
          std::move(postCondition));
    }
    return {std::move(joinSideResults), std::move(scanResults)};
  }
};

// _____________________________________________________________________________
TEST_P(IndexScanWithLazyJoin, prefilterTablesDoesFilterCorrectly) {
  IndexScan scan = makeScan();

  auto makeJoinSide = [](auto* self) -> Result::Generator {
    using P = Result::IdTableVocabPair;
    P p1{self->makeIdTable({iri("<a>"), iri("<c>")}), LocalVocab{}};
    co_yield p1;
    P p2{self->makeIdTable({iri("<c>")}), LocalVocab{}};
    co_yield p2;
    LocalVocab vocab;
    vocab.getIndexAndAddIfNotContained(LocalVocabEntry{
        ad_utility::triple_component::Literal::literalWithoutQuotes("Test")});
    P p3{self->makeIdTable({iri("<c>"), iri("<q>"), iri("<xb>")}),
         std::move(vocab)};
    co_yield p3;
  };

  auto [joinSideResults, scanResults] = consumeRanges(
      scan.prefilterTables(LazyResult{makeJoinSide(this)}, 0),
      [&scan, counter = 0]() mutable {
        bool rightFirst = GetParam();
        // If the left side is being consumed first, then we observe the effects
        // 3 calls later. This is an implementation detail and might change in
        // the future.
        int sideOffset = rightFirst ? 0 : 3;
        const auto& rti = scan.runtimeInfo();
        if (counter >= sideOffset) {
          EXPECT_EQ(rti.status_,
                    RuntimeInformation::Status::lazilyMaterializedInProgress);
        }
        if (counter >= 2 + sideOffset) {
          EXPECT_GT(rti.numRows_, 0);
        }
        counter++;
      });

  ASSERT_EQ(scanResults.size(), 2);
  ASSERT_EQ(joinSideResults.size(), 3);

  EXPECT_TRUE(scanResults.at(0).localVocab_.empty());
  EXPECT_TRUE(joinSideResults.at(0).localVocab_.empty());

  EXPECT_TRUE(scanResults.at(1).localVocab_.empty());
  EXPECT_TRUE(joinSideResults.at(1).localVocab_.empty());

  EXPECT_FALSE(joinSideResults.at(2).localVocab_.empty());

  EXPECT_EQ(
      scanResults.at(0).idTable_,
      tableFromTriples({{iri("<a>"), iri("<A>")}, {iri("<a>"), iri("<A2>")}}));
  EXPECT_EQ(joinSideResults.at(0).idTable_,
            makeIdTable({iri("<a>"), iri("<c>")}));

  EXPECT_EQ(
      scanResults.at(1).idTable_,
      tableFromTriples({{iri("<c>"), iri("<C>")}, {iri("<c>"), iri("<C2>")}}));
  EXPECT_EQ(joinSideResults.at(1).idTable_, makeIdTable({iri("<c>")}));

  EXPECT_EQ(joinSideResults.at(2).idTable_,
            makeIdTable({iri("<c>"), iri("<q>"), iri("<xb>")}));

  const auto& rti = scan.runtimeInfo();
  EXPECT_EQ(rti.status_,
            RuntimeInformation::Status::lazilyMaterializedInProgress);
  EXPECT_EQ(rti.numRows_, 4);
  // Note: In the code the `totalTime_` is also set, but the actual code runs
  // faster than a single millisecond, so it won't show up in the data.
  EXPECT_EQ(rti.details_["num-blocks-read"], 2);
  EXPECT_EQ(rti.details_["num-blocks-all"], 3);
  EXPECT_EQ(rti.details_["num-elements-read"], 4);
}

// _____________________________________________________________________________
TEST_P(IndexScanWithLazyJoin,
       prefilterTablesDoesFilterCorrectlyWithOverlappingValues) {
  std::string kg = "<a> <p> <A> . <b> <p> <B>. ";
  qec_ = getQec(std::move(kg));
  IndexScan scan = makeScan();

  auto makeJoinSide = [](auto* self) -> Result::Generator {
    using P = Result::IdTableVocabPair;
    P p1{self->makeIdTable({iri("<a>")}), LocalVocab{}};
    co_yield p1;
    P p2{self->makeIdTable({iri("<b>")}), LocalVocab{}};
    co_yield p2;
  };

  auto [joinSideResults, scanResults] =
      consumeRanges(scan.prefilterTables(LazyResult{makeJoinSide(this)}, 0));

  ASSERT_EQ(scanResults.size(), 1);
  ASSERT_EQ(joinSideResults.size(), 2);

  EXPECT_TRUE(scanResults.at(0).localVocab_.empty());
  EXPECT_TRUE(joinSideResults.at(0).localVocab_.empty());
  EXPECT_TRUE(joinSideResults.at(1).localVocab_.empty());

  EXPECT_EQ(
      scanResults.at(0).idTable_,
      tableFromTriples({{iri("<a>"), iri("<A>")}, {iri("<b>"), iri("<B>")}}));
  EXPECT_EQ(joinSideResults.at(0).idTable_, makeIdTable({iri("<a>")}));

  EXPECT_EQ(joinSideResults.at(1).idTable_, makeIdTable({iri("<b>")}));
}

// _____________________________________________________________________________
TEST_P(IndexScanWithLazyJoin,
       prefilterTablesDoesFilterCorrectlyWithSkipTablesWithoutMatchingBlock) {
  std::string kg = "<a> <p> <A> . <b> <p> <B>. ";
  qec_ = getQec(std::move(kg));
  IndexScan scan = makeScan();

  auto makeJoinSide = []() -> Result::Generator {
    using P = Result::IdTableVocabPair;
    P p1{makeIdTableFromVector({{Id::makeFromBool(true)}}), LocalVocab{}};
    co_yield p1;
    P p2{makeIdTableFromVector({{Id::makeFromBool(true)}}), LocalVocab{}};
    co_yield p2;
  };

  auto [joinSideResults, scanResults] =
      consumeRanges(scan.prefilterTables(LazyResult{makeJoinSide()}, 0));

  ASSERT_EQ(scanResults.size(), 0);
  ASSERT_EQ(joinSideResults.size(), 0);

  const auto& rti = scan.runtimeInfo();
  EXPECT_EQ(rti.status_,
            RuntimeInformation::Status::lazilyMaterializedInProgress);
  EXPECT_EQ(rti.numRows_, 0);
  EXPECT_EQ(rti.details_["num-blocks-read"], 0);
  EXPECT_EQ(rti.details_["num-blocks-all"], 1);
  EXPECT_EQ(rti.details_["num-elements-read"], 0);
}

// _____________________________________________________________________________
TEST_P(IndexScanWithLazyJoin, prefilterTablesDoesNotSkipOnRepeatingBlock) {
  TestIndexConfig config;
  // a and b are supposed to share one block and c and d.
  config.turtleInput =
      "<a> <p> <A> . <b> <p> <B> . <c> <p> <C> . <d> <p> <D> . ";
  config.blocksizePermutations = 16_B;
  qec_ = getQec(std::move(config));
  IndexScan scan = makeScan();

  // This is a regression test for an issue introduced in
  // https://github.com/ad-freiburg/qlever/pull/2252
  using P = Result::IdTableVocabPair;
  std::array pairs{P{makeIdTable({iri("<a>")}), LocalVocab{}},
                   P{makeIdTable({iri("<b>")}), LocalVocab{}},
                   P{makeIdTable({iri("<c>")}), LocalVocab{}}};

  auto [joinSideResults, scanResults] =
      consumeRanges(scan.prefilterTables(LazyResult{std::move(pairs)}, 0));

  ASSERT_EQ(scanResults.size(), 2);
  ASSERT_EQ(joinSideResults.size(), 3);
}

// _____________________________________________________________________________
TEST_P(IndexScanWithLazyJoin,
       prefilterTablesSkipsRemainingTablesIfIndexIsExhausted) {
  TestIndexConfig config;
  // a and b are supposed to share one block and c and d.
  config.turtleInput =
      "<a> <p> <A> . <b> <p> <B> . <c> <p> <C> . <d> <p> <D> . ";
  config.blocksizePermutations = 16_B;
  qec_ = getQec(std::move(config));
  IndexScan scan = makeScan();
  LocalVocab extraVocab;
  auto indexE =
      extraVocab.getIndexAndAddIfNotContained(LocalVocabEntry{iri("<e>")});
  auto indexG =
      extraVocab.getIndexAndAddIfNotContained(LocalVocabEntry{iri("<g>")});

  using P = Result::IdTableVocabPair;
  std::array pairs{
      P{makeIdTable({iri("<a>")}), LocalVocab{}},
      P{makeIdTable({iri("<c>")}), LocalVocab{}},
      P{makeIdTable({Id::makeFromLocalVocabIndex(indexE)}), extraVocab.clone()},
      P{makeIdTable({Id::makeFromLocalVocabIndex(indexG)}),
        extraVocab.clone()}};

  size_t counter = 0;

  auto [joinSideResults, scanResults] = consumeRanges(
      scan.prefilterTables(LazyResult{ad_utility::CachingTransformInputRange{
                               std::move(pairs),
                               [&counter, &indexG](auto& pair) mutable {
                                 counter++;
                                 // No need to consume the last table.
                                 EXPECT_NE(pair.idTable_.at(0, 0),
                                           Id::makeFromLocalVocabIndex(indexG));
                                 return std::move(pair);
                               }}},
                           0));

  EXPECT_EQ(counter, 3) << "Exactly 3 elements should be consumed from the "
                           "range. No need to evaluate the rest when it is "
                           "known that any future elements won't match.";
  ASSERT_EQ(scanResults.size(), 2);
  ASSERT_EQ(joinSideResults.size(), 2);
}

// _____________________________________________________________________________
TEST_P(IndexScanWithLazyJoin, prefilterTablesDoesNotFilterOnUndefined) {
  IndexScan scan = makeScan();

  auto makeJoinSide = [](auto* self) -> Result::Generator {
    using P = Result::IdTableVocabPair;
    P p1{IdTable{1, makeAllocator()}, LocalVocab{}};
    co_yield p1;
    P p2{makeIdTableFromVector({{Id::makeUndefined()}}), LocalVocab{}};
    co_yield p2;
    P p3{makeIdTableFromVector({{Id::makeUndefined()}}), LocalVocab{}};
    co_yield p3;
    P p4{IdTable{1, makeAllocator()}, LocalVocab{}};
    co_yield p4;
    P p5{self->makeIdTable({iri("<a>"), iri("<c>")}), LocalVocab{}};
    co_yield p5;
    P p6{self->makeIdTable({iri("<c>"), iri("<q>"), iri("<xb>")}),
         LocalVocab{}};
    co_yield p6;
    P p7{IdTable{1, makeAllocator()}, LocalVocab{}};
    co_yield p7;
  };

  auto [_, scanResults] =
      consumeRanges(scan.prefilterTables(LazyResult{makeJoinSide(this)}, 0));

  ASSERT_EQ(scanResults.size(), 3);
  EXPECT_TRUE(scanResults.at(0).localVocab_.empty());
  EXPECT_TRUE(scanResults.at(1).localVocab_.empty());
  EXPECT_TRUE(scanResults.at(2).localVocab_.empty());

  EXPECT_EQ(
      scanResults.at(0).idTable_,
      tableFromTriples({{iri("<a>"), iri("<A>")}, {iri("<a>"), iri("<A2>")}}));

  EXPECT_EQ(
      scanResults.at(1).idTable_,
      tableFromTriples({{iri("<b>"), iri("<B>")}, {iri("<b>"), iri("<B2>")}}));

  EXPECT_EQ(
      scanResults.at(2).idTable_,
      tableFromTriples({{iri("<c>"), iri("<C>")}, {iri("<c>"), iri("<C2>")}}));

  const auto& rti = scan.runtimeInfo();
  EXPECT_EQ(rti.status_,
            RuntimeInformation::Status::lazilyMaterializedInProgress);
  EXPECT_EQ(rti.numRows_, 6);
  EXPECT_EQ(rti.details_["num-blocks-read"], 3);
  EXPECT_EQ(rti.details_["num-blocks-all"], 3);
  EXPECT_EQ(rti.details_["num-elements-read"], 6);
}

// _____________________________________________________________________________
TEST_P(IndexScanWithLazyJoin, prefilterTablesDoesNotFilterWithSingleUndefined) {
  IndexScan scan = makeScan();

  auto makeJoinSide = []() -> Result::Generator {
    using P = Result::IdTableVocabPair;
    P p1{makeIdTableFromVector({{Id::makeUndefined()}}), LocalVocab{}};
    co_yield p1;
  };

  auto [_, scanResults] =
      consumeRanges(scan.prefilterTables(LazyResult{makeJoinSide()}, 0));

  ASSERT_EQ(scanResults.size(), 3);
  EXPECT_TRUE(scanResults.at(0).localVocab_.empty());
  EXPECT_TRUE(scanResults.at(1).localVocab_.empty());
  EXPECT_TRUE(scanResults.at(2).localVocab_.empty());

  EXPECT_EQ(
      scanResults.at(0).idTable_,
      tableFromTriples({{iri("<a>"), iri("<A>")}, {iri("<a>"), iri("<A2>")}}));

  EXPECT_EQ(
      scanResults.at(1).idTable_,
      tableFromTriples({{iri("<b>"), iri("<B>")}, {iri("<b>"), iri("<B2>")}}));

  EXPECT_EQ(
      scanResults.at(2).idTable_,
      tableFromTriples({{iri("<c>"), iri("<C>")}, {iri("<c>"), iri("<C2>")}}));
}

// _____________________________________________________________________________
TEST_P(IndexScanWithLazyJoin, prefilterTablesWorksWithSingleEmptyTable) {
  IndexScan scan = makeScan();

  auto makeJoinSide = []() -> Result::Generator {
    using P = Result::IdTableVocabPair;
    P p{IdTable{1, makeAllocator()}, LocalVocab{}};
    co_yield p;
  };

  auto [_, scanResults] =
      consumeRanges(scan.prefilterTables(LazyResult{makeJoinSide()}, 0));

  ASSERT_EQ(scanResults.size(), 0);

  const auto& rti = scan.runtimeInfo();
  EXPECT_EQ(rti.status_,
            RuntimeInformation::Status::lazilyMaterializedInProgress);
  EXPECT_EQ(rti.numRows_, 0);
  EXPECT_EQ(rti.details_["num-blocks-read"], 0);
  EXPECT_EQ(rti.details_["num-blocks-all"], 3);
  EXPECT_EQ(rti.details_["num-elements-read"], 0);
}

// _____________________________________________________________________________
TEST_P(IndexScanWithLazyJoin, prefilterTablesWorksWithEmptyGenerator) {
  IndexScan scan = makeScan();

  auto makeJoinSide = []() -> Result::Generator { co_return; };

  auto [_, scanResults] =
      consumeRanges(scan.prefilterTables(LazyResult{makeJoinSide()}, 0));

  ASSERT_EQ(scanResults.size(), 0);
}

INSTANTIATE_TEST_SUITE_P(IndexScanWithLazyJoinSuite, IndexScanWithLazyJoin,
                         ::testing::Bool(),
                         [](const testing::TestParamInfo<bool>& info) {
                           return info.param ? "RightSideFirst"
                                             : "LeftSideFirst";
                         });

// _____________________________________________________________________________
TEST(IndexScan, prefilterTablesWithEmptyIndexScanReturnsEmptyGenerators) {
  auto* qec = getQec("<a> <p> <A>");
  // Match with something that does not match.
  SparqlTripleSimple xpy{Tc{Var{"?x"}}, iri("<not_p>"), Tc{Var{"?y"}}};
  IndexScan scan{qec, Permutation::PSO, xpy};

  auto makeJoinSide = []() -> Result::Generator {
    ADD_FAILURE()
        << "The generator should not be consumed when the IndexScan is empty."
        << std::endl;
    co_return;
  };

  auto [leftGenerator, rightGenerator] =
      scan.prefilterTables(Result::LazyResult{makeJoinSide()}, 0);

  EXPECT_EQ(leftGenerator.begin(), leftGenerator.end());
  EXPECT_EQ(rightGenerator.begin(), rightGenerator.end());
}
// _____________________________________________________________________________
TEST(IndexScan, clone) {
  auto qec = getQec("<x> <price_tag> 10.");
  {
    SparqlTripleSimple xpy{Tc{Var{"?x"}}, iri("<not_p>"), Tc{Var{"?y"}}};
    IndexScan scan{qec, Permutation::PSO, xpy};

    auto clone = scan.clone();
    ASSERT_TRUE(clone);
    const auto& cloneReference = *clone;
    EXPECT_EQ(typeid(scan), typeid(cloneReference));
    EXPECT_EQ(cloneReference.getDescriptor(), scan.getDescriptor());
  }
  {
    using namespace makeFilterExpression;
    using namespace filterHelper;
    SparqlTripleSimple triple{Tc{Variable{"?x"}}, iri("<price_tag>"),
                              Tc{Variable{"?price"}}};
    auto qet =
        ad_utility::makeExecutionTree<IndexScan>(qec, Permutation::POS, triple);
    ASSERT_TRUE(qet->getRootOperation()->canResultBeCached());
    auto clone = qet->clone();
    ASSERT_TRUE(clone);
    const auto& cloneReference = *clone;
    ASSERT_TRUE(cloneReference.getRootOperation()->canResultBeCached());
    auto prefilterPairs =
        makePrefilterVec(pr(eq(IntId(10)), Variable{"?price"}));
    auto optUpdatedQet = qet->setPrefilterGetUpdatedQueryExecutionTree(
        std::move(prefilterPairs));
    ASSERT_TRUE(optUpdatedQet.has_value());
    const auto& updatedQet = optUpdatedQet.value();
    ASSERT_FALSE(updatedQet->getRootOperation()->canResultBeCached());
    auto clonedQet = updatedQet->clone();
    ASSERT_TRUE(clonedQet);
    const auto& clonedQetReference = *clonedQet;
    ASSERT_EQ(typeid(clonedQetReference), typeid(*updatedQet));
    // If the `ScanSpecAndBlocks` of the prefiltered `IndexScan` operation is
    // not copied properly while cloning, the following test fails. This is
    // because the `ScanSpecAndBlocks` added by default via its constructor is
    // not prefiltered, and subsequently would allow us to cache the result.
    ASSERT_FALSE(clonedQetReference.getRootOperation()->canResultBeCached());
  }
}

// _____________________________________________________________________________
TEST(IndexScan, columnOriginatesFromGraphOrUndef) {
  auto* qec = getQec();
  IndexScan scan1{qec, Permutation::PSO,
                  SparqlTripleSimple{Var{"?x"}, Var{"?y"}, Var{"?z"}}};
  EXPECT_TRUE(scan1.columnOriginatesFromGraphOrUndef(Var{"?x"}));
  EXPECT_TRUE(scan1.columnOriginatesFromGraphOrUndef(Var{"?y"}));
  EXPECT_TRUE(scan1.columnOriginatesFromGraphOrUndef(Var{"?z"}));
  EXPECT_THROW(scan1.columnOriginatesFromGraphOrUndef(Var{"?notExisting"}),
               ad_utility::Exception);

  IndexScan scan2{
      qec, Permutation::PSO,
      SparqlTripleSimple{
          Var{"?x"}, Var{"?y"}, Var{"?z"}, {std::pair{3, Var{"?g"}}}}};
  EXPECT_TRUE(scan2.columnOriginatesFromGraphOrUndef(Var{"?x"}));
  EXPECT_TRUE(scan2.columnOriginatesFromGraphOrUndef(Var{"?y"}));
  EXPECT_TRUE(scan2.columnOriginatesFromGraphOrUndef(Var{"?z"}));
  EXPECT_FALSE(scan2.columnOriginatesFromGraphOrUndef(Var{"?g"}));
  EXPECT_THROW(scan2.columnOriginatesFromGraphOrUndef(Var{"?notExisting"}),
               ad_utility::Exception);

  IndexScan scan3{qec, Permutation::OSP,
                  SparqlTripleSimple{iri("<a>"), Var{"?y"}, iri("<c>")}};
  EXPECT_THROW(scan3.columnOriginatesFromGraphOrUndef(Var{"?x"}),
               ad_utility::Exception);
  EXPECT_TRUE(scan3.columnOriginatesFromGraphOrUndef(Var{"?y"}));
  EXPECT_THROW(scan3.columnOriginatesFromGraphOrUndef(Var{"?z"}),
               ad_utility::Exception);
}

namespace {
// For each of the `varsToKeep` return a pair of `[variable, columnIndex]` where
// the column indices are obtained from the `underlyingScan`. The result is
// sorted by the column indices.
constexpr auto getVarsAndColumnIndices = [](const auto& varsToKeep,
                                            const IndexScan& underlyingScan) {
  std::vector<std::pair<Variable, ColumnIndex>> varColPairs;
  for (const auto& var : varsToKeep) {
    auto colIdxInBaseScan = underlyingScan.getExternallyVisibleVariableColumns()
                                .at(var)
                                .columnIndex_;
    varColPairs.emplace_back(var, colIdxInBaseScan);
  }
  ql::ranges::sort(varColPairs, {}, ad_utility::second);
  return varColPairs;
};
}  // namespace

// Tests for the stripping of columns directly inside the `IndexScan` class.
TEST(IndexScanTest, StripColumns) {
  // Generic setup
  TestIndexConfig config;
  using namespace ad_utility::memory_literals;
  // Each triple will be in a separate block.
  config.blocksizePermutations = 8_B;
  config.turtleInput = "<s> <p> <o>. <s2> <p> <o>. <s2> <p2> <o2>";
  auto qec = ad_utility::testing::getQec(config);

  auto def = makeAlwaysDefinedColumn;

  // We create various scans that all should have unique cache keys, this hash
  // set helps us test that invariant.
  ad_utility::HashSet<std::string> cacheKeys;

  // Generic lambda to test stripping with any number of columns for any
  // IndexScan. Parameters:
  // `baseScan` : an arbitrary `IndexScan`.
  // `baseScanDifferentVars` : the same index scan, but the first variable must
  // be the same, and all other variables must be different. This tests the
  // block prefiltering for joins, which currently only works on a single
  // variable.
  auto testStrippedColumns = [&](IndexScan& baseScan,
                                 IndexScan& baseScanDifferentVars,
                                 const std::vector<Variable>& varsToKeep,
                                 const std::vector<ColumnIndex>& sortedOn,
                                 ad_utility::source_location l =
                                     AD_CURRENT_SOURCE_LOC()) {
    auto trace = generateLocationTrace(l);
    IdTable baseResult =
        baseScan.computeResultOnlyForTesting(false).idTable().clone();
    qec->clearCacheUnpinnedOnly();
    // Create set with variables to keep, plus non-existent variables to test
    // filtering
    std::set<Variable> varsWithNonExistent(varsToKeep.begin(),
                                           varsToKeep.end());
    varsWithNonExistent.insert(Var{"?notFound"});

    auto subsetScan =
        baseScan.makeTreeWithStrippedColumns(varsWithNonExistent).value();
    EXPECT_EQ(subsetScan->getResultWidth(), varsToKeep.size());

    auto [_, wasNew] = cacheKeys.insert(subsetScan->getCacheKey());
    EXPECT_TRUE(wasNew);

    using namespace ::testing;

    // Create pairs of (variable, original_column_index) and sort by original
    // index
    std::vector<std::pair<Variable, ColumnIndex>> columnOrigins =
        getVarsAndColumnIndices(varsToKeep, baseScan);
    // Build expected variable to column mapping with dense, ordered column
    // indices
    VariableToColumnMap expected;
    std::vector<ColumnIndex> originalColumnIndices;
    for (size_t i = 0; i < columnOrigins.size(); ++i) {
      expected[columnOrigins[i].first] = def(i);
      originalColumnIndices.push_back(columnOrigins[i].second);
    }

    EXPECT_THAT(subsetScan->getVariableColumns(),
                UnorderedElementsAreArray(expected));

    // Test multiplicity preservation
    for (const auto& [v, colIdxAndInfo] : subsetScan->getVariableColumns()) {
      auto colIdxInBaseScan =
          baseScan.getExternallyVisibleVariableColumns().at(v).columnIndex_;
      EXPECT_FLOAT_EQ(subsetScan->getMultiplicity(colIdxAndInfo.columnIndex_),
                      baseScan.getMultiplicity(colIdxInBaseScan));
    }

    // Create expected result by selecting the right columns
    auto expectedResult =
        baseResult.asColumnSubsetView(originalColumnIndices).clone();

    // Test fully materialized evaluation.
    EXPECT_THAT(subsetScan->resultSortedOn(), ElementsAreArray(sortedOn));
    EXPECT_THAT(subsetScan->getResult(false)->idTable(),
                matchesIdTable(expectedResult.clone()));

    // Test lazy evaluation.
    qec->clearCacheUnpinnedOnly();
    auto res = subsetScan->getResult(true);
    auto lazyResToTable = [&subsetScan, &qec](auto&& gen) {
      IdTable resultTable(subsetScan->getResultWidth(), qec->getAllocator());
      for (auto&& table : gen) {
        resultTable.insertAtEnd(table);
      }
      return resultTable;
    };
    EXPECT_THAT(lazyResToTable(
                    ad_utility::OwningView{res->idTables()} |
                    ql::views::transform(&Result::IdTableVocabPair::idTable_)),
                matchesIdTable(expectedResult.clone()));

    // Test lazy scan functionality only for single-column results that are
    // sorted.
    // Note: We currently test the join between an index scan and itself. This
    // makes the testing easier, because then the block prefiltering actually
    // doesn't filter out any blocks. We are not interested in the block
    // prefiltering here (which is tested further above), but only in the
    // stripping of the columns.
    if (varsToKeep.size() == 1 && !sortedOn.empty()) {
      const auto& scanOp1 =
          dynamic_cast<const IndexScan&>(*subsetScan->getRootOperation());
      auto subsetScan2 =
          baseScanDifferentVars.makeTreeWithStrippedColumns({varsToKeep[0]})
              .value();
      const auto& scanOp2 =
          dynamic_cast<const IndexScan&>(*subsetScan2->getRootOperation());
      auto [s1, s2] = IndexScan::lazyScanForJoinOfTwoScans(scanOp1, scanOp2);
      EXPECT_THAT(lazyResToTable(s1), matchesIdTable(expectedResult.clone()));
      EXPECT_THAT(lazyResToTable(s2), matchesIdTable(expectedResult.clone()));

      auto s3 =
          scanOp1.lazyScanForJoinOfColumnWithScan(expectedResult.getColumn(0));
      EXPECT_THAT(lazyResToTable(s3), matchesIdTable(expectedResult.clone()));
    }

    // Test functions whose results don't depend on column stripping
    // These should return the same values for both base and stripped scan
    auto& strippedScanOp =
        dynamic_cast<IndexScan&>(*subsetScan->getRootOperation());

    // Test accessor functions
    EXPECT_EQ(strippedScanOp.getDescriptor(), baseScan.getDescriptor());
    EXPECT_EQ(strippedScanOp.subject().toString(),
              baseScan.subject().toString());
    EXPECT_EQ(strippedScanOp.predicate().toString(),
              baseScan.predicate().toString());
    EXPECT_EQ(strippedScanOp.object().toString(), baseScan.object().toString());
    EXPECT_EQ(strippedScanOp.graphsToFilter(), baseScan.graphsToFilter());
    EXPECT_THAT(strippedScanOp.additionalVariables(),
                ElementsAreArray(baseScan.additionalVariables()));
    EXPECT_THAT(strippedScanOp.additionalColumns(),
                ElementsAreArray(baseScan.additionalColumns()));
    EXPECT_EQ(strippedScanOp.numVariables(), baseScan.numVariables());
    EXPECT_EQ(strippedScanOp.permutation().permutation(),
              baseScan.permutation().permutation());
    EXPECT_EQ(strippedScanOp.permutation().onDiskBase(),
              baseScan.permutation().onDiskBase());

    // Test size and cost functions
    EXPECT_EQ(strippedScanOp.getExactSize(), baseScan.getExactSize());
    EXPECT_EQ(strippedScanOp.getCostEstimate(), baseScan.getCostEstimate());
    EXPECT_EQ(strippedScanOp.getSizeEstimate(), baseScan.getSizeEstimate());
    EXPECT_EQ(strippedScanOp.knownEmptyResult(), baseScan.knownEmptyResult());
    EXPECT_EQ(strippedScanOp.isIndexScanWithNumVariables(0),
              baseScan.isIndexScanWithNumVariables(0));
    EXPECT_EQ(strippedScanOp.isIndexScanWithNumVariables(1),
              baseScan.isIndexScanWithNumVariables(1));
    EXPECT_EQ(strippedScanOp.isIndexScanWithNumVariables(2),
              baseScan.isIndexScanWithNumVariables(2));
    EXPECT_EQ(strippedScanOp.isIndexScanWithNumVariables(3),
              baseScan.isIndexScanWithNumVariables(3));

    // Test optimization functions
    EXPECT_EQ(strippedScanOp.supportsLimitOffset(),
              baseScan.supportsLimitOffset());

    // Test specification functions
    EXPECT_EQ(strippedScanOp.getScanSpecification().col0Id(),
              baseScan.getScanSpecification().col0Id());
    EXPECT_EQ(strippedScanOp.getScanSpecification().col1Id(),
              baseScan.getScanSpecification().col1Id());
    EXPECT_EQ(strippedScanOp.getScanSpecification().col2Id(),
              baseScan.getScanSpecification().col2Id());
    auto strippedTriple = strippedScanOp.getPermutedTriple();
    auto baseTriple = baseScan.getPermutedTriple();
    for (size_t i = 0; i < 3; ++i) {
      EXPECT_EQ(*strippedTriple[i], *baseTriple[i]);
    }

    // Test column origin function for variables that exist in both scans
    for (const auto& var : varsToKeep) {
      EXPECT_EQ(strippedScanOp.columnOriginatesFromGraphOrUndef(var),
                baseScan.columnOriginatesFromGraphOrUndef(var));
    }
  };

  // Same as above, but bind the first two arguments by reference.
  auto testStrippedBindFront = [&](IndexScan& baseScan,
                                   IndexScan& baseScanDifferentVars) {
    return [&](const std::vector<Variable>& varsToKeep,
               const std::vector<ColumnIndex>& sortedOn,
               ad_utility::source_location l = AD_CURRENT_SOURCE_LOC()) {
      return testStrippedColumns(baseScan, baseScanDifferentVars, varsToKeep,
                                 sortedOn, l);
    };
  };

  // Test group 1: Full scan with three variables (?x ?y ?z)
  {
    IndexScan fullScan{qec, Permutation::SPO,
                       SparqlTripleSimple{Var{"?x"}, Var{"?y"}, Var{"?z"}}};
    IndexScan fullScanDifferentVars{
        qec, Permutation::SPO,
        SparqlTripleSimple{Var{"?x"}, Var{"?a"}, Var{"?b"}}};
    auto testFullScanStrippedColumns =
        testStrippedBindFront(fullScan, fullScanDifferentVars);

    // Test all combinations for full scan
    testFullScanStrippedColumns({}, {});            // zero columns
    testFullScanStrippedColumns({Var{"?x"}}, {0});  // single column (sorted)
    testFullScanStrippedColumns({Var{"?y"}}, {});  // single column (not sorted)
    testFullScanStrippedColumns({Var{"?z"}}, {});  // single column (not sorted)
    testFullScanStrippedColumns({Var{"?x"}, Var{"?y"}},
                                {0, 1});  // two columns (sorted)
    testFullScanStrippedColumns({Var{"?x"}, Var{"?z"}},
                                {0});  // two columns (sorted)
    testFullScanStrippedColumns({Var{"?y"}, Var{"?z"}},
                                {});  // two columns (not sorted)
    testFullScanStrippedColumns({Var{"?x"}, Var{"?y"}, Var{"?z"}},
                                {0, 1, 2});  // all columns
  }

  // Test group 2: Two-variable scan with fixed predicate using PSO permutation
  {
    using I = TripleComponent::Iri;
    IndexScan twoVarScan{
        qec, Permutation::PSO,
        SparqlTripleSimple{Var{"?x"}, I::fromIriref("<p>"), Var{"?y"}}};
    IndexScan twoVarScanDifferentVars{
        qec, Permutation::PSO,
        SparqlTripleSimple{Var{"?x"}, I::fromIriref("<p>"), Var{"?b"}}};

    auto testTwoVarScanStrippedColumns =
        testStrippedBindFront(twoVarScan, twoVarScanDifferentVars);

    // Test all combinations for two-variable scan
    testTwoVarScanStrippedColumns({}, {});            // zero columns
    testTwoVarScanStrippedColumns({Var{"?x"}}, {0});  // single column (sorted)
    testTwoVarScanStrippedColumns({Var{"?y"}},
                                  {});  // single column (not sorted)
    testTwoVarScanStrippedColumns({Var{"?x"}, Var{"?y"}},
                                  {0, 1});  // both columns
  }

  // Test group 3: One-variable scan with two fixed entries
  {
    using I = TripleComponent::Iri;
    IndexScan oneVarScan{qec, Permutation::SPO,
                         SparqlTripleSimple{I::fromIriref("<s>"),
                                            I::fromIriref("<p>"), Var{"?x"}}};
    IndexScan oneVarScanDifferentVars{
        qec, Permutation::SPO,
        SparqlTripleSimple{I::fromIriref("<s>"), I::fromIriref("<p>"),
                           Var{"?x"}}};
    auto testOneVarScanStrippedColumns =
        testStrippedBindFront(oneVarScan, oneVarScanDifferentVars);

    // Test all combinations for one-variable scan
    testOneVarScanStrippedColumns({}, {});            // zero columns
    testOneVarScanStrippedColumns({Var{"?x"}}, {0});  // single column (sorted)
  }

  // Test group 4: Zero-variable scan with three fixed entries
  {
    using I = TripleComponent::Iri;
    IndexScan zeroVarScan{
        qec, Permutation::SPO,
        SparqlTripleSimple{I::fromIriref("<s>"), I::fromIriref("<p>"),
                           I::fromIriref("<o>")}};
    IndexScan zeroVarScanDifferentVars{
        qec, Permutation::SPO,
        SparqlTripleSimple{I::fromIriref("<s>"), I::fromIriref("<p>"),
                           I::fromIriref("<o>")}};
    auto testZeroVarScanStrippedColumns =
        testStrippedBindFront(zeroVarScan, zeroVarScanDifferentVars);

    // Test the only combination for zero-variable scan
    testZeroVarScanStrippedColumns({},
                                   {});  // zero columns (only possible case)
  }

  // Test group 5: Scan with additional variables
  {
    SparqlTripleSimple tripleWithAdditionalVar{Var{"?x"}, Var{"?y"}, Var{"?z"}};
    tripleWithAdditionalVar.additionalScanColumns_.emplace_back(
        3, Var{"?additional"});
    IndexScan scanWithAdditional{qec, Permutation::SPO,
                                 tripleWithAdditionalVar};

    SparqlTripleSimple tripleWithAdditionalVarDifferent{Var{"?x"}, Var{"?b"},
                                                        Var{"?c"}};
    tripleWithAdditionalVarDifferent.additionalScanColumns_.emplace_back(
        3, Var{"?additional2"});
    IndexScan scanWithAdditionalDifferentVars{qec, Permutation::SPO,
                                              tripleWithAdditionalVarDifferent};

    auto testScanWithAdditionalStrippedColumns = testStrippedBindFront(
        scanWithAdditional, scanWithAdditionalDifferentVars);

    // Test all combinations for scan with additional variables
    testScanWithAdditionalStrippedColumns({}, {});  // zero columns
    testScanWithAdditionalStrippedColumns({Var{"?x"}},
                                          {0});  // single regular (sorted)
    testScanWithAdditionalStrippedColumns({Var{"?y"}},
                                          {});  // single regular (not sorted)
    testScanWithAdditionalStrippedColumns({Var{"?z"}},
                                          {});  // single regular (not sorted)
    testScanWithAdditionalStrippedColumns(
        {Var{"?additional"}}, {});  // single additional (not sorted)
    testScanWithAdditionalStrippedColumns({Var{"?x"}, Var{"?y"}},
                                          {0, 1});  // two regular
    testScanWithAdditionalStrippedColumns({Var{"?x"}, Var{"?additional"}},
                                          {0});  // regular + additional
    testScanWithAdditionalStrippedColumns(
        {Var{"?y"}, Var{"?additional"}},
        {});  // regular + additional (not sorted)
    testScanWithAdditionalStrippedColumns({Var{"?x"}, Var{"?y"}, Var{"?z"}},
                                          {0, 1, 2});  // all regular
    testScanWithAdditionalStrippedColumns(
        {Var{"?x"}, Var{"?y"}, Var{"?additional"}},
        {0, 1});  // regular + additional
    testScanWithAdditionalStrippedColumns(
        {Var{"?x"}, Var{"?y"}, Var{"?z"}, Var{"?additional"}},
        {0, 1, 2, 3});  // all columns
  }
}

// _____________________________________________________________________________
TEST(IndexScanTest, StripColumnsWithPrefiltering) {
  TestIndexConfig config;
  using namespace ad_utility::memory_literals;
  config.blocksizePermutations = 8_B;
  config.turtleInput = "<s> <p> <o>. <s2> <p> <o>. <s2> <p2> <o2>";
  auto qec = ad_utility::testing::getQec(config);

  using namespace makeFilterExpression;
  using namespace filterHelper;

  // Create base scan with three free variables (?x ?y ?z) using SPO
  // permutation where subject is bound to ?x (first column)

  auto makeBaseScan = [&qec]() {
    return ad_utility::makeExecutionTree<IndexScan>(
        qec, Permutation::SPO,
        SparqlTripleSimple{Var{"?x"}, Var{"?y"}, Var{"?z"}});
  };
  auto baseScanTree = makeBaseScan();
  IndexScan& baseScanForPrefilter =
      dynamic_cast<IndexScan&>(*baseScanTree->getRootOperation());

  // Create prefilter condition: ?x < <s2>
  auto prefilterPairs = []() {
    return makePrefilterVec(pr(lt(LocalVocabEntry::iriref("<s2>")), Var{"?x"}));
  };

  // Test with different variable combinations
  std::vector<std::vector<Variable>> testCases = {
      {Var{"?x"}},                       // single column (sorted)
      {Var{"?y"}},                       // single column (not sorted)
      {Var{"?x"}, Var{"?y"}},            // two columns
      {Var{"?x"}, Var{"?y"}, Var{"?z"}}  // all columns
  };

  for (const auto& varsToKeep : testCases) {
    // Approach 1: First apply prefilter, then strip columns
    auto prefilteredThenStripped = [&]() {
      auto prefilteredQet =
          makeBaseScan()
              ->setPrefilterGetUpdatedQueryExecutionTree(prefilterPairs())
              .value_or(makeBaseScan());
      std::set<Variable> varsSet(varsToKeep.begin(), varsToKeep.end());
      return QueryExecutionTree::makeTreeWithStrippedColumns(
          std::move(prefilteredQet), varsSet);
    }();

    auto strippedThenPrefiltered = [&]() {
      // Approach 2: First strip columns, then apply prefilter
      std::set<Variable> varsSet(varsToKeep.begin(), varsToKeep.end());
      auto strippedFirst = QueryExecutionTree::makeTreeWithStrippedColumns(
          makeBaseScan(), varsSet);
      return strippedFirst
          ->setPrefilterGetUpdatedQueryExecutionTree(prefilterPairs())
          .value_or(strippedFirst);
    }();

    // Both approaches should yield the same cache key (indicating equivalent
    // operations)
    EXPECT_EQ(prefilteredThenStripped->getCacheKey(),
              strippedThenPrefiltered->getCacheKey())
        << "Cache keys should be equal for varsToKeep with "
        << varsToKeep.size() << " variables";

    // Both approaches should yield the same result width
    EXPECT_EQ(prefilteredThenStripped->getResultWidth(),
              strippedThenPrefiltered->getResultWidth())
        << "Result widths should be equal for varsToKeep with "
        << varsToKeep.size() << " variables";

    // Both approaches should yield the same variable columns
    EXPECT_EQ(prefilteredThenStripped->getVariableColumns(),
              strippedThenPrefiltered->getVariableColumns())
        << "Variable columns should be equal for varsToKeep with "
        << varsToKeep.size() << " variables";

    // Both approaches should yield the same actual results
    // First get the full prefiltered result (without column stripping)
    auto fullPrefilteredQet =
        makeBaseScan()
            ->setPrefilterGetUpdatedQueryExecutionTree(prefilterPairs())
            .value_or(makeBaseScan());

    qec->clearCacheUnpinnedOnly();
    IdTable fullResult =
        fullPrefilteredQet->getResult(false)->idTable().clone();

    // Create expected result by applying column subset (same logic as
    // infrastructure lambda)
    std::vector<std::pair<Variable, ColumnIndex>> columnOrigins =
        getVarsAndColumnIndices(varsToKeep, baseScanForPrefilter);

    std::vector<ColumnIndex> originalColumnIndices;
    for (const auto& [var, colIdx] : columnOrigins) {
      originalColumnIndices.push_back(colIdx);
    }

    IdTable expectedResult =
        fullResult.asColumnSubsetView(originalColumnIndices).clone();

    // Now compare both approaches against the expected result
    qec->clearCacheUnpinnedOnly();
    IdTable result1 =
        prefilteredThenStripped->getResult(false)->idTable().clone();
    qec->clearCacheUnpinnedOnly();
    IdTable result2 =
        strippedThenPrefiltered->getResult(false)->idTable().clone();
    EXPECT_THAT(result1, matchesIdTable(expectedResult.clone()))
        << "Approach 1 (prefilter-then-strip) should match expected result for "
        << varsToKeep.size() << " variables";
    EXPECT_THAT(result2, matchesIdTable(expectedResult.clone()))
        << "Approach 2 (strip-then-prefilter) should match expected result for "
        << varsToKeep.size() << " variables";
  }
}
