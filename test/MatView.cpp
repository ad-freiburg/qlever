
#include <gtest/gtest.h>

#include <memory>

#include "engine/IndexScan.h"
#include "engine/MaterializedViews.h"
#include "engine/idTable/CompressedExternalIdTable.h"
#include "global/ValueId.h"
#include "gmock/gmock.h"
#include "index/ExternalSortFunctors.h"
#include "libqlever/Qlever.h"
#include "rdfTypes/Iri.h"
#include "rdfTypes/Variable.h"
#include "util/CancellationHandle.h"
#include "util/Exception.h"
#include "util/MemorySize/MemorySize.h"
#include "util/ProgressBar.h"

namespace {

static constexpr std::string_view LlacDEngolasters =
    "\"POLYGON((1.565688 42.5186623,1.5661338 42.5182027,1.5663576 "
    "42.5179785,1.5664138 42.5179865,1.5664705 42.5180128,1.5667617 "
    "42.5181478,1.566954 42.5182088,1.5672636 42.5182493,1.5674062 "
    "42.5182732,1.5675577 42.5182992,1.5675701 42.5183013,1.5678669 "
    "42.518455,1.5681637 42.5186286,1.568665 42.5189332,1.568852 "
    "42.5190495,1.5690621 42.5191871,1.5691127 42.519272,1.569233 "
    "42.5193142,1.5693351 42.5193446,1.5695777 42.5195104,1.5696504 "
    "42.5195638,1.5697548 42.5196404,1.5699242 42.5197572,1.5700667 "
    "42.5198867,1.5701905 42.5200199,1.5703702 42.5202766,1.5705206 "
    "42.5204751,1.5707193 42.520763,1.5707791 42.5208843,1.5707822 "
    "42.5208906,1.5708125 42.5210452,1.5708318 42.5211242,1.5708511 "
    "42.5212285,1.5709177 42.5212841,1.5709855 42.5213482,1.5710119 "
    "42.5214258,1.5710063 42.5214948,1.5709825 42.5215449,1.5709303 "
    "42.5215831,1.5708868 42.521559,1.5708732 42.5215706,1.570861 "
    "42.5215821,1.5708845 42.5215986,1.570812 42.521631,1.570716 "
    "42.5216543,1.5706377 42.5216514,1.5705614 42.5216513,1.5704713 "
    "42.5216378,1.570403 42.5216281,1.5703397 42.5216166,1.5702273 "
    "42.5215941,1.5701085 42.5215799,1.5699714 42.5215719,1.5698981 "
    "42.5215625,1.5698363 42.5215463,1.5697602 42.521524,1.5696768 "
    "42.5214852,1.5696101 42.5214381,1.5695306 42.5213436,1.5694539 "
    "42.5212441,1.5692922 42.521091,1.5691597 42.5209889,1.569066 "
    "42.5209277,1.5689973 42.5208867,1.5687751 42.5207655,1.5686955 "
    "42.5207345,1.5685358 42.5206761,1.5684397 42.520637,1.5683306 "
    "42.5205725,1.5681799 42.5204791,1.568085 42.520424,1.5679194 "
    "42.5203323,1.5677271 42.5202674,1.5676224 42.5201892,1.5675409 "
    "42.5201152,1.5674484 42.5200467,1.5673622 42.5200081,1.5672228 "
    "42.5199284,1.5670023 42.5198247,1.5667776 42.5197182,1.5666388 "
    "42.5196381,1.5665155 42.519531,1.5664183 42.5194184,1.5662371 "
    "42.5191672,1.566099 42.5189767,1.5659643 42.518831,1.5658353 "
    "42.5187105,1.565792 42.5186963,1.565688 "
    "42.5186623))\"^^<http://www.opengis.net/ont/geosparql#wktLiteral>";

TEST(MatView, Writer) {
  std::string indexBasename = "osm-andorra";
  qlever::EngineConfig config;
  config.baseName_ = indexBasename;
  std::string memoryLimitStr = "16GB";
  auto allocator = ad_utility::makeUnlimitedAllocator<Id>();
  auto memoryLimit = ad_utility::MemorySize::parse(memoryLimitStr);
  qlever::Qlever qlv{config};

  AD_LOG_INFO << "Started. " << std::endl;

  AD_LOG_INFO << "Plan " << std::endl;
  auto [qet, qec, parsed] = qlv.parseAndPlanQuery(
      "PREFIX geo: <http://www.opengis.net/ont/geosparql#> SELECT ?a ?b ?c ?g "
      "?x WHERE { ?a geo:hasGeometry ?b . ?b geo:asWKT ?c . VALUES ?g { 42 43 "
      "}  BIND (RAND() AS ?x) }");
  AD_LOG_INFO << "OnDiskBase: " << qec->getIndex().getOnDiskBase() << std::endl;
  AD_LOG_INFO << "Run hasGeom/asWKT " << std::endl;
  auto res = qet->getResult(true);
  // AD_LOG_INFO << res->idTable().numRows() << " rows x "
  //             << res->idTable().numColumns() << " cols" << std::endl;
  AD_CORRECTNESS_CHECK(!res->isFullyMaterialized());
  auto generator = res->idTables();
  auto vc = qet->getVariableColumns();
  // This does not hold, switch columns
  // EXPECT_EQ(vc[Variable{"?a"}].columnIndex_, 0);
  // EXPECT_EQ(vc[Variable{"?b"}].columnIndex_, 1);
  // EXPECT_EQ(vc[Variable{"?c"}].columnIndex_, 2);
  // EXPECT_EQ(vc[Variable{"?g"}].columnIndex_, 3);

  constexpr size_t NumStaticCols =
      0;  // ---> change to "dynamic table" (NumStaticCols == 0) -> then Sorter
  // needs gets num cols only in constructor but templated is 0
  auto targetVars = parsed.getVisibleVariables();
  AD_CONTRACT_CHECK(targetVars.size() >=
                    4);  // Fill with zeroes or something ...
  // EXPECT_THAT(targetVars,
  //             ::testing::ElementsAreArray({Variable{"?a"}, Variable{"?b"},
  //                                          Variable{"?c"}, Variable{"?g"}}));
  // size_t numCols = 4;
  size_t numCols = targetVars.size();
  using Sorter =
      ad_utility::CompressedExternalIdTableSorter<SortTriple<0, 1, 2>,
                                                  NumStaticCols>;
  Sorter spoSorter{indexBasename + ".mv-spo-sorter.dat", numCols, memoryLimit,
                   allocator};
  size_t totalTriples = 0;
  ad_utility::ProgressBar progressBar{totalTriples, "Triples processed: "};

  ///
  // auto idxS = qet->getVariableColumn(
  //     Variable{"?a"});  // or equiv: vc[Variable{"?b"}].columnIndex_
  // auto idxP = qet->getVariableColumn(Variable{"?b"});
  // auto idxO = qet->getVariableColumn(Variable{"?c"});
  // auto idxG = qet->getVariableColumn(Variable{"?g"});

  // std::array<ColumnIndex, NumStaticCols> columnPermutationBeforeSorting{
  //     idxS, idxP, idxO, idxG};
  // std::vector<ColumnIndex> columnPermutationBeforeSorting{idxS, idxP, idxO,
  //                                                         idxG};
  std::vector<ColumnIndex> columnPermutationBeforeSorting;
  columnPermutationBeforeSorting.reserve(numCols);
  for (const auto& var : targetVars) {
    columnPermutationBeforeSorting.push_back(qet->getVariableColumn(var));
  }
  ///

  for (auto& [block, vocab] : generator) {
    AD_CORRECTNESS_CHECK(vocab.empty());
    totalTriples += block.numRows();
    // Permute this block to SPO column order for sorting. ---> the IdTable may
    // have a different column ordering from the SELECT statement, thus we must
    // permute this to the right col ordering how our view should look (and for
    // sorting)
    block.setColumnSubset(columnPermutationBeforeSorting);
    // columnBasedIdTable::IdTable blockNew{4};
    // blockNew.reserve(block.numRows());
    // for (size_t row = 0; row < block.numRows(); ++row) {
    // }
    spoSorter.pushBlock(block);
    if (progressBar.update()) {
      AD_LOG_INFO << progressBar.getProgressString() << std::flush;
    }
  }
  AD_LOG_INFO << progressBar.getFinalProgressString() << std::flush;

  // Write .
  AD_LOG_INFO << "Creating permutation..." << std::endl;
  auto sortedBlocksSPO = spoSorter.template getSortedBlocks<0>();
  // std::array<ColumnIndex, NumStaticCols> columnPermutation{
  //     ColumnIndex{0}, ColumnIndex{1}, ColumnIndex{2}, ColumnIndex{3}};
  // AD_CORRECTNESS_CHECK(columnPermutation.size() == numCols);
  // std::vector<ColumnIndex> columnPermutation;
  // columnPermutation.reserve(numCols);
  // for (size_t i = 0; i < numCols; ++i) {
  //   columnPermutation.push_back(i);
  // }

  // auto idxS = qet->getVariableColumn(
  //     Variable{"?a"});  // or equiv: vc[Variable{"?b"}].columnIndex_
  // auto idxP = qet->getVariableColumn(Variable{"?b"});
  // auto idxO = qet->getVariableColumn(Variable{"?c"});
  // auto idxG = qet->getVariableColumn(Variable{"?g"});

  // std::array<ColumnIndex, NumStaticCols> columnPermutation{idxS, idxP, idxO,
  //                                                          idxG};

  // auto permuteBlock = [&columnPermutation](auto&& block) {
  //   block.setColumnSubset(columnPermutation);
  //   return std::move(block);
  // };
  // auto sortedBlocks = ad_utility::CachingTransformInputRange{
  //     ad_utility::OwningView{std::move(sortedBlocksSPO)}, permuteBlock};
  std::string spoFilename = indexBasename + ".mv.index.spo";
  CompressedRelationWriter spoWriter{
      numCols,
      ad_utility::File(spoFilename, "w"),
      // ad_utility::MemorySize::kilobytes(0)
      UNCOMPRESSED_BLOCKSIZE_COMPRESSED_METADATA_PER_COLUMN,
  };

  // ----
  std::string sopFilename = indexBasename + ".mv.index.sop";
  CompressedRelationWriter sopWriter{
      numCols,
      ad_utility::File(sopFilename, "w"),
      // ad_utility::MemorySize::kilobytes(0)
      UNCOMPRESSED_BLOCKSIZE_COMPRESSED_METADATA_PER_COLUMN,
  };
  // ---/---

  // Write the metadata for PSO and POS.
  AD_LOG_DEBUG << "Writing metadata ..." << std::endl;
  qlever::KeyOrder spoKeyOrder{0, 1, 2, 3};
  using MetaData = IndexMetaDataMmap;
  MetaData spoMetaData;
  spoMetaData.setup(spoFilename + ".meta", ad_utility::CreateTag{});
  auto spoCallback =
      [&spoMetaData](ql::span<const CompressedRelationMetadata> md) {
        AD_LOG_INFO << "cb0 " << md.size() << std::endl;
        for (const auto& m : md) {
          spoMetaData.add(m);
        }
      };
  // ----
  MetaData sopMetaData;
  sopMetaData.setup(sopFilename + ".meta", ad_utility::CreateTag{});
  auto sopCallback =
      [&sopMetaData](ql::span<const CompressedRelationMetadata> md) {
        AD_LOG_INFO << "cb1 " << md.size() << std::endl;
        for (const auto& m : md) {
          sopMetaData.add(m);
        }
      };
  // ---/---
  auto [numDistinctPredicates, blockData1, blockData2] =
      CompressedRelationWriter::createPermutationPair(
          spoFilename + ".sorter", {spoWriter, spoCallback},
          {sopWriter, sopCallback},
          // ad_utility::InputRangeTypeErased{std::move(sortedBlocks)},
          ad_utility::InputRangeTypeErased{std::move(sortedBlocksSPO)},
          spoKeyOrder, {});

  spoMetaData.blockData() = std::move(blockData1);
  spoMetaData.calculateStatistics(numDistinctPredicates);
  spoMetaData.setName(indexBasename + ".mv");
  {
    ad_utility::File psoFile(spoFilename, "r+");
    spoMetaData.appendToFile(&psoFile);
  }
  AD_LOG_INFO << "Statistics for PSO: " << spoMetaData.statistics()
              << std::endl;

  // ----
  sopMetaData.blockData() = std::move(blockData2);
  sopMetaData.calculateStatistics(numDistinctPredicates);
  sopMetaData.setName(indexBasename + ".mv");
  {
    ad_utility::File posFile(sopFilename, "r+");
    sopMetaData.appendToFile(&posFile);
  }
  AD_LOG_INFO << "Statistics for POS: " << sopMetaData.statistics()
              << std::endl;
  // ---/---
}

TEST(MatView, Reader) {
  // ------------------------------Redundant----------------

  std::string indexBasename = "osm-andorra";
  qlever::EngineConfig config;
  config.baseName_ = indexBasename;
  auto allocator = ad_utility::makeUnlimitedAllocator<Id>();
  qlever::Qlever qlv{config};

  AD_LOG_INFO << "Started. " << std::endl;

  // ------------------------------///----------------

  //
  AD_LOG_INFO << "get iri id" << std::endl;
  auto [tmpqet, tmpqec, tmpplan] = qlv.parseAndPlanQuery(
      "SELECT (<https://www.openstreetmap.org/way/6593464> AS ?id) {}");
  auto tmpres = tmpqet->getResult();
  auto osmId = tmpres->idTable().at(0, 0);
  ASSERT_EQ(osmId.getDatatype(), Datatype::VocabIndex);
  AD_LOG_INFO << osmId << std::endl;
  auto scanSpec =
      ScanSpecificationAsTripleComponent{
          TripleComponent::Iri::fromIriref(
              "<https://www.openstreetmap.org/way/6593464>"),
          std::nullopt, std::nullopt}
          .toScanSpecification(tmpqec->getIndex().getImpl());
  //

  AD_LOG_INFO << "load permutation" << std::endl;
  Permutation p{Permutation::Enum::SPO, allocator};
  std::string onDiskBaseP = indexBasename + ".mv";
  // std::string onDiskBaseP = indexBasename + ".view.geom";
  p.loadFromDisk(onDiskBaseP, [](Id) { return false; }, false);
  EXPECT_TRUE(p.isLoaded());
  AD_LOG_INFO << "get snapshot" << std::endl;
  // auto snapshot = tmpqec->locatedTriplesSnapshot();
  // static const LocatedTriplesSnapshot emptySnapshot{
  //     {}, LocalVocab{}.getLifetimeExtender(), 0};

  LocatedTriplesPerBlockAllPermutations emptyLocatedTriples;
  emptyLocatedTriples[static_cast<size_t>(Permutation::SPO)]
      .setOriginalMetadata(p.metaData().blockDataShared());
  LocalVocab emptyVocab;
  LocatedTriplesSnapshot emptySnapshot{emptyLocatedTriples,
                                       emptyVocab.getLifetimeExtender(), 0};

  AD_LOG_INFO << "get scan spec and blocks" << std::endl;
  ad_utility::SharedCancellationHandle cancellationHandle =
      std::make_shared<ad_utility::CancellationHandle<>>();
  scanSpec = {osmId, std::nullopt, std::nullopt};
  // scanSpec = {std::nullopt, std::nullopt, std::nullopt};
  auto scanSpecAndBlocks = p.getScanSpecAndBlocks(scanSpec, emptySnapshot);
  // auto scanSpecAndBlocks = p.getScanSpecAndBlocks(scanSpec, snapshot);

  auto [lb, ub] = p.getSizeEstimateForScan(scanSpecAndBlocks, emptySnapshot);
  // auto [lb, ub] = p.getSizeEstimateForScan(scanSpecAndBlocks, snapshot);
  AD_LOG_INFO << "scan size est " << lb << " - " << ub << std::endl;
  AD_LOG_INFO << "scan size "
              << p.getResultSizeOfScan(scanSpecAndBlocks, emptySnapshot)
              // << p.getResultSizeOfScan(scanSpecAndBlocks, snapshot)
              << std::endl;
  std::vector<ColumnIndex> additionalColumns{
      3, 4};  // Col 3 = 42, Col 4 should contain RAND()
  auto scan = p.scan(scanSpecAndBlocks, additionalColumns, cancellationHandle,
                     emptySnapshot);
  // auto scan = p.scan(scanSpecAndBlocks, {}, cancellationHandle, snapshot);
  AD_LOG_INFO << "scan: " << scan.numRows() << std::endl;
  auto v = scan.at(0, 1);
  EXPECT_EQ(v.getDatatype(), Datatype::VocabIndex);
  // AD_LOG_INFO << tmpqec->getIndex().getVocab()[v.getVocabIndex()] <<
  // std::endl;
  auto string = tmpqec->getIndex().getVocab()[v.getVocabIndex()];
  EXPECT_EQ(string, LlacDEngolasters);

  auto c = scan.at(0, 2);
  EXPECT_EQ(c.getDatatype(), Datatype::Int);
  AD_LOG_INFO << c.getInt() << std::endl;

  auto r = scan.at(0, 3);
  EXPECT_EQ(r.getDatatype(), Datatype::Double);
  AD_LOG_INFO << r.getDouble() << std::endl;
}

TEST(MatView, Writer2) {
  qlever::EngineConfig config;
  config.baseName_ = "osm-andorra";
  qlever::Qlever qlv{config};
  auto qp = qlv.parseAndPlanQuery(
      "PREFIX geo: <http://www.opengis.net/ont/geosparql#> SELECT ?a ?b ?c ?g "
      "?x WHERE { ?a geo:hasGeometry ?b . ?b geo:asWKT ?c . VALUES ?g { 42 43 "
      "}  BIND (RAND() AS ?x) }");

  MaterializedViewWriter mvw{"geom", qp};
  mvw.writeViewToDisk();
}

TEST(MatView, Reader2) {
  qlever::EngineConfig config;
  config.baseName_ = "osm-andorra";
  qlever::Qlever qlv{config};

  auto [tmpqet, tmpqec, tmpplan] = qlv.parseAndPlanQuery(
      "SELECT (<https://www.openstreetmap.org/way/6593464> AS ?id) {}");
  auto tmpres = tmpqet->getResult();
  auto osmId = tmpres->idTable().at(0, 0);
  ASSERT_EQ(osmId.getDatatype(), Datatype::VocabIndex);
  //-----------------------------------------------------

  MaterializedViewsManager m{tmpqec->getIndex().getOnDiskBase()};
  auto view = m.getView("geom");
  auto p = view.getPermutation();

  //-----------------------------------------------------
  SparqlTriple::AdditionalScanColumns additionalCols{// From MagicService
                                                     {3, Variable{"?x"}},
                                                     {4, Variable{"?y"}}};
  IndexScan scan(tmpqec.get(), Permutation::Enum::SPO,
                 {ad_utility::triple_component::Iri::fromIriref(
                      "<https://www.openstreetmap.org/way/6593464>"),
                  Variable{"?a"}, Variable{"?b"}, additionalCols},
                 IndexScan::Graphs::All(), std::nullopt, view);
  auto res = scan.getResult();
  AD_LOG_INFO << "scan: " << res->idTable().numRows() << std::endl;

  auto v = res->idTable().at(0, 1);
  EXPECT_EQ(v.getDatatype(), Datatype::VocabIndex);
  auto string = tmpqec->getIndex().getVocab()[v.getVocabIndex()];
  EXPECT_EQ(string, LlacDEngolasters);

  auto c = res->idTable().at(0, 2);
  EXPECT_EQ(c.getDatatype(), Datatype::Int);
  AD_LOG_INFO << c.getInt() << std::endl;

  auto r = res->idTable().at(0, 3);
  EXPECT_EQ(r.getDatatype(), Datatype::Double);
  AD_LOG_INFO << r.getDouble() << std::endl;
}

/*

Write:

???

Load:

???

Scan:

SERVICE <materialized-view> {
_:config <name> "geom" ; <col-a> ?a ; <col-b> ?b ; <col-x> ?x .
}
# or: <col-0> ?a ; <col-1> ?b .

*/

TEST(MatView, Writer3) {
  qlever::EngineConfig config;
  config.baseName_ = "osm-andorra";
  qlever::Qlever qlv{config};
  qlv.writeMaterializedView(
      "geom",
      "PREFIX geo: <http://www.opengis.net/ont/geosparql#> SELECT ?a ?b ?c ?g "
      "?x WHERE { ?a geo:hasGeometry ?b . ?b geo:asWKT ?c . VALUES ?g { 42 43 "
      "}  BIND (RAND() AS ?x) }");
}

}  // namespace
