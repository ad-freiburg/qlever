
#include <gtest/gtest.h>

#include <memory>

#include "engine/idTable/CompressedExternalIdTable.h"
#include "global/ValueId.h"
#include "index/ExternalSortFunctors.h"
#include "libqlever/Qlever.h"
#include "rdfTypes/Variable.h"
#include "util/CancellationHandle.h"
#include "util/Exception.h"
#include "util/MemorySize/MemorySize.h"
#include "util/ProgressBar.h"

namespace {

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
      "PREFIX geo: <http://www.opengis.net/ont/geosparql#> SELECT * "
      "WHERE { ?a "
      "geo:hasGeometry ?b . ?b geo:asWKT ?c . BIND (0 AS ?g) }");
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
  size_t numCols = 4;
  using Sorter = ad_utility::CompressedExternalIdTableSorter<
      SortTriple<0, 1, 2>,  // TODO non-3-col input?
      NumStaticCols>;
  Sorter spoSorter{indexBasename + ".mv-spo-sorter.dat", numCols, memoryLimit,
                   allocator};
  size_t totalTriples = 0;
  ad_utility::ProgressBar progressBar{totalTriples, "Triples processed: "};

  ///
  auto idxS = qet->getVariableColumn(
      Variable{"?a"});  // or equiv: vc[Variable{"?b"}].columnIndex_
  auto idxP = qet->getVariableColumn(Variable{"?b"});
  auto idxO = qet->getVariableColumn(Variable{"?c"});
  auto idxG = qet->getVariableColumn(Variable{"?g"});

  // std::array<ColumnIndex, NumStaticCols> columnPermutationBeforeSorting{
  //     idxS, idxP, idxO, idxG};
  std::vector<ColumnIndex> columnPermutationBeforeSorting{idxS, idxP, idxO,
                                                          idxG};

  ///

  for (auto& [block, vocab] : generator) {
    AD_CORRECTNESS_CHECK(vocab.empty());
    totalTriples += block.numRows();
    // Permute this block to SPO column order for sorting.
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
  auto scan = p.scan(scanSpecAndBlocks, {}, cancellationHandle, emptySnapshot);
  // auto scan = p.scan(scanSpecAndBlocks, {}, cancellationHandle, snapshot);
  AD_LOG_INFO << "scan: " << scan.numRows() << std::endl;
  auto v = scan.at(0, 1);
  EXPECT_EQ(v.getDatatype(), Datatype::VocabIndex);
  AD_LOG_INFO << tmpqec->getIndex().getVocab()[v.getVocabIndex()] << std::endl;
}

}  // namespace
