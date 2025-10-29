
#include <gtest/gtest.h>

#include "engine/idTable/CompressedExternalIdTable.h"
#include "index/ExternalSortFunctors.h"
#include "libqlever/Qlever.h"
#include "util/Exception.h"
#include "util/ProgressBar.h"

namespace {

TEST(MatView, Exp) {
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
      "PREFIX geo: <http://www.opengis.net/ont/geosparql#> SELECT * WHERE { ?a "
      "geo:hasGeometry ?b . ?b geo:asWKT ?c }");
  AD_LOG_INFO << "Run hasGeom/asWKT " << std::endl;
  auto res = qet->getResult(true);
  // AD_LOG_INFO << res->idTable().numRows() << " rows x "
  //             << res->idTable().numColumns() << " cols" << std::endl;
  AD_CORRECTNESS_CHECK(!res->isFullyMaterialized());
  auto generator = res->idTables();

  constexpr size_t NumStaticCols = 3;
  using Sorter = ad_utility::CompressedExternalIdTableSorter<
      SortTriple<0, 1, 2>,  // TODO non-3-col input?
      NumStaticCols>;
  Sorter spoSorter{indexBasename + ".mv-spo-sorter.dat", NumStaticCols,
                   memoryLimit, allocator};
  size_t totalTriples = 0;
  ad_utility::ProgressBar progressBar{totalTriples, "Triples processed: "};
  for (auto& [block, vocab] : generator) {
    AD_CORRECTNESS_CHECK(vocab.empty());
    totalTriples += block.numRows();
    spoSorter.pushBlock(block);
    if (progressBar.update()) {
      AD_LOG_INFO << progressBar.getProgressString() << std::flush;
    }
  }
  AD_LOG_INFO << progressBar.getFinalProgressString() << std::flush;

  // Write .
  AD_LOG_INFO << "Creating permutation..." << std::endl;
  auto sortedBlocksSPO = spoSorter.template getSortedBlocks<0>();
  std::array<ColumnIndex, NumStaticCols> columnPermutation{
      ColumnIndex{0}, ColumnIndex{1}, ColumnIndex{2}};
  auto permuteBlock = [&columnPermutation](auto&& block) {
    block.setColumnSubset(columnPermutation);
    return std::move(block);
  };
  auto sortedBlocks = ad_utility::CachingTransformInputRange{
      ad_utility::OwningView{std::move(sortedBlocksSPO)}, permuteBlock};
  std::string spoFilename = indexBasename + ".mv.index.spo";
  CompressedRelationWriter spoWriter{
      NumStaticCols, ad_utility::File(spoFilename, "w"),
      UNCOMPRESSED_BLOCKSIZE_COMPRESSED_METADATA_PER_COLUMN};

  // Write the metadata for PSO and POS.
  AD_LOG_DEBUG << "Writing metadata ..." << std::endl;
  qlever::KeyOrder spoKeyOrder{0, 1, 2, 3};
  using MetaData = IndexMetaDataMmap;
  MetaData psoMetaData, posMetaData;
  psoMetaData.setup(spoFilename + ".meta", ad_utility::CreateTag{});
  // auto psoCallback =
  //     [&psoMetaData](ql::span<const CompressedRelationMetadata> md) {
  //       for (const auto& m : md) {
  //         psoMetaData.add(m);
  //       }
  //     };

  // auto [numDistinctPredicates, blockData1, blockData2] =
  //     CompressedRelationWriter::createPermutationPair(
  //         psoFilename, {psoWriter, psoCallback}, {posWriter, posCallback},
  //         ad_utility::InputRangeTypeErased{std::move(sortedBlocks)},
  //         psoKeyOrder,
  //         std::vector<std::function<void(const IdTableStatic<0>&)>>{});
  // psoMetaData.blockData() = std::move(blockData1);
  // psoMetaData.calculateStatistics(numDistinctPredicates);
  // psoMetaData.setName(indexBasename);
  // {
  //   ad_utility::File psoFile(psoFilename, "r+");
  //   psoMetaData.appendToFile(&psoFile);
  // }
  // AD_LOG_INFO << "Statistics for PSO: " << psoMetaData.statistics()
  //             << std::endl;
}

}  // namespace
