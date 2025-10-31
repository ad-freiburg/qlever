// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Christoph Ullinger <ullingec@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#include "engine/MaterializedView.h"

#include "engine/idTable/CompressedExternalIdTable.h"
#include "index/ExternalSortFunctors.h"
#include "libqlever/Qlever.h"
#include "util/Exception.h"
#include "util/MemorySize/MemorySize.h"
#include "util/ProgressBar.h"

// _____________________________________________________________________________
MaterializedViewWriter::MaterializedViewWriter(
    std::string name, qlever::Qlever::QueryPlan queryPlan)
    : name_{name} {
  auto [qet, qec, parsedQuery] = queryPlan;
  AD_CORRECTNESS_CHECK(qet != nullptr);
  AD_CORRECTNESS_CHECK(qec != nullptr);
  qet_ = qet;
  qec_ = qec;
  parsedQuery_ = std::move(parsedQuery);
}

// _____________________________________________________________________________
std::string MaterializedViewWriter::getFilenameBase() const {
  return absl::StrCat(qec_->getIndex().getOnDiskBase(), ".view." + name_);
}

// _____________________________________________________________________________
std::vector<ColumnIndex> MaterializedViewWriter::getIdTableColumnPermutation()
    const {
  auto targetVars = parsedQuery_.getVisibleVariables();
  auto numCols = targetVars.size();
  AD_CONTRACT_CHECK(targetVars.size() >= 4,
                    "Currently the query used to write a materialized view "
                    "needs to have at least four columns.");

  std::vector<ColumnIndex> columnPermutation;
  columnPermutation.reserve(numCols);
  for (const auto& var : targetVars) {
    columnPermutation.push_back(qet_->getVariableColumn(var));
  }
  return columnPermutation;
}

// _____________________________________________________________________________
void MaterializedViewWriter::writeViewToDisk() {
  auto columnPermutation = getIdTableColumnPermutation();
  size_t numCols = columnPermutation.size();
  auto filename = getFilenameBase();

  // Run query
  AD_LOG_INFO << "Computing result for materialized view query " << name_
              << "..." << std::endl;
  auto result = qet_->getResult(true);
  AD_CORRECTNESS_CHECK(!result->isFullyMaterialized(),
                       "For now only lazy operations are supported as input to "
                       "the materialized view writer");
  auto generator = result->idTables();

  auto memoryLimit = ad_utility::MemorySize::gigabytes(16);   // TODO
  auto allocator = ad_utility::makeUnlimitedAllocator<Id>();  // TODO
  Sorter spoSorter{filename + ".spo-sorter.dat", numCols, memoryLimit,
                   allocator};

  // Sort results externally
  AD_LOG_INFO << "Sorting result rows from query by first column..."
              << std::endl;
  size_t totalTriples = 0;
  ad_utility::ProgressBar progressBar{totalTriples, "Triples processed: "};

  for (auto& [block, vocab] : generator) {
    AD_CORRECTNESS_CHECK(vocab.empty(),
                         "Materialized views cannot contain entries from a "
                         "local vocabulary currently.");
    totalTriples += block.numRows();
    // The IdTable may have a different column ordering from the SELECT
    // statement, thus we must permute this to the column ordering we want to
    // have in our materialized view. In particular, the indexed column should
    // be the first.
    block.setColumnSubset(columnPermutation);
    spoSorter.pushBlock(block);
    if (progressBar.update()) {
      AD_LOG_INFO << progressBar.getProgressString() << std::flush;
    }
  }
  AD_LOG_INFO << progressBar.getFinalProgressString() << std::flush;

  // Write compressed relation to disk
  AD_LOG_INFO << "Writing materialized view " << name_ << " to disk ..."
              << std::endl;
  auto sortedBlocksSPO = spoSorter.template getSortedBlocks<0>();
  std::string spoFilename = filename + ".index.spo";
  CompressedRelationWriter spoWriter{
      numCols,
      ad_utility::File{spoFilename, "w"},
      UNCOMPRESSED_BLOCKSIZE_COMPRESSED_METADATA_PER_COLUMN,
  };

  // ----
  std::string sopFilename = filename + ".index.sop";
  CompressedRelationWriter sopWriter{
      numCols,
      ad_utility::File{sopFilename, "w"},
      UNCOMPRESSED_BLOCKSIZE_COMPRESSED_METADATA_PER_COLUMN,
  };
  // ---/---

  qlever::KeyOrder spoKeyOrder{0, 1, 2, 3};
  MetaData spoMetaData;
  spoMetaData.setup(spoFilename + ".meta", ad_utility::CreateTag{});
  auto spoCallback =
      [&spoMetaData](ql::span<const CompressedRelationMetadata> md) {
        for (const auto& m : md) {
          spoMetaData.add(m);
        }
      };

  // ----
  auto sopCallback = [&](ql::span<const CompressedRelationMetadata>) {};
  // ---/---

  auto [numDistinctPredicates, blockData1, blockData2] =
      CompressedRelationWriter::createPermutationPair(
          spoFilename + ".sorter", {spoWriter, spoCallback},
          {sopWriter, sopCallback},
          ad_utility::InputRangeTypeErased{std::move(sortedBlocksSPO)},
          spoKeyOrder, {});

  AD_LOG_DEBUG << "Writing metadata ..." << std::endl;
  spoMetaData.blockData() = std::move(blockData1);
  spoMetaData.calculateStatistics(numDistinctPredicates);
  spoMetaData.setName(filename);
  {
    ad_utility::File spoFile(spoFilename, "r+");
    spoMetaData.appendToFile(&spoFile);
  }

  AD_LOG_INFO << "Statistics for view: " << spoMetaData.statistics()
              << std::endl;
  // TODO<ullingerc> This removes the unnecessary permutation which should not
  // be built in the first place
  std::filesystem::remove(sopFilename);
  AD_LOG_INFO << "Materialized view " << name_ << " written to disk."
              << std::endl;
}
