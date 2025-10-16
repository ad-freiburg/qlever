// Copyright 2025, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Hannah Bast <bast@cs.uni-freiburg.de>
//          Claude Code <noreply@anthropic.com>
//
// Standalone program to rebuild the last permutation pair (PSO and POS) from
// the OSP permutation. This is useful when the index build crashes while
// creating the last permutation pair, leaving corrupt files. The alternative
// would be to rebuild the entire index from scratch, which is very time-
// consuming when the dataset is large.

#include <boost/program_options.hpp>
#include <cstdlib>
#include <exception>
#include <iostream>
#include <string>

#include "CompilationInfo.h"
#include "engine/LocalVocab.h"
#include "engine/idTable/CompressedExternalIdTable.h"
#include "global/Constants.h"
#include "index/CompressedRelation.h"
#include "index/ConstantsIndexBuilding.h"
#include "index/DeltaTriples.h"
#include "index/ExternalSortFunctors.h"
#include "index/IndexMetaData.h"
#include "index/LocatedTriples.h"
#include "index/Permutation.h"
#include "index/ScanSpecification.h"
#include "util/CancellationHandle.h"
#include "util/InputRangeUtils.h"
#include "util/Log.h"
#include "util/ProgressBar.h"
#include "util/ReadableNumberFacet.h"

namespace po = boost::program_options;

using namespace ad_utility::memory_literals;

// Number of columns with patterns (S, P, O, Graph, SubjPattern, ObjPattern)
constexpr size_t NUM_COLUMNS_WITH_PATTERNS = NumColumnsIndexBuilding + 2;

int main(int argc, char** argv) {
  // Set up logging.
  setlocale(LC_CTYPE, "");
  std::locale loc;
  ad_utility::ReadableNumberFacet facet(1);
  std::locale locWithNumberGrouping(loc, &facet);
  ad_utility::Log::imbue(locWithNumberGrouping);

  // Parse command line arguments.
  std::string indexBasename;
  po::options_description options("Rebuild PSO and POS permutations from OSP");
  options.add_options()("help,h", "Produce this help message")(
      "index,i", po::value(&indexBasename)->required(),
      "The basename of the index (without .index.osp suffix)");
  po::variables_map optionsMap;
  try {
    po::store(po::parse_command_line(argc, argv, options), optionsMap);

    if (optionsMap.count("help")) {
      std::cout << "Rebuild PSO and POS permutations from OSP permutation\n\n";
      std::cout << options << '\n';
      std::cout << "\nExample usage:\n";
      std::cout << "  " << argv[0] << " -i /path/to/index\n\n";
      std::cout << "This will read all triples from OSP, sort them by PSO, "
                << "and create both PSO and POS permutations.\n";
      return EXIT_SUCCESS;
    }

    po::notify(optionsMap);
  } catch (const std::exception& e) {
    std::cerr << "Error in command-line argument: " << e.what() << '\n';
    std::cerr << options << '\n';
    return EXIT_FAILURE;
  }

  // Header message.
  AD_LOG_INFO << EMPH_ON << "QLever RebuildPsoAndPos, compiled on "
              << qlever::version::DatetimeOfCompilation << " using git hash "
              << qlever::version::GitShortHash << EMPH_OFF << std::endl;

  // Load the OSP permutation from disk.
  AD_LOG_INFO << "Loading OSP permutation from " << indexBasename + ".index.osp"
              << std::endl;
  auto allocator = ad_utility::makeUnlimitedAllocator<Id>();
  Permutation ospPermutation(Permutation::OSP, allocator);
  auto isInternalId = [](Id) { return false; };  // No internal IDs for now
  ospPermutation.loadFromDisk(indexBasename, isInternalId, false);

  // Create a generator that reads all triples from OSP,
  //
  // NOTE: At the core, we just need a `lazyScan` here. However, as
  // prerequisites, we need a `ScanSpecification`, a `LocatedTriplesSnapshot`,
  // and a `cancellationHandle`.
  AD_LOG_DEBUG << "Creating generator for reading triples from OSP..."
               << std::endl;
  ScanSpecification fullScan(std::nullopt, std::nullopt, std::nullopt);
  LocatedTriplesPerBlockAllPermutations emptyLocatedTriples;
  emptyLocatedTriples[static_cast<size_t>(Permutation::OSP)]
      .setOriginalMetadata(ospPermutation.metaData().blockDataShared());
  LocalVocab emptyVocab;
  LocatedTriplesSnapshot emptySnapshot{emptyLocatedTriples,
                                       emptyVocab.getLifetimeExtender(), 0};
  auto scanSpecAndBlocks =
      ospPermutation.getScanSpecAndBlocks(fullScan, emptySnapshot);
  auto cancellationHandle =
      std::make_shared<ad_utility::CancellationHandle<>>();
  std::vector<ColumnIndex> additionalColumns{
      ColumnIndex{ADDITIONAL_COLUMN_GRAPH_ID},
      ColumnIndex{ADDITIONAL_COLUMN_INDEX_SUBJECT_PATTERN},
      ColumnIndex{ADDITIONAL_COLUMN_INDEX_OBJECT_PATTERN}};
  auto generator = ospPermutation.lazyScan(scanSpecAndBlocks, std::nullopt,
                                           additionalColumns,
                                           cancellationHandle, emptySnapshot);

  // Feed triples into a sorter to sort them by PSO. The sorter will sort each
  // block as it's pushed and write it to disk, then later merge the sorted
  // blocks when we call getSortedBlocks().
  //
  // NOTE: OSP returns data in [O, S, P, G, S-Pattern, O-Pattern] column order.
  // To sort for PSO, we use SortTriple<2, 1, 0>.
  AD_LOG_INFO << "Reading triples and pushing to PSO sorter ..." << std::endl;
  using PsoSorterFromOsp =
      ad_utility::CompressedExternalIdTableSorter<SortTriple<2, 1, 0>,
                                                  NUM_COLUMNS_WITH_PATTERNS>;
  auto memoryLimit = 16_GB;
  PsoSorterFromOsp psoSorter(indexBasename + ".pso-rebuild-sorter.dat",
                             NUM_COLUMNS_WITH_PATTERNS, memoryLimit, allocator);
  size_t totalTriples = 0;
  ad_utility::ProgressBar progressBar{totalTriples, "Triples read: "};
  for (auto& block : generator) {
    totalTriples += block.numRows();
    psoSorter.pushBlock(block);
    if (progressBar.update()) {
      AD_LOG_INFO << progressBar.getProgressString() << std::flush;
    }
  }
  AD_LOG_INFO << progressBar.getFinalProgressString() << std::flush;

  // Write the PSO and POS permutations.
  AD_LOG_INFO << "Creating permutations PSO and POS ..." << std::endl;
  auto sortedBlocksOSP = psoSorter.template getSortedBlocks<0>();
  std::array<ColumnIndex, NUM_COLUMNS_WITH_PATTERNS> columnPermutation{
      ColumnIndex{1}, ColumnIndex{2}, ColumnIndex{0},
      ColumnIndex{3}, ColumnIndex{4}, ColumnIndex{5}};
  auto permuteBlock = [&columnPermutation](auto&& block) {
    block.setColumnSubset(columnPermutation);
    return std::move(block);
  };
  auto sortedBlocks = ad_utility::CachingTransformInputRange{
      ad_utility::OwningView{std::move(sortedBlocksOSP)}, permuteBlock};
  std::string psoFilename = indexBasename + ".index.pso";
  std::string posFilename = indexBasename + ".index.pos";
  CompressedRelationWriter psoWriter{
      NUM_COLUMNS_WITH_PATTERNS, ad_utility::File(psoFilename, "w"),
      UNCOMPRESSED_BLOCKSIZE_COMPRESSED_METADATA_PER_COLUMN};
  CompressedRelationWriter posWriter{
      NUM_COLUMNS_WITH_PATTERNS, ad_utility::File(posFilename, "w"),
      UNCOMPRESSED_BLOCKSIZE_COMPRESSED_METADATA_PER_COLUMN};

  // Write the metadata for PSO and POS.
  AD_LOG_DEBUG << "Writing metadata ..." << std::endl;
  qlever::KeyOrder psoKeyOrder{1, 0, 2, 3};
  using MetaData = IndexMetaDataMmap;
  MetaData psoMetaData, posMetaData;
  psoMetaData.setup(psoFilename + ".meta", ad_utility::CreateTag{});
  posMetaData.setup(posFilename + ".meta", ad_utility::CreateTag{});
  auto psoCallback =
      [&psoMetaData](ql::span<const CompressedRelationMetadata> md) {
        for (const auto& m : md) {
          psoMetaData.add(m);
        }
      };
  auto posCallback =
      [&posMetaData](ql::span<const CompressedRelationMetadata> md) {
        for (const auto& m : md) {
          posMetaData.add(m);
        }
      };
  auto [numDistinctPredicates, blockData1, blockData2] =
      CompressedRelationWriter::createPermutationPair(
          psoFilename, {psoWriter, psoCallback}, {posWriter, posCallback},
          ad_utility::InputRangeTypeErased{std::move(sortedBlocks)},
          psoKeyOrder,
          std::vector<std::function<void(const IdTableStatic<0>&)>>{});
  psoMetaData.blockData() = std::move(blockData1);
  posMetaData.blockData() = std::move(blockData2);
  psoMetaData.calculateStatistics(numDistinctPredicates);
  posMetaData.calculateStatistics(numDistinctPredicates);
  psoMetaData.setName(indexBasename);
  posMetaData.setName(indexBasename);
  {
    ad_utility::File psoFile(psoFilename, "r+");
    psoMetaData.appendToFile(&psoFile);
  }
  {
    ad_utility::File posFile(posFilename, "r+");
    posMetaData.appendToFile(&posFile);
  }
  AD_LOG_INFO << "Statistics for PSO: " << psoMetaData.statistics()
              << std::endl;
  AD_LOG_INFO << "Statistics for POS: " << posMetaData.statistics()
              << std::endl;

  // Done.
  AD_LOG_INFO << "Rebuilding of PSO and POS from OSP completed" << std::endl;
}
