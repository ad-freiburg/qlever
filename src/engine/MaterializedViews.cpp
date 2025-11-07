// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Christoph Ullinger <ullingec@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#include "engine/MaterializedViews.h"

#include <absl/strings/str_cat.h>

#include <filesystem>
#include <nlohmann/json.hpp>
#include <stdexcept>

#include "engine/QueryExecutionContext.h"
#include "engine/QueryExecutionTree.h"
#include "engine/VariableToColumnMap.h"
#include "engine/idTable/CompressedExternalIdTable.h"
#include "index/ExternalSortFunctors.h"
#include "libqlever/Qlever.h"
#include "parser/MaterializedViewQuery.h"
#include "parser/TripleComponent.h"
#include "util/AllocatorWithLimit.h"
#include "util/Exception.h"
#include "util/MemorySize/MemorySize.h"
#include "util/ProgressBar.h"

// _____________________________________________________________________________
MaterializedViewWriter::MaterializedViewWriter(
    std::string name, const qlever::Qlever::QueryPlan& queryPlan)
    : name_{std::move(name)} {
  MaterializedView::throwIfInvalidName(name_);
  auto [qet, qec, parsedQuery] = queryPlan;
  AD_CORRECTNESS_CHECK(qet != nullptr);
  AD_CORRECTNESS_CHECK(qec != nullptr);
  qet_ = qet;
  qec_ = qec;
  parsedQuery_ = std::move(parsedQuery);
}

// _____________________________________________________________________________
std::string MaterializedView::getFilenameBase(const std::string& onDiskBase,
                                              const std::string& name) {
  return absl::StrCat(onDiskBase, ".view.", name);
}

// _____________________________________________________________________________
std::string MaterializedViewWriter::getFilenameBase() const {
  return MaterializedView::getFilenameBase(qec_->getIndex().getOnDiskBase(),
                                           name_);
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
void MaterializedViewWriter::writeViewToDisk(
    ad_utility::MemorySize memoryLimit,
    ad_utility::AllocatorWithLimit<Id> allocator) const {
  // SPO comparator
  using Comparator = SortTriple<0, 1, 2>;
  // Sorter with a dynamic number of columns (template argument `NumStaticCols
  // == 0`)
  using Sorter = ad_utility::CompressedExternalIdTableSorter<Comparator, 0>;
  using MetaData = IndexMetaDataMmap;

  auto columnPermutation = getIdTableColumnPermutation();
  size_t numCols = columnPermutation.size();
  auto filename = getFilenameBase();

  // Run query
  AD_LOG_INFO << "Computing result for materialized view query " << name_
              << "..." << std::endl;
  auto result = qet_->getResult(true);
  AD_CORRECTNESS_CHECK(!result->isFullyMaterialized(),
                       "For now only lazy operations are supported as input to "
                       "the materialized view writer. The query you are "
                       "trying to index might be cached - please clear the "
                       "cache and try again.");
  auto generator = result->idTables();

  // Sort results externally
  AD_LOG_INFO << "Sorting result rows from query by first column..."
              << std::endl;
  Sorter spoSorter{filename + ".spo-sorter.dat", numCols, memoryLimit,
                   allocator};
  size_t totalTriples = 0;
  ad_utility::ProgressBar progressBar{totalTriples, "Triples processed: "};

  for (auto& [block, vocab] : generator) {
    AD_CORRECTNESS_CHECK(vocab.empty(),
                         "Materialized views cannot contain entries from a "
                         "local vocabulary currently.");
    totalTriples += block.numRows();
    // The `IdTable` may have a different column ordering from the `SELECT`
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

  qlever::KeyOrder spoKeyOrder{0, 1, 2, 3};
  MetaData spoMetaData;
  spoMetaData.setup(spoFilename + ".meta", ad_utility::CreateTag{});
  auto spoCallback =
      [&spoMetaData](ql::span<const CompressedRelationMetadata> md) {
        for (const auto& m : md) {
          spoMetaData.add(m);
        }
      };

  // TODO<ullingerc> Remove and write single permutation
  std::string sopFilename = filename + ".index.sop";
  CompressedRelationWriter sopWriter{
      numCols,
      ad_utility::File{sopFilename, "w"},
      UNCOMPRESSED_BLOCKSIZE_COMPRESSED_METADATA_PER_COLUMN,
  };
  auto sopCallback = [](ql::span<const CompressedRelationMetadata>) {
    // TODO<ullingerc> This callback is unused and should be removed once we can
    // write single permutations
  };

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

  // Export column names to view info JSON file
  std::vector<std::string> columns = ::ranges::to_vector(
      parsedQuery_.getVisibleVariables() |
      ql::views::transform([](const Variable& var) { return var.name(); }));
  nlohmann::json viewInfo = {{"version", MATERIALIZED_VIEWS_VERSION},
                             {"columns", std::move(columns)}};
  {
    std::ofstream viewInfoFile(filename + ".viewinfo.json");
    if (!viewInfoFile) {
      throw std::runtime_error(
          absl::StrCat("Cannot write ", filename, ".viewinfo.json"));
    }
    viewInfoFile << viewInfo.dump() << std::endl;
  }

  AD_LOG_INFO << "Statistics for view: " << spoMetaData.statistics()
              << std::endl;
  // TODO<ullingerc> This removes the unnecessary permutation which should not
  // be built in the first place
  std::filesystem::remove(sopFilename);
  AD_LOG_INFO << "Materialized view " << name_ << " written to disk."
              << std::endl;
}

// _____________________________________________________________________________
MaterializedView::MaterializedView(std::string onDiskBase, std::string name)
    : onDiskBase_{std::move(onDiskBase)}, name_{std::move(name)} {
  AD_CORRECTNESS_CHECK(onDiskBase_ != "",
                       "The index base filename was not set.");
  throwIfInvalidName(name_);
  AD_LOG_INFO << "Loading materialized view " << name_ << " from disk..."
              << std::endl;
  auto filename = getFilenameBase(onDiskBase_, name_);

  auto metadataFilename = absl::StrCat(filename, ".viewinfo.json");
  if (!std::filesystem::exists(metadataFilename)) {
    throw std::runtime_error(
        absl::StrCat("The materialized view '", name_, "' does not exist."));
  }

  // Read metadata from JSON
  nlohmann::json viewInfoJson;
  {
    std::ifstream viewInfoFile{metadataFilename};
    if (!viewInfoFile) {
      throw std::runtime_error(
          absl::StrCat("Error reading ", filename, ".viewinfo.json"));
    }
    viewInfoFile >> viewInfoJson;
  }

  // Check version of view and restore column names
  auto version = viewInfoJson.at("version").get<size_t>();
  AD_CORRECTNESS_CHECK(version == MATERIALIZED_VIEWS_VERSION);

  // Make variable to column map
  auto columnNames = viewInfoJson.at("columns").get<std::vector<std::string>>();
  AD_CORRECTNESS_CHECK(
      columnNames.size() >= 4,
      "Expected at least four columns in materialized view metadata");
  for (const auto& [index, columnName] :
       ::ranges::views::enumerate(columnNames)) {
    varToColMap_.insert({Variable{columnName},
                         {static_cast<ColumnIndex>(index),
                          ColumnIndexAndTypeInfo::PossiblyUndefined}});
  }
  indexedColVariable_ = Variable{columnNames.at(0)};

  // Read permutation
  permutation_->loadFromDisk(
      filename, [](Id) { return false; }, false);
  AD_CORRECTNESS_CHECK(permutation_->isLoaded());
}

// _____________________________________________________________________________
std::shared_ptr<const Permutation> MaterializedView::getPermutation() const {
  AD_CORRECTNESS_CHECK(permutation_ != nullptr);
  return permutation_;
}

// _____________________________________________________________________________
void MaterializedViewsManager::loadView(const std::string& name) const {
  if (loadedViews_.contains(name)) {
    return;
  }
  loadedViews_.insert(
      {name, std::make_shared<MaterializedView>(onDiskBase_, name)});
};

// _____________________________________________________________________________
std::shared_ptr<const MaterializedView> MaterializedViewsManager::getView(
    const std::string& name) const {
  loadView(name);
  return loadedViews_.at(name);
}

// _____________________________________________________________________________
SparqlTripleSimple MaterializedView::makeScanConfig(
    const parsedQuery::MaterializedViewQuery& viewQuery,
    Variable placeholderPredicate, Variable placeholderObject) const {
  AD_CORRECTNESS_CHECK(viewQuery.viewName_ == name_);
  if (viewQuery.childGraphPattern_.has_value()) {
    throw MaterializedViewConfigException(
        "A materialized view query may not have a child group graph pattern.");
  }

  if (!viewQuery.scanCol_.has_value()) {
    throw MaterializedViewConfigException(
        "You must set a variable, IRI or literal for the scan column of a "
        "materialized view.");
  }

  auto s = viewQuery.scanCol_.value();
  std::optional<Variable> scanColVar =
      s.isVariable() ? std::optional{s.getVariable()} : std::nullopt;
  AD_CORRECTNESS_CHECK(
      placeholderPredicate != placeholderObject,
      "Placeholders for predicate and object must not be the same variable");
  TripleComponent p{std::move(placeholderPredicate)};
  TripleComponent o{std::move(placeholderObject)};
  AdditionalScanColumns additionalCols;

  // Assemble which columns should be bound to which variables
  ad_utility::HashSet<Variable> uniqueTargetVar;
  for (const auto& [viewVar, targetVar] : viewQuery.requestedVariables_) {
    if (!varToColMap_.contains(viewVar)) {
      throw MaterializedViewConfigException(absl::StrCat(
          "The column '", viewVar.name(),
          "' does not exist in the materialized view '", name_, "'."));
    }
    if (uniqueTargetVar.contains(targetVar)) {
      throw MaterializedViewConfigException(
          absl::StrCat("Each target variable for a payload column may only be "
                       "associated with one column. However  '",
                       targetVar.name(), "' was requested multiple times."));
    }
    if (scanColVar.has_value() && scanColVar.value() == targetVar) {
      throw MaterializedViewConfigException(absl::StrCat(
          "The variable for the scan column of a materialized "
          "view may not also be used for a payload column, but '",
          scanColVar.value().name(), "' violated this requirement."));
    }
    auto colIdx = varToColMap_.at(viewVar).columnIndex_;
    if (colIdx == 0) {
      throw MaterializedViewConfigException(absl::StrCat(
          "The scan column of a materialized view may not be requested as "
          "payload, but '",
          scanColVar.value().name(), "' violated this requirement."));
    } else if (colIdx == 1) {
      p = targetVar;
    } else if (colIdx == 2) {
      o = targetVar;
    } else {
      AD_CORRECTNESS_CHECK(colIdx > 2);
      additionalCols.emplace_back(colIdx, targetVar);
    }
    uniqueTargetVar.insert(targetVar);
  }

  // Additional columns must be sorted (required by internals of `IndexScan`)
  std::sort(additionalCols.begin(), additionalCols.end(),
            [](const auto& a, const auto& b) { return a.first < b.first; });

  return {s, p, o, additionalCols};
}

namespace string_constants::detail {
static constexpr auto validViewName = ctll::fixed_string{R"(^[a-zA-Z0-9\-]+$)"};
}

// _____________________________________________________________________________
bool MaterializedView::isValidName(const std::string& name) {
  return ctre::match<string_constants::detail::validViewName>(name);
}

// _____________________________________________________________________________
void MaterializedView::throwIfInvalidName(const std::string& name) {
  if (!MaterializedView::isValidName(name)) {
    throw MaterializedViewConfigException(
        absl::StrCat("'", name,
                     "' is not a valid name for a materialized view. Only "
                     "alphanumeric characters and hyphens are allowed."));
  }
}
