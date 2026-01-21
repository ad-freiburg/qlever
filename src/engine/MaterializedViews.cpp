// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Christoph Ullinger <ullingec@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#include "engine/MaterializedViews.h"

#include <absl/strings/str_cat.h>

#include <filesystem>
#include <memory>
#include <nlohmann/json.hpp>
#include <stdexcept>

#include "engine/IndexScan.h"
#include "engine/QueryExecutionContext.h"
#include "engine/QueryExecutionTree.h"
#include "engine/VariableToColumnMap.h"
#include "engine/idTable/CompressedExternalIdTable.h"
#include "index/DeltaTriples.h"
#include "index/ExternalSortFunctors.h"
#include "libqlever/Qlever.h"
#include "parser/MaterializedViewQuery.h"
#include "parser/TripleComponent.h"
#include "util/AllocatorWithLimit.h"
#include "util/Exception.h"
#include "util/MemorySize/MemorySize.h"
#include "util/ProgressBar.h"
#include "util/Views.h"

// _____________________________________________________________________________
MaterializedViewWriter::MaterializedViewWriter(
    std::string onDiskBase, std::string name,
    const qlever::Qlever::QueryPlan& queryPlan,
    ad_utility::MemorySize memoryLimit,
    ad_utility::AllocatorWithLimit<Id> allocator)
    : onDiskBase_{std::move(onDiskBase)},
      name_{std::move(name)},
      memoryLimit_{std::move(memoryLimit)},
      allocator_{std::move(allocator)} {
  MaterializedView::throwIfInvalidName(name_);
  auto [qet, qec, parsedQuery] = queryPlan;
  AD_CORRECTNESS_CHECK(qet != nullptr);
  AD_CORRECTNESS_CHECK(qec != nullptr);
  qet_ = qet;
  qec_ = qec;
  parsedQuery_ = std::move(parsedQuery);
  auto [columnNamesAndPermutation, numAddEmptyColumns] =
      getIdTableColumnNamesAndPermutation();
  columnNames_ = ::ranges::to<std::vector<Variable>>(columnNamesAndPermutation |
                                                     ql::views::keys);
  columnPermutation_ = ::ranges::to<std::vector<ColumnIndex>>(
      columnNamesAndPermutation | ql::views::values);
  numAddEmptyColumns_ = numAddEmptyColumns;
}

// _____________________________________________________________________________
void MaterializedViewWriter::writeViewToDisk(
    std::string onDiskBase, std::string name,
    const qlever::Qlever::QueryPlan& queryPlan,
    ad_utility::MemorySize memoryLimit,
    ad_utility::AllocatorWithLimit<Id> allocator) {
  MaterializedViewWriter writer{std::move(onDiskBase), std::move(name),
                                queryPlan, std::move(memoryLimit),
                                std::move(allocator)};
  writer.computeResultAndWritePermutation();
}

// _____________________________________________________________________________
std::string MaterializedView::getFilenameBase(std::string_view onDiskBase,
                                              std::string_view name) {
  return absl::StrCat(onDiskBase, ".view.", name);
}

// _____________________________________________________________________________
std::string MaterializedViewWriter::getFilenameBase() const {
  return MaterializedView::getFilenameBase(onDiskBase_, name_);
}

// _____________________________________________________________________________
MaterializedViewWriter::ColumnNamesAndPermutation
MaterializedViewWriter::getIdTableColumnNamesAndPermutation() const {
  AD_CONTRACT_CHECK(
      parsedQuery_.hasSelectClause(),
      "Materialized views may only be built from 'SELECT' statements. "
      "'CONSTRUCT', 'ASK' and update queries are not allowed.");

  auto targetVarsAndCols =
      qet_->selectedVariablesToColumnIndices(parsedQuery_.selectClause());
  const size_t numCols = targetVarsAndCols.size();

  // Column information for the columns selected by the user's query.
  auto existingCols = ::ranges::to<std::vector<ColumnNameAndIndex>>(
      targetVarsAndCols | ql::views::transform([](const auto& opt) {
        AD_CONTRACT_CHECK(opt.has_value());
        return ColumnNameAndIndex{opt.value().variable_,
                                  opt.value().columnIndex_};
      }));

  // Add dummy columns such that the view has at least four columns in total.
  uint8_t numAddEmptyCols = 0;
  if (numCols < 4) {
    AD_LOG_INFO << "The query to write the materialized view '" << name_
                << "' selects only " << numCols << " column(s). " << 4 - numCols
                << " empty column(s) will be appended." << std::endl;
    numAddEmptyCols = 4 - numCols;
  }

  return {std::move(existingCols), numAddEmptyCols};
}

// _____________________________________________________________________________
void MaterializedViewWriter::permuteIdTableAndCheckNoLocalVocabEntries(
    IdTable& block) const {
  // The `IdTable` may have a different column ordering from the
  // `SELECT` statement, thus we must permute this to the column
  // ordering we want to have in our materialized view. In
  // particular, the indexed column should be the first.
  block.setColumnSubset(columnPermutation_);

  // Add empty columns such that the view has at least four columns.
  for (uint8_t i = 0; i < numAddEmptyColumns_; ++i) {
    block.addEmptyColumn();
    // Initialize the new empty column to `UNDEF` (all bits zero) such that it
    // can be compressed optimally.
    const size_t col = block.numColumns() - 1;
    for (size_t row = 0; row < block.numRows(); ++row) {
      block.at(row, col) = ValueId::makeUndefined();
    }
  }

  // Check that there are no values of type `LocalVocabIndex` in the selected
  // columns of the `IdTable` as materialized views do not support them as of
  // now.
  bool hasLocalVocab =
      ql::ranges::any_of(block.getColumns(), [](const auto& col) {
        return ql::ranges::any_of(col, [](ValueId id) {
          return id.getDatatype() == Datatype::LocalVocabIndex;
        });
      });
  if (hasLocalVocab) {
    throw std::runtime_error{
        "The query to write a materialized view returned a string not "
        "contained in the index (local vocabulary entry). This could be "
        "the result of a string-related function in your query or the "
        "presence of SPARQL UPDATEs in this instance of Qlever. Both are "
        "currently not supported in materialized views."};
  }
}

// _____________________________________________________________________________
MaterializedViewWriter::RangeOfIdTables
MaterializedViewWriter::getBlocksForAlreadySortedResult(
    std::shared_ptr<const Result> result) const {
  // Results are already sorted correctly: we do not need to invoke the
  // external sorter, but we still need to permute the `IdTable`s to the
  // desired column ordering and construct a range for the
  // `CompressedRelationWriter` from them.
  AD_LOG_INFO << "Query result rows for materialized view " << name_
              << " are already sorted." << std::endl;

  if (result->isFullyMaterialized()) {
    // If we have a fully materialized result, we need to copy it for the
    // necessary modifications (permuting columns).
    IdTable idTableCopyForPermutation = result->idTable().clone();
    permuteIdTableAndCheckNoLocalVocabEntries(idTableCopyForPermutation);
    std::vector<IdTableStatic<0>> singleIdTable;
    singleIdTable.push_back(std::move(idTableCopyForPermutation));
    return RangeOfIdTables{std::move(singleIdTable)};
  } else {
    // Transform the lazy result (permute columns)
    return RangeOfIdTables{
        ad_utility::OwningView{result->idTables()} |
        ql::views::transform(
            [&](auto& idTableAndLocalVocab) -> IdTableStatic<0> {
              auto& [block, vocab] = idTableAndLocalVocab;
              permuteIdTableAndCheckNoLocalVocabEntries(block);
              return std::move(block);
            })};
  }
}

// _____________________________________________________________________________
MaterializedViewWriter::RangeOfIdTables
MaterializedViewWriter::getBlocksForUnsortedResult(
    Sorter& spoSorter, std::shared_ptr<const Result> result) const {
  // Results are not yet sorted by the required columns. Sort results
  // externally.
  AD_LOG_INFO << "Sorting query result rows for materialized view " << name_
              << " ..." << std::endl;
  size_t totalTriples = 0;
  ad_utility::ProgressBar progressBar{totalTriples, "Triples sorted: "};

  auto processBlock = [&](IdTable& block) {
    permuteIdTableAndCheckNoLocalVocabEntries(block);
    totalTriples += block.numRows();
    spoSorter.pushBlock(block);
    if (progressBar.update()) {
      AD_LOG_INFO << progressBar.getProgressString() << std::flush;
    }
  };

  if (result->isFullyMaterialized()) {
    // If we have a fully materialized result, this is const, so we need to
    // copy it for the necessary modifications (permuting columns).
    // TODO<ullingerc> This could be avoided if
    // `CompressedExternalIdTableSorter::pushBlock` would also accept
    // `IdTableView`.
    IdTable idTableCopyForPermutation = result->idTable().clone();
    processBlock(idTableCopyForPermutation);
  } else {
    // Process lazy result blockwise
    auto generator = result->idTables();
    for (auto& [block, vocab] : generator) {
      processBlock(block);
    }
  }

  AD_LOG_INFO << progressBar.getFinalProgressString() << std::flush;
  return spoSorter.template getSortedBlocks<0>();
}

// _____________________________________________________________________________
MaterializedViewWriter::RangeOfIdTables MaterializedViewWriter::getSortedBlocks(
    Sorter& spoSorter, std::shared_ptr<const Result> result) const {
  // Check if the query result is already sorted by SPO considering the target
  // column ordering
  constexpr size_t numSortedColumns = 3;
  const auto& resultSortedBy = result->sortedBy();
  bool isAlreadySorted =
      ql::ranges::equal(resultSortedBy | ql::views::take(numSortedColumns),
                        columnPermutation_ | ql::views::take(numSortedColumns));

  // Either call the version that only permutes the correctly sorted blocks or
  // the version that sorts them.
  if (isAlreadySorted) {
    return getBlocksForAlreadySortedResult(result);
  } else {
    return getBlocksForUnsortedResult(spoSorter, result);
  }
}

// _____________________________________________________________________________
IndexMetaDataMmap MaterializedViewWriter::writePermutation(
    RangeOfIdTables sortedBlocksSPO) const {
  std::string spoFilename = getFilenameBase() + ".index.spo";
  CompressedRelationWriter spoWriter{
      numCols(),
      ad_utility::File{spoFilename, "w"},
      UNCOMPRESSED_BLOCKSIZE_COMPRESSED_METADATA_PER_COLUMN,
  };

  qlever::KeyOrder spoKeyOrder{0, 1, 2, 3};
  IndexMetaDataMmap spoMetaData;
  spoMetaData.setup(spoFilename + ".meta", ad_utility::CreateTag{});
  auto spoCallback =
      [&spoMetaData](ql::span<const CompressedRelationMetadata> md) {
        for (const auto& m : md) {
          spoMetaData.add(m);
        }
      };

  auto [numDistinctPredicates, blockData] =
      CompressedRelationWriter::createPermutation(
          {spoWriter, spoCallback},
          ad_utility::InputRangeTypeErased{std::move(sortedBlocksSPO)},
          spoKeyOrder, {});

  AD_LOG_DEBUG << "Writing metadata ..." << std::endl;
  spoMetaData.blockData() = std::move(blockData);
  spoMetaData.calculateStatistics(numDistinctPredicates);
  spoMetaData.setName(getFilenameBase());
  {
    ad_utility::File spoFile(spoFilename, "r+");
    spoMetaData.appendToFile(&spoFile);
  }

  return spoMetaData;
}

// _____________________________________________________________________________
void MaterializedViewWriter::writeViewMetadata() const {
  // Export column names to view info JSON file
  nlohmann::json viewInfo = {
      {"version", MATERIALIZED_VIEWS_VERSION},
      {"columns", (columnNames_ | ql::views::transform([](const Variable& v) {
                     return v.name();
                   }) |
                   ::ranges::to<std::vector<std::string>>())}};
  ad_utility::makeOfstream(getFilenameBase() + ".viewinfo.json")
      << viewInfo.dump() << std::endl;
}

// _____________________________________________________________________________
void MaterializedViewWriter::computeResultAndWritePermutation() const {
  // Run query and sort the result externally (only if necessary)
  AD_LOG_INFO << "Computing result for materialized view query " << name_
              << "..." << std::endl;
  auto result = qet_->getResult(true);

  Sorter spoSorter{getFilenameBase() + ".spo-sorter.dat", numCols(),
                   memoryLimit_, allocator_};
  RangeOfIdTables sortedBlocksSPO = getSortedBlocks(spoSorter, result);

  // Write compressed relation to disk
  AD_LOG_INFO << "Writing materialized view " << name_ << " to disk ..."
              << std::endl;
  auto spoMetaData = writePermutation(std::move(sortedBlocksSPO));
  writeViewMetadata();

  AD_LOG_INFO << "Statistics for view " << name_ << ": "
              << spoMetaData.statistics() << std::endl;
  AD_LOG_INFO << "Materialized view " << name_ << " written to disk."
              << std::endl;
}

// _____________________________________________________________________________
MaterializedView::MaterializedView(std::string onDiskBase, std::string name)
    : onDiskBase_{std::move(onDiskBase)},
      name_{std::move(name)},
      locatedTriplesState_{makeEmptyLocatedTriplesState()} {
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
  ad_utility::makeIfstream(metadataFilename) >> viewInfoJson;

  // Check version of view and restore column names
  auto version = viewInfoJson.at("version").get<size_t>();
  AD_CORRECTNESS_CHECK(version == MATERIALIZED_VIEWS_VERSION);

  // Make variable to column map
  auto columnNames = viewInfoJson.at("columns").get<std::vector<std::string>>();
  for (const auto& [index, columnName] :
       ::ranges::views::enumerate(columnNames)) {
    varToColMap_.insert({Variable{columnName},
                         {static_cast<ColumnIndex>(index),
                          ColumnIndexAndTypeInfo::PossiblyUndefined}});
  }

  // Read permutation
  permutation_->loadFromDisk(filename, false);
  AD_CORRECTNESS_CHECK(permutation_->isLoaded());
}

// _____________________________________________________________________________
std::shared_ptr<const Permutation> MaterializedView::permutation() const {
  AD_CORRECTNESS_CHECK(permutation_ != nullptr);
  return permutation_;
}

// _____________________________________________________________________________
void MaterializedViewsManager::loadView(const std::string& name) const {
  auto lock = loadedViews_.wlock();
  if (lock->contains(name)) {
    return;
  }
  lock->insert({name, std::make_shared<MaterializedView>(onDiskBase_, name)});
};

// _____________________________________________________________________________
std::shared_ptr<const MaterializedView> MaterializedViewsManager::getView(
    const std::string& name) const {
  loadView(name);
  return loadedViews_.rlock()->at(name);
}

// _____________________________________________________________________________
void MaterializedView::throwIfScanColumnMissing(
    const std::optional<TripleComponent>& s) const {
  // The scan column must be set.
  if (!s.has_value()) {
    throw MaterializedViewConfigException(
        "The first column of a materialized view must always be read to a "
        "variable or restricted to a fixed value.");
  }
}

// _____________________________________________________________________________
void MaterializedView::throwIfColumnsHaveIllegalFixedValues(
    const std::optional<TripleComponent>& s, const TripleComponent& p,
    const TripleComponent& o) const {
  // Not all versions of scan specifications are allowed because a view only has
  // one permutation: fixed values are only allowed in these arrangements:
  // subject, subject + predicate, subject + predicate + object.
  auto sIsVar = s.value().isVariable();
  auto pIsVar = p.isVariable();
  auto oIsVar = o.isVariable();
  if (!pIsVar && sIsVar) {
    throw MaterializedViewConfigException(
        "When setting the second column of a materialized view to a fixed "
        "value, the first column must also be fixed.");
  }
  if (!oIsVar && (pIsVar || sIsVar)) {
    throw MaterializedViewConfigException(
        "When setting the third column of a materialized view to a fixed "
        "value, the first two columns must also be fixed.");
  }
}

// _____________________________________________________________________________
void MaterializedView::throwIfColumnNotInView(const Variable& column) const {
  if (!varToColMap_.contains(column)) {
    throw MaterializedViewConfigException(absl::StrCat(
        "The column '", column.name(),
        "' does not exist in the materialized view '", name_, "'."));
  }
}

// _____________________________________________________________________________
void MaterializedView::throwIfAdditionalColumnIsNotVariable(
    const Variable& column, const TripleComponent& value) const {
  if (!value.isVariable()) {
    throw MaterializedViewConfigException(absl::StrCat(
        "Currently only the first three columns of a materialized view may "
        "be restricted to fixed values. All other columns must be "
        "variables, but column '",
        column.name(), "' was fixed to '", value.toString(), "'."));
  }
}

// _____________________________________________________________________________
void MaterializedView::throwIfScanColumnIsSetTwice(
    const std::optional<TripleComponent>& s,
    const TripleComponent& value) const {
  if (s.has_value()) {
    throw MaterializedViewConfigException(absl::StrCat(
        "The first column of a materialized view may not be requested "
        "twice, but '",
        value.toString(), "' violated this requirement."));
  }
}

// _____________________________________________________________________________
void MaterializedView::throwIfVariableUsedTwice(
    const ad_utility::HashSet<Variable>& variablesSeen,
    const TripleComponent& target) const {
  if (target.isVariable() && variablesSeen.contains(target.getVariable())) {
    throw MaterializedViewConfigException(
        absl::StrCat("Each target variable for a reading from a materialized "
                     "view may only be associated with one column. However '",
                     target.toString(), "' was requested multiple times."));
  }
}

// _____________________________________________________________________________
SparqlTripleSimple MaterializedView::makeScanConfig(
    const parsedQuery::MaterializedViewQuery& viewQuery) const {
  AD_CORRECTNESS_CHECK(viewQuery.viewName_ == name_);
  if (viewQuery.childGraphPattern_.has_value()) {
    throw MaterializedViewConfigException(
        "A materialized view query may not have a child group graph pattern.");
  }

  // If `scanCol_` is set (when using the magic predicate), fix the subject to
  // this. Otherwise the subject is determined from `requestedColumns_` below.
  std::optional<TripleComponent> s = viewQuery.scanCol_;
  // The placeholders are immediately removed from the result by column
  // stripping. Therefore their names are not a concern when a single query
  // contains multiple instances of `MaterializedViewQuery`.
  TripleComponent p{Variable{"?_ql_materialized_view_p"}};
  TripleComponent o{Variable{"?_ql_materialized_view_o"}};
  AdditionalScanColumns additionalCols;

  // Assemble which columns should be bound to which variables
  ad_utility::HashSet<Variable> variablesSeen;
  for (const auto& [viewVar, target] : viewQuery.requestedColumns_) {
    throwIfColumnNotInView(viewVar);

    throwIfVariableUsedTwice(variablesSeen, target);

    auto colIdx = varToColMap_.at(viewVar).columnIndex_;
    if (colIdx == 0) {
      throwIfScanColumnIsSetTwice(s, target);
      s = target;
    } else if (colIdx == 1) {
      p = target;
    } else if (colIdx == 2) {
      o = target;
    } else {
      AD_CORRECTNESS_CHECK(colIdx > 2);
      throwIfAdditionalColumnIsNotVariable(viewVar, target);
      additionalCols.emplace_back(colIdx, target.getVariable());
    }

    if (target.isVariable()) {
      variablesSeen.insert(target.getVariable());
    }
  }

  throwIfScanColumnMissing(s);
  throwIfColumnsHaveIllegalFixedValues(s, p, o);

  // Additional columns must be sorted (required by internals of `IndexScan`)
  std::sort(additionalCols.begin(), additionalCols.end(),
            [](const auto& a, const auto& b) { return a.first < b.first; });

  return {s.value(), p, o, additionalCols};
}

namespace string_constants::detail {
static constexpr auto validViewName = ctll::fixed_string{R"(^[a-zA-Z0-9\-]+$)"};
}

// _____________________________________________________________________________
bool MaterializedView::isValidName(std::string_view name) {
  return ctre::match<string_constants::detail::validViewName>(name);
}

// _____________________________________________________________________________
void MaterializedView::throwIfInvalidName(std::string_view name) {
  if (!MaterializedView::isValidName(name)) {
    throw MaterializedViewConfigException(
        absl::StrCat("'", name,
                     "' is not a valid name for a materialized view. Only "
                     "alphanumeric characters and hyphens are allowed."));
  }
}

// _____________________________________________________________________________
void MaterializedViewsManager::setOnDiskBase(const std::string& onDiskBase) {
  AD_CORRECTNESS_CHECK(onDiskBase_ == "" && loadedViews_.rlock()->empty(),
                       "Changing the on disk basename is not allowed.");
  onDiskBase_ = onDiskBase;
}

// _____________________________________________________________________________
LocatedTriplesSharedState MaterializedView::locatedTriplesState() const {
  return {locatedTriplesState_};
}

// _____________________________________________________________________________
std::shared_ptr<LocatedTriplesState>
MaterializedView::makeEmptyLocatedTriplesState() const {
  LocatedTriplesPerBlockAllPermutations<false> emptyLocatedTriples;
  emptyLocatedTriples.at(static_cast<size_t>(permutation_->permutation()))
      .setOriginalMetadata(permutation_->metaData().blockDataShared());
  LocatedTriplesPerBlockAllPermutations<true> emptyInternalLocatedTriples;
  LocalVocab emptyVocab;

  return std::make_shared<LocatedTriplesState>(
      emptyLocatedTriples, emptyInternalLocatedTriples,
      emptyVocab.getLifetimeExtender(), 0);
}

// _____________________________________________________________________________
std::shared_ptr<IndexScan> MaterializedView::makeIndexScan(
    QueryExecutionContext* qec,
    const parsedQuery::MaterializedViewQuery& viewQuery) const {
  // The `scanTriple` might contain placeholder variables if column 1 or column
  // 2 of the view is not requested by `viewQuery`. This is due to triple
  // semantics in `IndexScan`. Note that these placeholder variables are
  // automatically stripped from the result of the `IndexScan` since
  // `viewQuery.getVarsToKeep()` returns only the variables explicitly requested
  // by the user. Therefore despite using hard-coded placeholder variable names,
  // no join occurs if multiple materialized views are requested in a single
  // query.
  auto scanTriple = makeScanConfig(viewQuery);
  return std::make_shared<IndexScan>(
      qec, permutation_, LocatedTriplesSharedState{locatedTriplesState_},
      std::move(scanTriple), IndexScan::Graphs::All(), std::nullopt,
      viewQuery.getVarsToKeep());
}

// _____________________________________________________________________________
std::shared_ptr<IndexScan> MaterializedViewsManager::makeIndexScan(
    QueryExecutionContext* qec,
    const parsedQuery::MaterializedViewQuery& viewQuery) const {
  if (!viewQuery.viewName_.has_value()) {
    throw MaterializedViewConfigException(
        "To read from a materialized view its name must be set in the "
        "query configuration.");
  }
  auto view = getView(viewQuery.viewName_.value());
  return view->makeIndexScan(qec, viewQuery);
}
