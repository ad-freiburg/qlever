// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Christoph Ullinger <ullingec@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#ifndef QLEVER_SRC_ENGINE_MATERIALIZEDVIEWS_H_
#define QLEVER_SRC_ENGINE_MATERIALIZEDVIEWS_H_

#include "engine/VariableToColumnMap.h"
#include "engine/idTable/CompressedExternalIdTable.h"
#include "index/DeltaTriples.h"
#include "index/ExternalSortFunctors.h"
#include "index/Permutation.h"
#include "libqlever/QleverTypes.h"
#include "parser/MaterializedViewQuery.h"
#include "parser/ParsedQuery.h"
#include "parser/SparqlTriple.h"
#include "util/HashMap.h"

// Forward declarations
class QueryExecutionContext;
class QueryExecutionTree;
class IndexScan;

// For the future, materialized views save their version. If we change something
// about the way materialized views are stored, we can break the existing ones
// cleanly without breaking the entire index format.
static constexpr size_t MATERIALIZED_VIEWS_VERSION = 1;

// The `MaterializedViewWriter` can be used to write a new materialized view to
// disk, given an already planned query. The query will be executed lazily and
// the results will be written to the view.
class MaterializedViewWriter {
 private:
  // Filename components for writing the view to disk.
  std::string onDiskBase_;
  std::string name_;

  // Query plan to retrieve the view's rows.
  std::shared_ptr<QueryExecutionTree> qet_;
  std::shared_ptr<QueryExecutionContext> qec_;
  ParsedQuery parsedQuery_;

  // Memory limit and allocator for `CompressedExternalIdTableSorter`, which is
  // used if the query result is not correctly sorted.
  ad_utility::MemorySize memoryLimit_;
  ad_utility::AllocatorWithLimit<Id> allocator_;

  // The correctly ordered column names of the view.
  std::vector<Variable> columnNames_;

  // The columns of the `IdTable`s we get when executing the query can be in
  // arbitrary order. This permutation needs to be applied to get `IdTable`s
  // with the same column ordering as the `SELECT` statement.
  std::vector<ColumnIndex> columnPermutation_;

  // The number of empty columns to add to the query result such that the
  // resulting table has at least four columns.
  uint8_t numAddEmptyColumns_;

  using RangeOfIdTables = ad_utility::InputRangeTypeErased<IdTableStatic<0>>;
  // SPO comparator
  using Comparator = SortTriple<0, 1, 2>;
  // Sorter for SPO permutation with a dynamic number of columns (template
  // argument `NumStaticCols == 0`)
  using Sorter = ad_utility::CompressedExternalIdTableSorter<Comparator, 0>;

 public:
  using QueryPlan = qlever::QueryPlan;

 private:
  // Initialize a writer given the base filename of the view and a query plan.
  // The view will be written to files prefixed with the index basename followed
  // by the view name.
  MaterializedViewWriter(std::string onDiskBase, std::string name,
                         const QueryPlan& queryPlan,
                         ad_utility::MemorySize memoryLimit,
                         ad_utility::AllocatorWithLimit<Id> allocator);

  // Get the base filename for the view's permutation and metadata files. This
  // name is the result of concatenating `onDiskBase` and `name`.
  std::string getFilenameBase() const;

  // Helper that computes the column ordering how the `IdTable`s from executing
  // the `QueryExecutionTree` must be permuted to match the requested target
  // columns and column ordering. This is called in the constructor to populate
  // `columnNamesAndPermutation_`.
  using ColumnNameAndIndex = std::pair<Variable, ColumnIndex>;
  struct ColumnNamesAndPermutation {
    std::vector<ColumnNameAndIndex> columnNamesAndIndices_;
    uint8_t numAddEmptyColumns_;
  };
  ColumnNamesAndPermutation getIdTableColumnNamesAndPermutation() const;

  // The number of columns of the view.
  size_t numCols() const {
    return columnPermutation_.size() + numAddEmptyColumns_;
  }

  // Helper to permute an `IdTable` according to `columnPermutation_` and verify
  // that there are no `LocalVocabEntry` values in any of the selected columns.
  void permuteIdTableAndCheckNoLocalVocabEntries(IdTable& block) const;

  // Helper for `computeResultAndWritePermutation`: If the query given by the
  // user is already sorted correctly, this function can be used to obtain the
  // permuted blocks.
  RangeOfIdTables getBlocksForAlreadySortedResult(
      std::shared_ptr<const Result> result) const;

  // Helper for `computeResultAndWritePermutation`: If the query given by the
  // user is not sorted correctly, this function can be used to invoke the
  // external sorted and obtain sorted and correctly permuted blocks.
  RangeOfIdTables getBlocksForUnsortedResult(
      Sorter& spoSorter, std::shared_ptr<const Result> result) const;

  // Helper for `computeResultAndWritePermutation`: Checks if the result is
  // correctly sorted and invokes `getBlocksForAlreadySortedResult` or
  // `getBlocksForUnsortedResult` accordingly.
  RangeOfIdTables getSortedBlocks(Sorter& spoSorter,
                                  std::shared_ptr<const Result> result) const;

  // Helper for `computeResultAndWritePermutation`: given sorted and permuted
  // blocks from `getSortedBlocks`, write the `Permutation` to disk using
  // `CompressedRelationWriter`. Returns the permutation metadata.
  IndexMetaDataMmap writePermutation(RangeOfIdTables sortedBlocksSPO) const;

  // Helper for `computeResultAndWritePermutation`: Writes the metadata JSON
  // files with column names and ordering to disk.
  void writeViewMetadata() const;

  // Actually computes, permutes and if needed externally sorts the query result
  // and writes the view (SPO permutation and metadata) to disk.
  void computeResultAndWritePermutation() const;

 public:
  // Write a `MaterializedView` given the index' `onDiskBase`, a valid `name`
  // (consisting only of alphanumerics and hyphens) and a `queryPlan` to be
  // executed. The query's result is written to the view.
  //
  // The `memoryLimit` and `allocator` are used only for sorting the
  // permutation if the query result is not correctly sorted already. The
  // `queryPlan` is executed with the normal query memory limit.
  static void writeViewToDisk(
      std::string onDiskBase, std::string name, const QueryPlan& queryPlan,
      ad_utility::MemorySize memoryLimit = ad_utility::MemorySize::gigabytes(4),
      ad_utility::AllocatorWithLimit<Id> allocator =
          ad_utility::makeUnlimitedAllocator<Id>());
};

// This class represents a single loaded `MaterializedView`. It can be used for
// `IndexScan`s.
class MaterializedView {
 private:
  std::string onDiskBase_;
  std::string name_;
  std::shared_ptr<Permutation> permutation_{std::make_shared<Permutation>(
      Permutation::Enum::SPO, ad_utility::makeUnlimitedAllocator<Id>())};
  VariableToColumnMap varToColMap_;
  std::shared_ptr<LocatedTriplesState> locatedTriplesState_;

  using AdditionalScanColumns = SparqlTripleSimple::AdditionalScanColumns;

  // Helper to create an empty `LocatedTriplesState` for `IndexScan`s as
  // materialized views do not support updates yet.
  std::shared_ptr<LocatedTriplesState> makeEmptyLocatedTriplesState() const;

 public:
  // Load a materialized view from disk given the filename components. The
  // constructor will throw an exception if the name is invalid or the view does
  // not exist.
  MaterializedView(std::string onDiskBase, std::string name);

  // Get the name of the view.
  const std::string& name() const { return name_; }

  // Get the variable to column map.
  const VariableToColumnMap& variableToColumnMap() const {
    return varToColMap_;
  }

  // Return the combined filename from the index' `onDiskBase` and the name of
  // the view. Note that this function does not check for validity or existence.
  static std::string getFilenameBase(std::string_view onDiskBase,
                                     std::string_view name);

  // Return a pointer to the open `Permutation` object for this view. Note that
  // this is always an SPO permutation because materialized views are indexed on
  // the first column. The result of this function is guaranteed to never be
  // `nullptr`.
  std::shared_ptr<const Permutation> permutation() const;

  // Return a reference to the `LocatedTriplesSnapshot` for the permutation. For
  // now this is always an empty snapshot but with the correct permutation
  // metadata.
  LocatedTriplesSharedState locatedTriplesState() const;

  // Checks if the given name is allowed for a materialized view. Currently only
  // alphanumerics and hyphens are allowed. This is relevant for safe filenames
  // and for correctly splitting the special predicate.
  static bool isValidName(std::string_view name);
  static void throwIfInvalidName(std::string_view name);

  // Given a `MaterializedViewQuery` obtained from a special `SERVICE` or
  // predicate, compute the `SparqlTripleSimple` to be passed to the constructor
  // of `IndexScan` such that the columns requested by the user are returned.
  SparqlTripleSimple makeScanConfig(
      const parsedQuery::MaterializedViewQuery& viewQuery) const;

  // Helpers for checking metadata-dependent invariants of
  // `MaterializedViewQuery` in `makeScanConfig`.
  void throwIfScanColumnMissing(const std::optional<TripleComponent>& s) const;
  void throwIfColumnsHaveIllegalFixedValues(
      const std::optional<TripleComponent>& s, const TripleComponent& p,
      const TripleComponent& o) const;
  void throwIfColumnNotInView(const Variable& column) const;
  void throwIfAdditionalColumnIsNotVariable(const Variable& column,
                                            const TripleComponent& value) const;
  void throwIfScanColumnIsSetTwice(const std::optional<TripleComponent>& s,
                                   const TripleComponent& value) const;
  void throwIfVariableUsedTwice(
      const ad_utility::HashSet<Variable>& variablesSeen,
      const TripleComponent& target) const;

  // Given a `QueryExecutionContext` and the arguments for `makeScanConfig`
  // construct an `IndexScan` operation for scanning the requested columns
  // of this view. The result of this function is guaranteed to never be
  // `nullptr`.
  std::shared_ptr<IndexScan> makeIndexScan(
      QueryExecutionContext* qec,
      const parsedQuery::MaterializedViewQuery& viewQuery) const;
};

// The `MaterializedViewsManager` is part of the `QueryExecutionContext` and is
// used to manage the currently loaded `MaterializedViews` in a `Server` or
// `Qlever` instance.
class MaterializedViewsManager {
 private:
  std::string onDiskBase_;
  mutable ad_utility::Synchronized<
      ad_utility::HashMap<std::string, std::shared_ptr<MaterializedView>>>
      loadedViews_;

 public:
  MaterializedViewsManager() = default;
  explicit MaterializedViewsManager(std::string onDiskBase)
      : onDiskBase_{std::move(onDiskBase)} {};

  // For use with the default constructor: set the index basename after creation
  // of the `MaterializedViewsManager`. This should only be called once and
  // before any calls to `loadView` and `getView`.
  void setOnDiskBase(const std::string& onDiskBase);

  // Since we don't want to break the const-ness in a lot of places just for the
  // loading of views, `loadedViews_` is mutable. Note that this is okay,
  // because the views themselves aren't changed (only loaded on-demand).
  void loadView(const std::string& name) const;

  // Load the given view if it is not already loaded and return it. This pointer
  // is never `nullptr`. If the view does not exist, the function throws.
  std::shared_ptr<const MaterializedView> getView(
      const std::string& name) const;

  // The same as `MaterializedView::makeIndexScan` above, but load and use the
  // right view automatically as requested in the `MaterializedViewQuery`.
  std::shared_ptr<IndexScan> makeIndexScan(
      QueryExecutionContext* qec,
      const parsedQuery::MaterializedViewQuery& viewQuery) const;
};

#endif  // QLEVER_SRC_ENGINE_MATERIALIZEDVIEWS_H_
