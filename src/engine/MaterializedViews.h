// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Christoph Ullinger <ullingec@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#ifndef QLEVER_SRC_ENGINE_MATERIALIZEDVIEWS_H_
#define QLEVER_SRC_ENGINE_MATERIALIZEDVIEWS_H_

#include "engine/VariableToColumnMap.h"
#include "index/DeltaTriples.h"
#include "index/Permutation.h"
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
  std::string name_;
  std::shared_ptr<QueryExecutionTree> qet_;
  std::shared_ptr<QueryExecutionContext> qec_;
  ParsedQuery parsedQuery_;
  ad_utility::MemorySize memoryLimit_;
  ad_utility::AllocatorWithLimit<Id> allocator_;

 public:
  // Type alias repeated here from `Qlever.h` to avoid cyclic include.
  using QueryPlan =
      std::tuple<std::shared_ptr<QueryExecutionTree>,
                 std::shared_ptr<QueryExecutionContext>, ParsedQuery>;

 private:
  // Initialize a writer given the base filename of the view and a query plan.
  // The view will be written to files prefixed with the index basename followed
  // by the view name.
  MaterializedViewWriter(std::string name, const QueryPlan& queryPlan,
                         ad_utility::MemorySize memoryLimit,
                         ad_utility::AllocatorWithLimit<Id> allocator);

  std::string getFilenameBase() const;

  // Computes the column ordering how the `IdTable`s from executing the
  // `QueryExecutionTree` must be permuted to match the requested target columns
  // and column ordering.
  using ColumnPermutation = std::vector<ColumnIndex>;
  using ColumnNames = std::vector<std::string>;
  using ColumnNamesAndPermutation = std::pair<ColumnNames, ColumnPermutation>;
  ColumnNamesAndPermutation getIdTableColumnNamesAndPermutation() const;

  // Actually computes and externally sorts the query result and writes the view
  // (SPO permutation and metadata) to disk.
  void computeResultAndWritePermutation() const;

 public:
  static void writeViewToDisk(
      std::string name, const QueryPlan& queryPlan,
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
  // `?s`, `?p`, `?o` are placeholders: the true value is read from on-disk
  // metadata in the constructor
  std::array<Variable, 3> indexedColVariables_{Variable{"?s"}, Variable{"?p"},
                                               Variable{"?o"}};
  std::shared_ptr<LocatedTriplesSnapshot> locatedTriplesSnapshot_;

  using AdditionalScanColumns = SparqlTripleSimple::AdditionalScanColumns;

  // Helper to create an empty `LocatedTriplesSnapshot` for `IndexScan`s as
  // materialized views do not support updates yet.
  std::shared_ptr<LocatedTriplesSnapshot> makeEmptyLocatedTriplesSnapshot()
      const;

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

  // Get the name of the indexed columns in the order on which they are sorted.
  const std::array<Variable, 3>& indexedColumns() const {
    return indexedColVariables_;
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
  std::shared_ptr<const LocatedTriplesSnapshot> locatedTriplesSnapshot() const;

  // Checks if the given name is allowed for a materialized view. Currently only
  // alphanumerics and hyphens are allowed. This is relevant for safe filenames
  // and for correctly splitting the special predicate.
  static bool isValidName(std::string_view name);
  static void throwIfInvalidName(std::string_view name);

  // Given a `MaterializedViewQuery` obtained from a special `SERVICE` or
  // predicate, compute the `SparqlTripleSimple` to be passed to the constructor
  // of `IndexScan` such that the columns requested by the user are returned. If
  // the `viewQuery` does not request columns 1 and 2, the placeholders should
  // be set to unique variable names.
  SparqlTripleSimple makeScanConfig(
      const parsedQuery::MaterializedViewQuery& viewQuery,
      Variable placeholderPredicate, Variable placeholderObject) const;

  // Given a `QueryExecutionContext` and the arguments for `makeScanConfig`
  // construct an `IndexScan` operation for scanning the requested columns of
  // this view. The result of this function is guaranteed to never be `nullptr`.
  std::shared_ptr<IndexScan> makeIndexScan(
      QueryExecutionContext* qec,
      const parsedQuery::MaterializedViewQuery& viewQuery,
      Variable placeholderPredicate, Variable placeholderObject) const;
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
      const parsedQuery::MaterializedViewQuery& viewQuery,
      Variable placeholderPredicate, Variable placeholderObject) const;
};

#endif  // QLEVER_SRC_ENGINE_MATERIALIZEDVIEWS_H_
