// Copyright 2015 - 2024, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Björn Buchhold <buchhold@cs.uni-freiburg.de>
//          Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#pragma once

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "engine/Operation.h"
#include "engine/QueryExecutionContext.h"
#include "parser/ParsedQuery.h"
#include "parser/data/Types.h"
#include "util/stream_generator.h"

// A query execution tree. Processed bottom up, which gives an ordering to the
// operations needed to solve a query.
class QueryExecutionTree {
 public:
  explicit QueryExecutionTree(QueryExecutionContext* qec);
  QueryExecutionTree(QueryExecutionContext* qec,
                     std::shared_ptr<Operation> operation)
      : QueryExecutionTree(qec) {
    rootOperation_ = std::move(operation);
    readFromCache();
  }

  std::string getCacheKey() const;

  const QueryExecutionContext* getQec() const { return qec_; }

  const VariableToColumnMap& getVariableColumns() const {
    AD_CONTRACT_CHECK(rootOperation_);
    return rootOperation_->getExternallyVisibleVariableColumns();
  }

  // For a given `ColumnIndex` return the corresponding `pair<Variable,
  // ColumnIndexAndTypeInfo>` by searching the `VariableToColumnMap`. If the
  // given column index does not exist, an `AD_CONTRACT_CHECK` fails. This runs
  // in O(numVariables) because it does a linear search over the
  // `VariableToColumnMap`.
  const VariableToColumnMap::value_type& getVariableAndInfoByColumnIndex(
      ColumnIndex colIdx) const;

  std::shared_ptr<Operation> getRootOperation() const { return rootOperation_; }

  bool isEmpty() const { return !rootOperation_; }

  size_t getVariableColumn(const Variable& variable) const;

  size_t getResultWidth() const { return rootOperation_->getResultWidth(); }

  std::shared_ptr<const Result> getResult(bool requestLaziness = false) const {
    return rootOperation_->getResult(
        isRoot(), requestLaziness ? ComputationMode::LAZY_IF_SUPPORTED
                                  : ComputationMode::FULLY_MATERIALIZED);
  }

  // A variable, its column index in the Id space result, and the `ResultType`
  // of this column.
  struct VariableAndColumnIndex {
    std::string variable_;
    size_t columnIndex_;
  };

  using ColumnIndicesAndTypes =
      std::vector<std::optional<VariableAndColumnIndex>>;

  // Returns a vector where the i-th element contains the column index and
  // `ResultType` of the i-th `selectVariable` in the `resultTable`
  ColumnIndicesAndTypes selectedVariablesToColumnIndices(
      const parsedQuery::SelectClause& selectClause,
      bool includeQuestionMark = true) const;

  const std::vector<ColumnIndex>& resultSortedOn() const {
    return rootOperation_->getResultSortedOn();
  }

  size_t getCostEstimate();

  size_t getSizeEstimate();

  float getMultiplicity(size_t col) const {
    return rootOperation_->getMultiplicity(col);
  }

  size_t getDistinctEstimate(size_t col) const {
    return static_cast<size_t>(rootOperation_->getSizeEstimate() /
                               rootOperation_->getMultiplicity(col));
  }

  bool isVariableCovered(Variable variable) const;

  bool knownEmptyResult();

  // Try to find the result for this tree in the LRU cache
  // of our qec. If found, we store a shared ptr to pin it
  // and set the size estimate correctly and the cost estimate
  // to zero. Currently multiplicities are not affected
  void readFromCache();

  // recursively get all warnings from descendant operations
  std::vector<std::string> collectWarnings() const {
    return rootOperation_->collectWarnings();
  }

  template <typename F>
  void forAllDescendants(F f) {
    static_assert(
        std::is_same_v<void, std::invoke_result_t<F, QueryExecutionTree*>>);
    for (auto ptr : rootOperation_->getChildren()) {
      if (ptr) {
        f(ptr);
        ptr->forAllDescendants(f);
      }
    }
  }

  template <typename F>
  void forAllDescendants(F f) const {
    static_assert(
        std::is_same_v<void,
                       std::invoke_result_t<F, const QueryExecutionTree*>>);
    for (auto ptr : rootOperation_->getChildren()) {
      if (ptr) {
        f(ptr);
        ptr->forAllDescendants(f);
      }
    }
  }

  bool& isRoot() noexcept { return isRoot_; }
  [[nodiscard]] const bool& isRoot() const noexcept { return isRoot_; }

  // Create a `QueryExecutionTree` that produces exactly the same result as
  // `qet`, but sorted according to the `sortColumns`. If `qet` is already
  // sorted accordingly, it is simply returned.
  static std::shared_ptr<QueryExecutionTree> createSortedTree(
      std::shared_ptr<QueryExecutionTree> qet,
      const std::vector<ColumnIndex>& sortColumns);

  // Similar to `createSortedTree` (see directly above), but create the sorted
  // trees for two different trees, the sort columns of which are specified as
  // a vector of two-dimensional arrays. This format often appears in
  // `QueryPlanner.cpp`.
  static std::pair<std::shared_ptr<QueryExecutionTree>,
                   std::shared_ptr<QueryExecutionTree>>
  createSortedTrees(std::shared_ptr<QueryExecutionTree> qetA,
                    std::shared_ptr<QueryExecutionTree> qetB,
                    const std::vector<std::array<ColumnIndex, 2>>& sortColumns);

  // The return type of the `getSortedTreesAndJoinColumns` function below. It is
  // deliberately stored as a tuple vs. a struct with named members, so that we
  // can use `std::tie` on it (see for example `MultiColumnJoin.cpp`).
  using SortedTreesAndJoinColumns =
      std::tuple<std::shared_ptr<QueryExecutionTree>,
                 std::shared_ptr<QueryExecutionTree>,
                 std::vector<std::array<ColumnIndex, 2>>>;

  // First compute the join columns of the two trees, and then sort the trees by
  // those join columns. Return the sorted trees as well as the join columns.
  // Throw an exception if the two trees have no join columns in common.
  static SortedTreesAndJoinColumns getSortedSubtreesAndJoinColumns(
      std::shared_ptr<QueryExecutionTree> qetA,
      std::shared_ptr<QueryExecutionTree> qetB);

  // Return the column pairs where the two `QueryExecutionTree`s have the
  // same variable. The result is sorted by the column indices, so that it is
  // deterministic when called repeatedly. This is important to find a
  // `QueryExecutionTree` in the cache.
  static std::vector<std::array<ColumnIndex, 2>> getJoinColumns(
      const QueryExecutionTree& qetA, const QueryExecutionTree& qetB);

  // If the result of this `Operation` is sorted (either because this
  // `Operation` enforces this sorting, or because it preserves the sorting of
  // its children), return the variable that is the primary sort key. Else
  // return nullopt.
  std::optional<Variable> getPrimarySortKeyVariable() const {
    return getRootOperation()->getPrimarySortKeyVariable();
  }

  // _____________________________________________________________
  friend void PrintTo(const QueryExecutionTree& tree, std::ostream* os) {
    auto& s = *os;
    s << tree.getRootOperation()->getDescriptor();
  }

 private:
  QueryExecutionContext* qec_;  // No ownership
  std::shared_ptr<Operation> rootOperation_ =
      nullptr;  // Owned child. Will be deleted at deconstruction.
  std::optional<size_t> sizeEstimate_ = std::nullopt;
  bool isRoot_ = false;  // used to distinguish the root from child
                         // operations/subtrees when pinning only the result.

  std::shared_ptr<const Result> cachedResult_ = nullptr;

 public:
  // Helper class to avoid bug in g++ that leads to memory corruption when
  // used inside of coroutines when using srd::array<std::string, 3> instead
  // see https://gcc.gnu.org/bugzilla/show_bug.cgi?id=103909 for more
  // information
  struct StringTriple {
    std::string subject_;
    std::string predicate_;
    std::string object_;
    StringTriple(std::string subject, std::string predicate, std::string object)
        : subject_{std::move(subject)},
          predicate_{std::move(predicate)},
          object_{std::move(object)} {}
  };
};

namespace ad_utility {
// Create a `QueryExecutionTree` with `Operation` at the root.
// The `Operation` is created using `qec` and `args...` as constructor
// arguments.
template <typename Operation>
std::shared_ptr<QueryExecutionTree> makeExecutionTree(
    QueryExecutionContext* qec, auto&&... args) {
  return std::make_shared<QueryExecutionTree>(
      qec, std::make_shared<Operation>(qec, AD_FWD(args)...));
}
}  // namespace ad_utility
