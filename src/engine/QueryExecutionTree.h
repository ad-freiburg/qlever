// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)
#pragma once

#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>

#include "../parser/ParsedQuery.h"
#include "../parser/data/ConstructQueryExportContext.h"
#include "../parser/data/Types.h"
#include "../parser/data/VarOrTerm.h"
#include "../util/Conversions.h"
#include "../util/Generator.h"
#include "../util/HashSet.h"
#include "../util/stream_generator.h"
#include "./Operation.h"
#include "./QueryExecutionContext.h"

using std::shared_ptr;
using std::string;

// A query execution tree. Processed bottom up, which gives an ordering to the
// operations needed to solve a query.
class QueryExecutionTree {
 public:
  explicit QueryExecutionTree(QueryExecutionContext* const qec);
  template <typename Op>
  QueryExecutionTree(QueryExecutionContext* const qec,
                     std::shared_ptr<Op> operation)
      : QueryExecutionTree(qec) {
    setOperation(std::move(operation));
  }

  enum OperationType {
    UNDEFINED,
    SCAN,
    JOIN,
    SORT,
    ORDER_BY,
    FILTER,
    DISTINCT,
    TEXT_WITHOUT_FILTER,
    TEXT_WITH_FILTER,
    OPTIONAL_JOIN,
    COUNT_AVAILABLE_PREDICATES,
    GROUP_BY,
    HAS_PREDICATE_SCAN,
    UNION,
    MULTICOLUMN_JOIN,
    TRANSITIVE_PATH,
    VALUES,
    BIND,
    MINUS,
    NEUTRAL_ELEMENT,
    DUMMY
  };

  void setOperation(OperationType type, std::shared_ptr<Operation> op);

  template <typename Op>
  void setOperation(std::shared_ptr<Op>);

  string asString(size_t indent = 0);

  const QueryExecutionContext* getQec() const { return _qec; }

  const VariableToColumnMap& getVariableColumns() const {
    AD_CONTRACT_CHECK(_rootOperation);
    return _rootOperation->getExternallyVisibleVariableColumns();
  }

  std::shared_ptr<Operation> getRootOperation() const { return _rootOperation; }

  const OperationType& getType() const { return _type; }

  // Is the root operation of this tree an `IndexScan` operation.
  // This is the only query for a concrete type that is frequently used.
  bool isIndexScan() const;

  bool isEmpty() const {
    return _type == OperationType::UNDEFINED || !_rootOperation;
  }

  size_t getVariableColumn(const Variable& variable) const;

  size_t getResultWidth() const { return _rootOperation->getResultWidth(); }

  shared_ptr<const ResultTable> getResult() const {
    return _rootOperation->getResult(isRoot());
  }

  // A variable, its column index in the Id space result, and the `ResultType`
  // of this column.
  struct VariableAndColumnIndex {
    std::string _variable;
    size_t _columnIndex;
    ResultTable::ResultType _resultType;
  };

  using ColumnIndicesAndTypes = vector<std::optional<VariableAndColumnIndex>>;

  // Returns a vector where the i-th element contains the column index and
  // `ResultType` of the i-th `selectVariable` in the `resultTable`
  ColumnIndicesAndTypes selectedVariablesToColumnIndices(
      const parsedQuery::SelectClause& selectClause,
      const ResultTable& resultTable, bool includeQuestionMark = true) const;

  const std::vector<size_t>& resultSortedOn() const {
    return _rootOperation->getResultSortedOn();
  }

  void setTextLimit(size_t limit) {
    _rootOperation->setTextLimit(limit);
    // Invalidate caches asString representation.
    _asString = "";  // triggers recomputation.
    _sizeEstimate = std::numeric_limits<size_t>::max();
  }

  size_t getCostEstimate();

  size_t getSizeEstimate();

  float getMultiplicity(size_t col) const {
    return _rootOperation->getMultiplicity(col);
  }

  size_t getDistinctEstimate(size_t col) const {
    return static_cast<size_t>(_rootOperation->getSizeEstimate() /
                               _rootOperation->getMultiplicity(col));
  }

  bool isVariableCovered(Variable variable) const;

  bool knownEmptyResult();

  // Try to find the result for this tree in the LRU cache
  // of our qec. If found, we store a shared ptr to pin it
  // and set the size estimate correctly and the cost estimate
  // to zero. Currently multiplicities are not affected
  void readFromCache();

  // recursively get all warnings from descendant operations
  vector<string> collectWarnings() const {
    return _rootOperation->collectWarnings();
  }

  void recursivelySetTimeoutTimer(
      ad_utility::SharedConcurrentTimeoutTimer timer) {
    _rootOperation->recursivelySetTimeoutTimer(std::move(timer));
  }

  template <typename F>
  void forAllDescendants(F f) {
    static_assert(
        std::is_same_v<void, std::invoke_result_t<F, QueryExecutionTree*>>);
    for (auto ptr : _rootOperation->getChildren()) {
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
    for (auto ptr : _rootOperation->getChildren()) {
      if (ptr) {
        f(ptr);
        ptr->forAllDescendants(f);
      }
    }
  }

  bool& isRoot() noexcept { return _isRoot; }
  [[nodiscard]] const bool& isRoot() const noexcept { return _isRoot; }

  // Create a `QueryExecutionTree` that produces exactly the same result as
  // `qet`, but sorted according to the `sortColumns`. If `qet` is already
  // sorted accordingly, it is simply returned.
  static std::shared_ptr<QueryExecutionTree> createSortedTree(
      std::shared_ptr<QueryExecutionTree> qet,
      const vector<size_t>& sortColumns);

  // Similar to `createSortedTree` (see directly above), but create the sorted
  // trees for two different trees, the sort columns of which are specified as
  // a vector of two-dimensional arrays. This format often appears in
  // `QueryPlanner.cpp`.
  static std::array<std::shared_ptr<QueryExecutionTree>, 2> createSortedTrees(
      std::shared_ptr<QueryExecutionTree> qetA,
      std::shared_ptr<QueryExecutionTree> qetB,
      const vector<std::array<size_t, 2>>& sortColumns);

  // If the result of this `Operation` is sorted (either because this
  // `Operation` enforces this sorting, or because it preserves the sorting of
  // its children), return the variable that is the primary sort key. Else
  // return nullopt.
  std::optional<Variable> getPrimarySortKeyVariable() const {
    return getRootOperation()->getPrimarySortKeyVariable();
  }

 private:
  QueryExecutionContext* _qec;  // No ownership
  std::shared_ptr<Operation>
      _rootOperation;  // Owned child. Will be deleted at deconstruction.
  OperationType _type;
  string _asString;
  size_t _indent = 0;  // the indent with which the _asString repr was formatted
  size_t _sizeEstimate;
  bool _isRoot = false;  // used to distinguish the root from child
                         // operations/subtrees when pinning only the result.

  std::shared_ptr<const ResultTable> _cachedResult = nullptr;

 public:
  // Helper class to avoid bug in g++ that leads to memory corruption when
  // used inside of coroutines when using srd::array<std::string, 3> instead
  // see https://gcc.gnu.org/bugzilla/show_bug.cgi?id=103909 for more
  // information
  struct StringTriple {
    std::string _subject;
    std::string _predicate;
    std::string _object;
    StringTriple(std::string subject, std::string predicate, std::string object)
        : _subject{std::move(subject)},
          _predicate{std::move(predicate)},
          _object{std::move(object)} {}
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
