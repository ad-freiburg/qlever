// Copyright 2019, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Florian Kramer (florian.kramer@neptun.uni-freiburg.de)
//         Johannes Herrmann (johannes.r.herrmann(at)gmail.com)

#ifndef QLEVER_SRC_ENGINE_TRANSITIVEPATHBASE_H
#define QLEVER_SRC_ENGINE_TRANSITIVEPATHBASE_H

#include <absl/hash/hash.h>

#include <functional>
#include <memory>

#include "engine/Operation.h"
#include "engine/QueryExecutionTree.h"

using TreeAndCol = std::pair<std::shared_ptr<QueryExecutionTree>, size_t>;
struct TransitivePathSide {
  // treeAndCol contains the QueryExecutionTree of this side and the column
  // where the Ids of this side are located. This member only has a value if
  // this side was bound.
  std::optional<TreeAndCol> treeAndCol_;
  // Column of the sub table where the Ids of this side are located
  size_t subCol_;
  TripleComponent value_;
  // The column in the output table where this side Ids are written to.
  // This member is set by the TransitivePath class
  size_t outputCol_ = 0;

  bool isVariable() const { return value_.isVariable(); }

  bool isBoundVariable() const { return treeAndCol_.has_value(); }

  bool isUnboundVariable() const { return isVariable() && !isBoundVariable(); }

  std::string getCacheKey() const {
    std::ostringstream os;
    if (!isVariable()) {
      os << "Value " << value_;
    }

    os << ", subColumn: " << subCol_ << "to " << outputCol_;

    if (treeAndCol_.has_value()) {
      const auto& [tree, col] = treeAndCol_.value();
      os << ", Subtree:\n";
      os << tree->getCacheKey() << "with join column " << col << "\n";
    }
    return std::move(os).str();
  }

  bool isSortedOnInputCol() const {
    if (!treeAndCol_.has_value()) {
      return false;
    }

    auto [tree, col] = treeAndCol_.value();
    const std::vector<ColumnIndex>& sortedOn =
        tree->getRootOperation()->getResultSortedOn();
    // TODO<C++23> use ql::ranges::starts_with
    return (!sortedOn.empty() && sortedOn[0] == col);
  }

  TransitivePathSide clone() const {
    TransitivePathSide copy = *this;
    if (copy.treeAndCol_.has_value()) {
      copy.treeAndCol_.value().first = copy.treeAndCol_.value().first->clone();
    }
    return copy;
  }
};

// We deliberately use the `std::` variants of a hash set and hash map because
// `absl`s types are not exception safe.
using Set = std::unordered_set<Id, absl::Hash<Id>, std::equal_to<Id>,
                               ad_utility::AllocatorWithLimit<Id>>;
using Map = std::unordered_map<
    Id, Set, absl::Hash<Id>, std::equal_to<Id>,
    ad_utility::AllocatorWithLimit<std::pair<const Id, Set>>>;

// Helper struct, that allows a generator to yield a a node and all its
// connected nodes (the `targets`), along with a local vocabulary and the row
// index of the node in the input table. The `IdTable` pointer might be null if
// the `Id` is not associated with a table. In this case the `row` value does
// not represent anything meaningful and should not be used.
struct NodeWithTargets {
  Id node_;
  Set targets_;
  LocalVocab localVocab_;
  const IdTable* idTable_;
  size_t row_;

  // Explicit to prevent issues with co_yield and lifetime.
  // See https://gcc.gnu.org/bugzilla/show_bug.cgi?id=103909 for more info.
  NodeWithTargets(Id node, Set targets, LocalVocab localVocab,
                  const IdTable* idTable, size_t row)
      : node_{node},
        targets_{std::move(targets)},
        localVocab_{std::move(localVocab)},
        idTable_{idTable},
        row_{row} {}
};

using NodeGenerator = cppcoro::generator<NodeWithTargets>;

/**
 * @class TransitivePathBase
 * @brief A common base class for different implementations of the Transitive
 * Path operation. To create an actual object use the static factory function
 * `makeStaticPath`.
 *
 */
class TransitivePathBase : public Operation {
 protected:
  using Graphs = ScanSpecificationAsTripleComponent::Graphs;

  std::shared_ptr<QueryExecutionTree> subtree_;
  TransitivePathSide lhs_;
  TransitivePathSide rhs_;
  size_t resultWidth_ = 2;
  size_t minDist_;
  size_t maxDist_;
  VariableToColumnMap variableColumns_;
  // Indicate that the variable is only bound because the path is empty, not
  // because `bindLeftOrRightSide` was called. This means that it is bound to a
  // full scan of all subjects and objects in the knowledge graph, but can be
  // re-bound to something cheaper later if the query permits it.
  bool boundVariableIsForEmptyPath_ = false;

 public:
  TransitivePathBase(QueryExecutionContext* qec,
                     std::shared_ptr<QueryExecutionTree> child,
                     TransitivePathSide leftSide, TransitivePathSide rightSide,
                     size_t minDist, size_t maxDist, Graphs activeGraphs);

  ~TransitivePathBase() override = 0;

  /**
   * Returns a new TransitivePath operation that uses the fact that leftop
   * generates all possible values for the left side of the paths. If the
   * results of leftop is smaller than all possible values this will result in a
   * faster transitive path operation (as the transitive paths has to be
   * computed for fewer elements).
   */
  std::shared_ptr<TransitivePathBase> bindLeftSide(
      std::shared_ptr<QueryExecutionTree> leftop, size_t inputCol) const;

  /**
   * Returns a new TransitivePath operation that uses the fact that rightop
   * generates all possible values for the right side of the paths. If the
   * results of rightop is smaller than all possible values this will result in
   * a faster transitive path operation (as the transitive paths has to be
   * computed for fewer elements).
   */
  std::shared_ptr<TransitivePathBase> bindRightSide(
      std::shared_ptr<QueryExecutionTree> rightop, size_t inputCol) const;

  bool isBoundOrId() const;

  /**
   * Getters, mainly necessary for testing
   */
  size_t getMinDist() const { return minDist_; }
  size_t getMaxDist() const { return maxDist_; }
  const TransitivePathSide& getLeft() const { return lhs_; }
  const TransitivePathSide& getRight() const { return rhs_; }

 protected:
  std::string getCacheKeyImpl() const override;

  /**
   * @brief Decide on which transitive path side the hull computation should
   * start and where it should end. The start and target side are chosen by
   * the following criteria:
   *
   * 1. If a side is bound, then this side will be the start side.
   * 2. If a side is an id, then this side will be the start side.
   * 3. If both sides are variables, the left side is chosen as start
   * (arbitrarily).
   *
   * @return std::pair<TransitivePathSide&, TransitivePathSide&> The first entry
   * of the pair is the start side, the second entry is the target side.
   */
  std::pair<TransitivePathSide&, TransitivePathSide&> decideDirection();

  /**
   * @brief Fill the given table with the transitive hull and use the
   * startSideTable to fill in the rest of the columns.
   * This function is called if the start side is bound and a variable.
   *
   * @param hull The transitive hull, represented by a generator that yields
   * sets of connected nodes with some metadata.
   * @param startSideCol The column of the result table for the startSide of the
   * hull
   * @param targetSideCol The column of the result table for the targetSide of
   * the hull
   * @param skipCol This column contains the Ids of the start side in the
   * startSideTable and will be skipped.
   * @param yieldOnce If true, the generator will yield only a single time.
   * @param inputWidth The width of the input table that is referenced by the
   * elements of `hull`.
   */
  Result::Generator fillTableWithHull(NodeGenerator hull, size_t startSideCol,
                                      size_t targetSideCol, size_t skipCol,
                                      bool yieldOnce, size_t inputWidth) const;

  /**
   * @brief Fill the given table with the transitive hull.
   * This function is called if the sides are unbound or ids.
   *
   * @param hull The transitive hull.
   * @param startSideCol The column of the result table for the startSide of the
   * hull
   * @param targetSideCol The column of the result table for the targetSide of
   * the hull
   * @param yieldOnce If true, the generator will yield only a single time.
   */
  Result::Generator fillTableWithHull(NodeGenerator hull, size_t startSideCol,
                                      size_t targetSideCol,
                                      bool yieldOnce) const;

  // Copy the columns from the input table to the output table
  template <size_t INPUT_WIDTH, size_t OUTPUT_WIDTH>
  static void copyColumns(const IdTableView<INPUT_WIDTH>& inputTable,
                          IdTableStatic<OUTPUT_WIDTH>& outputTable,
                          size_t inputRow, size_t outputRow, size_t skipCol);

  // A small helper function: Insert the `value` to the set at `map[key]`.
  // As the sets all have an allocator with memory limit, this construction is a
  // little bit more involved, so this can be a separate helper function.
  void insertIntoMap(Map& map, Id key, Id value) const;

 public:
  std::string getDescriptor() const override;

  size_t getResultWidth() const override;

  vector<ColumnIndex> resultSortedOn() const override;

  bool knownEmptyResult() override;

  float getMultiplicity(size_t col) override;

 private:
  uint64_t getSizeEstimateBeforeLimit() override;

  template <size_t INPUT_WIDTH, size_t OUTPUT_WIDTH>
  Result::Generator fillTableWithHullImpl(NodeGenerator hull,
                                          size_t startSideCol,
                                          size_t targetSideCol, bool yieldOnce,
                                          size_t skipCol = 0) const;

  // Return an execution tree, that "joins" the given `tripleComponent` with all
  // of the subjects or objects in the knowledge graph, so if the graph does not
  // contain this value it is filtered out.
  static std::shared_ptr<QueryExecutionTree> joinWithIndexScan(
      QueryExecutionContext* qec, Graphs activeGraphs,
      const TripleComponent& tripleComponent);

  // Return an execution tree that represents one side of an empty path. This is
  // used as a starting point for evaluating the empty path.
  static std::shared_ptr<QueryExecutionTree> makeEmptyPathSide(
      QueryExecutionContext* qec, Graphs activeGraphs);

 public:
  size_t getCostEstimate() override;

  /**
   * @brief Make a concrete TransitivePath object using the given parameters.
   * The concrete object will either be TransitivePathFallback or
   * TransitivePathBinSearch, depending on the useBinSearch flag.
   *
   * @param qec QueryExecutionContext for the TransitivePath Operation
   * @param child QueryExecutionTree for the subquery of the TransitivePath
   * @param leftSide Settings for the left side of the TransitivePath
   * @param rightSide Settings for the right side of the TransitivePath
   * @param minDist Minimum distance a resulting path may have (distance =
   * number of nodes)
   * @param maxDist Maximum distance a resulting path may have (distance =
   * number of nodes)
   * @param useBinSearch If true, the returned object will be a
   * TransitivePathBinSearch. Else it will be a TransitivePathFallback
   * @param activeGraphs Contains the graphs that are active in the current
   * context.
   */
  static std::shared_ptr<TransitivePathBase> makeTransitivePath(
      QueryExecutionContext* qec, std::shared_ptr<QueryExecutionTree> child,
      TransitivePathSide leftSide, TransitivePathSide rightSide, size_t minDist,
      size_t maxDist, bool useBinSearch, Graphs activeGraphs = {});

  /**
   * @brief Make a concrete TransitivePath object using the given parameters.
   * The concrete object will either be TransitivePathFallback or
   * TransitivePathBinSearch, depending on the runtime constant "use-binsearch".
   *
   * @param qec QueryExecutionContext for the TransitivePath Operation
   * @param child QueryExecutionTree for the subquery of the TransitivePath
   * @param leftSide Settings for the left side of the TransitivePath
   * @param rightSide Settings for the right side of the TransitivePath
   * @param minDist Minimum distance a resulting path may have (distance =
   * number of nodes)
   * @param maxDist Maximum distance a resulting path may have (distance =
   * number of nodes)
   * @param activeGraphs Contains the graphs that are active in the current
   * context.
   */
  static std::shared_ptr<TransitivePathBase> makeTransitivePath(
      QueryExecutionContext* qec, std::shared_ptr<QueryExecutionTree> child,
      TransitivePathSide leftSide, TransitivePathSide rightSide, size_t minDist,
      size_t maxDist, Graphs activeGraphs = {});

  vector<QueryExecutionTree*> getChildren() override;

  VariableToColumnMap computeVariableToColumnMap() const override;

  // The internal implementation of `bindLeftSide` and `bindRightSide` which
  // share a lot of code.
  std::shared_ptr<TransitivePathBase> bindLeftOrRightSide(
      std::shared_ptr<QueryExecutionTree> leftOrRightOp, size_t inputCol,
      bool isLeft) const;

  // Return a set of subtrees that can be used alternatively when the left or
  // right side is bound. This is used by the `TransitivePathBinSearch` class,
  // which has to store both ways to sort the subtree until it knows which side
  // becomes bound.
  virtual std::span<const std::shared_ptr<QueryExecutionTree>>
  alternativeSubtrees() const {
    return {};
  }
};

#endif  // QLEVER_SRC_ENGINE_TRANSITIVEPATHBASE_H
