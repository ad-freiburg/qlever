// Copyright 2019, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Florian Kramer (florian.kramer@neptun.uni-freiburg.de)

#pragma once

#include <memory>

#include "engine/Operation.h"
#include "engine/QueryExecutionTree.h"
#include "engine/idTable/IdTable.h"

using TreeAndCol = std::pair<std::shared_ptr<QueryExecutionTree>, size_t>;
struct TransitivePathSide {
  // treeAndCol contains the QueryExecutionTree of this side and the column
  // where the Ids of this side are located. This member only has a value if
  // this side was bound.
  std::optional<TreeAndCol> treeAndCol_;
  // Column of the sub table where the Ids of this side are located
  size_t subCol_;
  std::variant<Id, Variable> value_;
  // The column in the ouput table where this side Ids are written to.
  // This member is set by the TransitivePath class
  size_t outputCol_ = 0;

  bool isVariable() const { return std::holds_alternative<Variable>(value_); };

  bool isBoundVariable() const { return treeAndCol_.has_value(); };

  std::string getCacheKey() const {
    std::ostringstream os;
    if (!isVariable()) {
      os << "Id: " << std::get<Id>(value_);
    }

    os << ", subColumn: " << subCol_ << "to " << outputCol_;

    if (treeAndCol_.has_value()) {
      const auto& [tree, col] = treeAndCol_.value();
      os << ", Subtree:\n";
      os << tree->asString() << "with join column " << col << "\n";
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
    // TODO<C++23> use std::ranges::starts_with
    return (!sortedOn.empty() && sortedOn[0] == col);
  }
};

class TransitivePath : public Operation {
  // We deliberately use the `std::` variants of a hash set and hash map because
  // `absl`s types are not exception safe.
  constexpr static auto hash = [](Id id) {
    return std::hash<uint64_t>{}(id.getBits());
  };
  using Set = std::unordered_set<Id, decltype(hash), std::equal_to<Id>,
                                 ad_utility::AllocatorWithLimit<Id>>;
  using Map = std::unordered_map<
      Id, Set, decltype(hash), std::equal_to<Id>,
      ad_utility::AllocatorWithLimit<std::pair<const Id, Set>>>;

  std::shared_ptr<QueryExecutionTree> subtree_;
  TransitivePathSide lhs_;
  TransitivePathSide rhs_;
  size_t resultWidth_ = 2;
  size_t minDist_;
  size_t maxDist_;
  VariableToColumnMap variableColumns_;

 public:
  TransitivePath(QueryExecutionContext* qec,
                 std::shared_ptr<QueryExecutionTree> child,
                 TransitivePathSide leftSide, TransitivePathSide rightSide,
                 size_t minDist, size_t maxDist);

  /**
   * Returns a new TransitivePath operation that uses the fact that leftop
   * generates all possible values for the left side of the paths. If the
   * results of leftop is smaller than all possible values this will result in a
   * faster transitive path operation (as the transitive paths has to be
   * computed for fewer elements).
   */
  std::shared_ptr<TransitivePath> bindLeftSide(
      std::shared_ptr<QueryExecutionTree> leftop, size_t inputCol) const;

  /**
   * Returns a new TransitivePath operation that uses the fact that rightop
   * generates all possible values for the right side of the paths. If the
   * results of rightop is smaller than all possible values this will result in
   * a faster transitive path operation (as the transitive paths has to be
   * computed for fewer elements).
   */
  std::shared_ptr<TransitivePath> bindRightSide(
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
  virtual std::string asStringImpl(size_t indent = 0) const override;

 public:
  virtual std::string getDescriptor() const override;

  virtual size_t getResultWidth() const override;

  virtual vector<ColumnIndex> resultSortedOn() const override;

  virtual void setTextLimit(size_t limit) override;

  virtual bool knownEmptyResult() override;

  virtual float getMultiplicity(size_t col) override;

 private:
  uint64_t getSizeEstimateBeforeLimit() override;

 public:
  virtual size_t getCostEstimate() override;

  vector<QueryExecutionTree*> getChildren() override {
    std::vector<QueryExecutionTree*> res;
    auto addChildren = [](std::vector<QueryExecutionTree*>& res,
                          TransitivePathSide side) {
      if (side.treeAndCol_.has_value()) {
        res.push_back(side.treeAndCol_.value().first.get());
      }
    };
    addChildren(res, lhs_);
    addChildren(res, rhs_);
    res.push_back(subtree_.get());
    return res;
  }

  /**
   * @brief Compute the transitive hull with a bound side.
   * This function is called when the startSide is bound and
   * it is a variable. The other IdTable contains the result
   * of the start side and will be used to get the start nodes.
   *
   * @tparam RES_WIDTH Number of columns of the result table
   * @tparam SUB_WIDTH Number of columns of the sub table
   * @tparam SIDE_WIDTH Number of columns of the
   * @param res The result table which will be filled in-place
   * @param sub The IdTable for the sub result
   * @param startSide The start side for the transitive hull
   * @param targetSide The target side for the transitive hull
   * @param startSideTable The IdTable of the startSide
   */
  template <size_t RES_WIDTH, size_t SUB_WIDTH, size_t SIDE_WIDTH>
  void computeTransitivePathBound(IdTable* res, const IdTable& sub,
                                  const TransitivePathSide& startSide,
                                  const TransitivePathSide& targetSide,
                                  const IdTable& startSideTable) const;

  /**
   * @brief Compute the transitive hull.
   * This function is called when no side is bound (or an id).
   *
   * @tparam RES_WIDTH Number of columns of the result table
   * @tparam SUB_WIDTH Number of columns of the sub table
   * @param res The result table which will be filled in-place
   * @param sub The IdTable for the sub result
   * @param startSide The start side for the transitive hull
   * @param targetSide The target side for the transitive hull
   */
  template <size_t RES_WIDTH, size_t SUB_WIDTH>
  void computeTransitivePath(IdTable* res, const IdTable& sub,
                             const TransitivePathSide& startSide,
                             const TransitivePathSide& targetSide) const;

 private:
  /**
   * @brief Compute the result for this TransitivePath operation
   * This function chooses the start and target side for the transitive
   * hull computation. This choice of the start side has a large impact
   * on the time it takes to compute the hull. The set of nodes on the
   * start side should be as small as possible.
   *
   * @return ResultTable The result of the TransitivePath operation
   */
  virtual ResultTable computeResult() override;

  VariableToColumnMap computeVariableToColumnMap() const override;

  // The internal implementation of `bindLeftSide` and `bindRightSide` which
  // share a lot of code.
  std::shared_ptr<TransitivePath> bindLeftOrRightSide(
      std::shared_ptr<QueryExecutionTree> leftOrRightOp, size_t inputCol,
      bool isLeft) const;

  /**
   * @brief Compute the transitive hull starting at the given nodes,
   * using the given Map.
   *
   * @param edges Adjacency lists, mapping Ids (nodes) to their connected
   * Ids.
   * @param nodes A list of Ids. These Ids are used as starting points for the
   * transitive hull. Thus, this parameter guides the performance of this
   * algorithm.
   * @param target Optional target Id. If supplied, only paths which end
   * in this Id are added to the hull.
   * @return Map Maps each Id to its connected Ids in the transitive hull
   */
  Map transitiveHull(const Map& edges, const std::vector<Id>& startNodes,
                     std::optional<Id> target) const;

  /**
   * @brief Fill the given table with the transitive hull and use the
   * startSideTable to fill in the rest of the columns.
   * This function is called if the start side is bound and a variable.
   *
   * @tparam WIDTH The number of columns of the result table.
   * @tparam START_WIDTH The number of columns of the start table.
   * @param table The result table which will be filled.
   * @param hull The transitive hull.
   * @param nodes The start nodes of the transitive hull. These need to be in
   * the same order and amount as the starting side nodes in the startTable.
   * @param startSideCol The column of the result table for the startSide of the
   * hull
   * @param targetSideCol The column of the result table for the targetSide of
   * the hull
   * @param startSideTable An IdTable that holds other results. The other
   * results will be transferred to the new result table.
   * @param skipCol This column contains the Ids of the start side in the
   * startSideTable and will be skipped.
   */
  template <size_t WIDTH, size_t START_WIDTH>
  static void fillTableWithHull(IdTableStatic<WIDTH>& table, const Map& hull,
                                std::vector<Id>& nodes, size_t startSideCol,
                                size_t targetSideCol,
                                const IdTable& startSideTable, size_t skipCol);

  /**
   * @brief Fill the given table with the transitive hull.
   * This function is called if the sides are unbound or ids.
   *
   * @tparam WIDTH The number of columns of the result table.
   * @param table The result table which will be filled.
   * @param hull The transitive hull.
   * @param startSideCol The column of the result table for the startSide of the
   * hull
   * @param targetSideCol The column of the result table for the targetSide of
   * the hull
   */
  template <size_t WIDTH>
  static void fillTableWithHull(IdTableStatic<WIDTH>& table, const Map& hull,
                                size_t startSideCol, size_t targetSideCol);

  /**
   * @brief Prepare a Map and a nodes vector for the transitive hull
   * computation.
   *
   * @tparam SUB_WIDTH Number of columns of the sub table
   * @tparam SIDE_WIDTH Number of columns of the startSideTable
   * @param sub The sub table result
   * @param startSide The TransitivePathSide where the edges start
   * @param targetSide The TransitivePathSide where the edges end
   * @param startSideTable An IdTable containing the Ids for the startSide
   * @return std::pair<Map, std::vector<Id>> A Map and Id vector (nodes) for the
   * transitive hull computation
   */
  template <size_t SUB_WIDTH, size_t SIDE_WIDTH>
  std::pair<Map, std::vector<Id>> setupMapAndNodes(
      const IdTable& sub, const TransitivePathSide& startSide,
      const TransitivePathSide& targetSide,
      const IdTable& startSideTable) const;

  /**
   * @brief Prepare a Map and a nodes vector for the transitive hull
   * computation.
   *
   * @tparam SUB_WIDTH Number of columns of the sub table
   * @param sub The sub table result
   * @param startSide The TransitivePathSide where the edges start
   * @param targetSide The TransitivePathSide where the edges end
   * @return std::pair<Map, std::vector<Id>> A Map and Id vector (nodes) for the
   * transitive hull computation
   */
  template <size_t SUB_WIDTH>
  std::pair<Map, std::vector<Id>> setupMapAndNodes(
      const IdTable& sub, const TransitivePathSide& startSide,
      const TransitivePathSide& targetSide) const;

  // initialize the map from the subresult
  template <size_t SUB_WIDTH>
  Map setupEdgesMap(const IdTable& dynSub, const TransitivePathSide& startSide,
                    const TransitivePathSide& targetSide) const;

  // initialize a vector for the starting nodes (Ids)
  template <size_t WIDTH>
  static std::span<const Id> setupNodes(const IdTable& table, size_t col);

  // Copy the columns from the input table to the output table
  template <size_t INPUT_WIDTH, size_t OUTPUT_WIDTH>
  static void copyColumns(const IdTableView<INPUT_WIDTH>& inputTable,
                          IdTableStatic<OUTPUT_WIDTH>& outputTable,
                          size_t inputRow, size_t outputRow, size_t skipCol);

  // A small helper function: Insert the `value` to the set at `map[key]`.
  // As the sets all have an allocator with memory limit, this construction is a
  // little bit more involved, so this can be a separate helper function.
  void insertIntoMap(Map& map, Id key, Id value) const;
};
