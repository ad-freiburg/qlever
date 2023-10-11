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
  std::optional<TreeAndCol> treeAndCol;
  // Column of the sub table where the Ids of this side are located
  size_t subCol;
  std::variant<Id, Variable> value;
  // The column in the ouput table where this side Ids are written to.
  // This member is set by the TransitivePath class
  size_t outputCol = 0;

  bool isVariable() const { return std::holds_alternative<Variable>(value); };

  bool isBound() const {
    return treeAndCol.has_value() || std::holds_alternative<Id>(value);
  };
};

class TransitivePath : public Operation {
  using Map = ad_utility::HashMap<Id, std::shared_ptr<ad_utility::HashSet<Id>>>;
  using MapIt = Map::iterator;

  std::shared_ptr<QueryExecutionTree> _subtree;
  TransitivePathSide _lhs;
  TransitivePathSide _rhs;
  size_t _resultWidth;
  size_t _minDist;
  size_t _maxDist;
  VariableToColumnMap _variableColumns;

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

  /**
   * Neither side of a tree may be bound twice
   */
  bool isBound() const;

  bool leftIsBound() const;

  bool rightIsBound() const;

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
    if (_lhs.treeAndCol.has_value()) {
      res.push_back(_lhs.treeAndCol.value().first.get());
    }
    res.push_back(_subtree.get());
    if (_rhs.treeAndCol.has_value()) {
      res.push_back(_rhs.treeAndCol.value().first.get());
    }
    return res;
  }

  /**
   * @brief Compute the transitive hull with a bound side.
   *
   * @tparam RES_WIDTH
   * @tparam SUB_WIDTH
   * @tparam OTHER_WIDTH
   * @param res
   * @param sub
   * @param startSide
   * @param targetSide
   * @param other
   */
  template <size_t RES_WIDTH, size_t SUB_WIDTH, size_t OTHER_WIDTH>
  void computeTransitivePathBound(IdTable* res, const IdTable& sub,
                                  const TransitivePathSide& startSide,
                                  const TransitivePathSide& targetSide,
                                  const IdTable& other) const;

  template <size_t RES_WIDTH, size_t SUB_WIDTH>
  void computeTransitivePath(IdTable* res, const IdTable& sub,
                             const TransitivePathSide& startSide,
                             const TransitivePathSide& targetSide) const;

 private:
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
   * @param edges An adjacency matrix, mapping Ids (nodes) to their connected
   * Ids.
   * @param nodes A list of Ids. These Ids are used as starting points for the
   * transitive hull. Thus, this parameter guides the performance of this
   * algorithm.
   * @return Map Maps each Id to its connected Ids in the transitive hull
   */
  Map transitiveHull(const Map& edges, const std::vector<Id>& nodes) const;

  /**
   * @brief Fill the given table with the transitive hull and use the
   * tableTemplate to fill in the rest of the columns.
   *
   * @tparam WIDTH The number of columns of the result table.
   * @tparam TEMP_WIDTH The number of columns of the template table.
   * @param table The result table which will be filled.
   * @param hull The transitive hull.
   * @param nodes The start nodes of the transitive hull. These need to be in
   * the same order and amount as the starting side nodes in the tableTemplate.
   * @param startSideCol The column of the result table for the startSide of the
   * hull
   * @param targetSideCol The column of the result table for the targetSide of
   * the hull
   * @param tableTemplate An IdTable that holds other results. The other results
   * will be transferred to the new result table.
   * @param skipCol This column contains the Ids of the start side in the
   * templateTable and will be skipped.
   */
  template <size_t WIDTH, size_t TEMP_WIDTH>
  static void fillTableWithHull(IdTableStatic<WIDTH>& table, Map hull,
                                std::vector<Id>& nodes, size_t startSideCol,
                                size_t targetSideCol,
                                const IdTable& tableTemplate, size_t skipCol);

  /**
   * @brief Fill the given table with the transitive hull.
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
  static void fillTableWithHull(IdTableStatic<WIDTH>& table, Map hull,
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
  static std::vector<Id> setupNodesVector(const IdTable& table, size_t col);

  // Copy the columns from the input table to the output table
  template <size_t INPUT_WIDTH, size_t OUTPUT_WIDTH>
  static void copyColumns(const IdTableView<INPUT_WIDTH>& inputTable,
                          IdTableStatic<OUTPUT_WIDTH>& outputTable,
                          size_t inputRow, size_t outputRow, size_t skipCol);
};
