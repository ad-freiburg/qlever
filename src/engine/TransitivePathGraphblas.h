// Copyright 2019, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Florian Kramer (florian.kramer@neptun.uni-freiburg.de)

#pragma once

#include <memory>

#include "TransitivePathBase.h"
#include "engine/GrbMatrix.h"
#include "engine/Operation.h"
#include "engine/QueryExecutionTree.h"
#include "engine/idTable/IdTable.h"
#include "util/HashMap.h"

// This struct keeps track of the mapping between Ids and matrix indices
struct IdMapping {
  bool contains(Id id) { return idMap_.contains(id); }

  size_t addId(Id id) {
    if (!idMap_.contains(id)) {
      indexMap_.push_back(id);
    }
    idMap_.try_emplace(id, indexMap_.size() - 1);
    return idMap_[id];
  }

  Id getId(size_t index) const { return indexMap_.at(index); }

  size_t getIndex(Id id) const { return idMap_.at(id); }

  size_t size() const { return indexMap_.size(); }

 private:
  ad_utility::HashMap<Id, size_t> idMap_;

  std::vector<Id> indexMap_;
};

class TransitivePathGraphblas : public TransitivePathBase {
 public:
  TransitivePathGraphblas(QueryExecutionContext* qec,
                          std::shared_ptr<QueryExecutionTree> child,
                          TransitivePathSide leftSide,
                          TransitivePathSide rightSide, size_t minDist,
                          size_t maxDist);

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

  // void computeTransitivePath(
  //     IdTable* res, const IdTable& sub, const TransitivePathSide& startSide,
  //     const TransitivePathSide& targetSide) const override;

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
  ResultTable computeResult() override;

  /**
   * @brief Compute the transitive hull of the graph. If given startNodes,
   * compute the transitive hull starting at the startNodes.
   *
   * @param graph Boolean, square, sparse, adjacency matrix. Row i, column j is
   * true, iff. there is an edge going from i to j in the graph.
   * @param startNodes Boolean, sparse, adjacency matrix, marking the start
   * nodes. There is one row for each start node. The number of columns has to
   * be equal to the number of columns of the graph matrix.
   * @return An adjacency matrix containing the transitive hull
   */
  GrbMatrix transitiveHull(const GrbMatrix& graph,
                           std::optional<GrbMatrix> startNodes) const;

  /**
   * @brief Fill the IdTable with the given transitive hull.
   *
   * @tparam WIDTH The number of columns of the result table.
   * @param table The result table which will be filled.
   * @param hull The transitive hull. Represented by a sparse, boolean adjacency
   * matrix
   * @param mapping IdMapping, which maps Ids to matrix indices and vice versa.
   * @param startSideCol The column of the result table for the startSide of the
   * hull
   * @param targetSideCol The column of the result table for the targetSide of
   * the hull
   */
  template <size_t WIDTH>
  static void fillTableWithHull(IdTableStatic<WIDTH>& table,
                                const GrbMatrix& hull, const IdMapping& mapping,
                                size_t startSideCol, size_t targetSideCol);

  /**
   * @brief Fill the IdTable with the given transitive hull. This function is
   * used in case the hull computation has one (or more) Ids as start nodes.
   *
   * @tparam WIDTH The number of columns of the result table.
   * @param table The result table which will be filled.
   * @param hull The transitive hull. Represented by a sparse, boolean adjacency
   * matrix
   * @param mapping IdMapping, which maps Ids to matrix indices and vice versa.
   * @param startNodes Ids of the start nodes.
   * @param startSideCol The column of the result table for the startSide of the
   * hull
   * @param targetSideCol The column of the result table for the targetSide of
   * the hull
   */
  template <size_t WIDTH>
  static void fillTableWithHull(IdTableStatic<WIDTH>& table,
                                const GrbMatrix& hull, const IdMapping& mapping,
                                std::span<const Id> startNodes,
                                size_t startSideCol, size_t targetSideCol);

  /**
   * @brief Fill the IdTable with the given transitive hull. This function is
   * used if the start side was already bound and there is an IdTable from which
   * data has to be copied to the result table.
   *
   * @tparam WIDTH The number of columns of the result table.
   * @tparam START_WIDTH The number of columns of the start table.
   * @param table The result table which will be filled.
   * @param hull The transitive hull. Represented by a sparse, boolean adjacency
   * matrix
   * @param mapping IdMapping, which maps Ids to matrix indices and vice versa.
   * @param startNodes Ids of the start nodes.
   * @param startSideCol The column of the result table for the startSide of the
   * hull
   * @param targetSideCol The column of the result table for the targetSide of
   * the hull
   * @param skipCol This column contains the Ids of the start side in the
   * startSideTable and will be skipped.
   */
  template <size_t WIDTH, size_t START_WIDTH>
  static void fillTableWithHull(IdTableStatic<WIDTH>& table,
                                const GrbMatrix& hull, const IdMapping& mapping,
                                const IdTable& startSideTable,
                                std::span<const Id> startNodes,
                                size_t startSideCol, size_t targetSideCol,
                                size_t skipCol);

  GrbMatrix getTargetRow(GrbMatrix& hull, size_t targetIndex) const;

  /**
   * @brief Create a boolean, sparse adjacency matrix from the given edges. The
   * edges are given as lists, where one list contains the start node of the
   * edge and the other list contains the target node of the edge.
   * Also create an IdMapping, which maps the given Ids to matrix indices.
   *
   * @param startCol Column from the IdTable, which contains edge start nodes
   * @param targetCol Column from the IdTable, which contains edge target nodes
   * @param numRows Number of rows in the IdTable
   */
  std::tuple<GrbMatrix, IdMapping> setupMatrix(std::span<const Id> startCol,
                                               std::span<const Id> targetCol,
                                               size_t numRows) const;

  /**
   * @brief Create a boolean, sparse, adjacency matrix which holds the starting
   * nodes for the transitive hull computation.
   *
   * @param startIds List of Ids where the transitive hull computation should
   * start
   * @param numRows Number of rows in the IdTable where startIds comes from
   * @param mapping An IdMapping between Ids and matrix indices
   * @return Matrix with one row for each start node
   */
  GrbMatrix setupStartNodeMatrix(std::span<const Id> startIds, size_t numRows,
                                 IdMapping mapping) const;

  // Copy the columns from the input table to the output table
  template <size_t INPUT_WIDTH, size_t OUTPUT_WIDTH>
  static void copyColumns(const IdTableView<INPUT_WIDTH>& inputTable,
                          IdTableStatic<OUTPUT_WIDTH>& outputTable,
                          size_t inputRow, size_t outputRow, size_t skipCol);
};
