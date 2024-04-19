// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Herrmann (johannes.r.herrmann(at)gmail.com)

#include <boost/graph/adjacency_list.hpp>
#include <boost/graph/graph_selectors.hpp>
#include <memory>
#include <span>
#include <vector>

#include "engine/Operation.h"
#include "engine/QueryExecutionTree.h"
#include "global/Id.h"

typedef boost::adjacency_list<boost::vecS, boost::vecS, boost::directedS> Graph;

struct Edge {};

struct Path {
  std::vector<Edge> edges;
};

class PathSearch : public Operation {
  std::shared_ptr<QueryExecutionTree> subtree_;
  size_t resultWidth_;
  VariableToColumnMap variableColumns_;

  Graph graph_;

 public:
  PathSearch(QueryExecutionContext* qec,
             std::shared_ptr<QueryExecutionTree> subtree);

  std::vector<QueryExecutionTree*> getChildren() override;

  string getCacheKeyImpl() const override;
  string getDescriptor() const override;
  size_t getResultWidth() const override;
  void setTextLimit(size_t limit) override;

  size_t getCostEstimate() override;

  uint64_t getSizeEstimateBeforeLimit() override;
  float getMultiplicity(size_t col) override;
  bool knownEmptyResult() override;

  vector<ColumnIndex> resultSortedOn() const override;

  ResultTable computeResult() override;
  VariableToColumnMap computeVariableToColumnMap() const override;

 private:
  void buildGraph(std::span<Id> startNodes, std::span<Id> endNodes);
  std::vector<Path> findPaths() const;
};
