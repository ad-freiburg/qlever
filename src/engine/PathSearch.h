// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Herrmann (johannes.r.herrmann(at)gmail.com)

#include <boost/graph/adjacency_list.hpp>
#include <memory>
#include <vector>

#include "engine/QueryExecutionContext.h"

struct Edge {};

struct Path {
  std::vector<Edge> edges;
};

class PathSearch : public Operation {
  std::shared_ptr<QueryExecutionTree> subtree_;
  boost::adjacency_list<> graph_;

 public:
  PathSearch(QueryExecutionContext* qec,
             std::shared_ptr<QueryExecutionTree> child);

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
  std::vector<Path> findPaths();
};
