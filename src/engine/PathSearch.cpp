// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Herrmann (johannes.r.herrmann(at)gmail.com)

#include "PathSearch.h"

#include <boost/graph/detail/adjacency_list.hpp>
#include <memory>
#include <sstream>

#include "engine/CallFixedSize.h"
#include "util/Exception.h"

// _____________________________________________________________________________
PathSearch::PathSearch(QueryExecutionContext* qec,
                       std::shared_ptr<QueryExecutionTree> subtree,
                       PathSearchConfiguration config)
    : Operation(qec),
      subtree_(std::move(subtree)),
      graph_(),
      config_(std::move(config)) {
  AD_CORRECTNESS_CHECK(qec != nullptr);
}

// _____________________________________________________________________________
std::vector<QueryExecutionTree*> PathSearch::getChildren() {
  std::vector<QueryExecutionTree*> res;
  res.push_back(subtree_.get());
  return res;
};

// _____________________________________________________________________________
std::string PathSearch::getCacheKeyImpl() const {
  std::ostringstream os;
  AD_CORRECTNESS_CHECK(subtree_);
  os << "Subtree:\n" << subtree_->getCacheKey() << '\n';
  return std::move(os).str();
};

// _____________________________________________________________________________
string PathSearch::getDescriptor() const {
  std::ostringstream os;
  os << "PathSearch";
  return std::move(os).str();
};

// _____________________________________________________________________________
size_t PathSearch::getResultWidth() const { return resultWidth_; };

// _____________________________________________________________________________
void PathSearch::setTextLimit(size_t limit) {
  for (auto child : getChildren()) {
    child->setTextLimit(limit);
  }
};

// _____________________________________________________________________________
size_t PathSearch::getCostEstimate() {
  // TODO: Figure out a smart way to estimate cost
  return 1000;
};

// _____________________________________________________________________________
uint64_t PathSearch::getSizeEstimateBeforeLimit() {
  // TODO: Figure out a smart way to estimate size
  return 1000;
};

// _____________________________________________________________________________
float PathSearch::getMultiplicity(size_t col) {
  (void)col;
  return 1;
};

// _____________________________________________________________________________
bool PathSearch::knownEmptyResult() { return subtree_->knownEmptyResult(); };

// _____________________________________________________________________________
vector<ColumnIndex> PathSearch::resultSortedOn() const { return {}; };

// _____________________________________________________________________________
ResultTable PathSearch::computeResult() {
  shared_ptr<const ResultTable> subRes = subtree_->getResult();
  IdTable idTable{allocator()};

  const IdTable& dynSub = subRes->idTable();
  buildGraph(dynSub.getColumn(config_.startColumn_),
             dynSub.getColumn(config_.endColumn_));

  auto paths = findPaths();

  CALL_FIXED_SIZE(std::array{dynSub.numColumns()}, &PathSearch::normalizePaths,
                  this, idTable, paths);

  return {std::move(idTable), resultSortedOn(), subRes->getSharedLocalVocab()};
};

// _____________________________________________________________________________
VariableToColumnMap PathSearch::computeVariableToColumnMap() const {
  return variableColumns_;
};

// _____________________________________________________________________________
void PathSearch::buildGraph(std::span<const Id> startNodes,
                            std::span<const Id> endNodes) {
  AD_CORRECTNESS_CHECK(startNodes.size() == endNodes.size());
  for (size_t i = 0; i < startNodes.size(); i++) {
    boost::add_edge(startNodes[i].getBits(), endNodes[i].getBits(), graph_);
  }
}

// _____________________________________________________________________________
std::vector<Path> PathSearch::findPaths() const {
  switch (config_.algorithm_) {
    case ALL_PATHS:
      return allPaths();
    default:
      AD_FAIL();
  }
}

// _____________________________________________________________________________
std::vector<Path> PathSearch::allPaths() const {
  std::vector<Path> paths;
  Path path;
  AllPathsVisitor vis(config_.source_.getBits(), path, paths);
  boost::depth_first_search(graph_, boost::visitor(vis));
  return paths;
}

// _____________________________________________________________________________
template <size_t WIDTH>
void PathSearch::normalizePaths(IdTable& tableDyn,
                                std::vector<Path> paths) const {
  IdTableStatic<WIDTH> table = std::move(tableDyn).toStatic<WIDTH>();

  size_t rowIndex = 0;
  for (size_t pathIndex = 0; pathIndex < paths.size(); pathIndex++) {
    auto path = paths[pathIndex];
    for (size_t edgeIndex = 0; edgeIndex < path.size(); edgeIndex++) {
      auto edge = path.edges_[edgeIndex];
      auto [start, end] = edge.toIds();
      table.emplace_back();
      table(rowIndex, config_.startColumn_) = start;
      table(rowIndex, config_.endColumn_) = end;
      table(rowIndex, config_.pathIndexColumn_) = Id::makeFromInt(pathIndex);
      table(rowIndex, config_.edgeIndexColumn_) = Id::makeFromInt(edgeIndex);

      rowIndex++;
    }
  }

  tableDyn = std::move(table).toDynamic();
}
