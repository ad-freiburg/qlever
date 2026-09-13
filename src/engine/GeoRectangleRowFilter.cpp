// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Hannah Bast <bast@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "engine/GeoRectangleRowFilter.h"

#include "engine/QueryExecutionTree.h"

// _____________________________________________________________________________
GeoRectangleRowFilter::GeoRectangleRowFilter(
    QueryExecutionContext* ctx, std::shared_ptr<QueryExecutionTree> child,
    ColumnIndex geometryColumn, const ad_utility::GeoRectangle& rectangle)
    : Operation{ctx},
      child_{std::move(child)},
      geometryColumn_{geometryColumn},
      rectangle_{rectangle},
      prefilter_{getIndex().getVocab().getGeoCellGrid(), rectangle} {
  AD_CONTRACT_CHECK(geometryColumn_ < child_->getResultWidth());
  // The result of a block-prefiltered scan does not match its cache key, and
  // neither does the result of this filter on top of it.
  if (!child_->getRootOperation()->canResultBeCached()) {
    disableStoringInCache();
  }
}

// _____________________________________________________________________________
std::vector<QueryExecutionTree*> GeoRectangleRowFilter::getChildrenImpl()
    const {
  return {child_.get()};
}

// _____________________________________________________________________________
std::string GeoRectangleRowFilter::getCacheKeyImpl() const {
  return absl::StrCat("GeoRectangleRowFilter(col ", geometryColumn_, ", [",
                      rectangle_.minLng_, ", ", rectangle_.minLat_, ", ",
                      rectangle_.maxLng_, ", ", rectangle_.maxLat_, "], ",
                      child_->getCacheKey(), ")");
}

// _____________________________________________________________________________
std::string GeoRectangleRowFilter::getDescriptor() const {
  return "Geo rectangle row filter";
}

// _____________________________________________________________________________
size_t GeoRectangleRowFilter::getResultWidth() const {
  return child_->getResultWidth();
}

// _____________________________________________________________________________
size_t GeoRectangleRowFilter::getCostEstimate() {
  return child_->getCostEstimate() + child_->getSizeEstimate();
}

// _____________________________________________________________________________
uint64_t GeoRectangleRowFilter::getSizeEstimateBeforeLimit() {
  return child_->getSizeEstimate();
}

// _____________________________________________________________________________
float GeoRectangleRowFilter::getMultiplicity(size_t col) {
  return child_->getMultiplicity(col);
}

// _____________________________________________________________________________
bool GeoRectangleRowFilter::knownEmptyResult() {
  return child_->knownEmptyResult();
}

// _____________________________________________________________________________
std::unique_ptr<Operation> GeoRectangleRowFilter::cloneImpl() const {
  return std::make_unique<GeoRectangleRowFilter>(
      getExecutionContext(), child_->clone(), geometryColumn_, rectangle_);
}

// _____________________________________________________________________________
std::vector<ColumnIndex> GeoRectangleRowFilter::resultSortedOn() const {
  return child_->resultSortedOn();
}

// _____________________________________________________________________________
VariableToColumnMap GeoRectangleRowFilter::computeVariableToColumnMap() const {
  return child_->getVariableColumns();
}

// _____________________________________________________________________________
void GeoRectangleRowFilter::filterTable(IdTable& table) const {
  auto column = table.getColumn(geometryColumn_);
  size_t numKept = 0;
  for (size_t row = 0; row < table.numRows(); ++row) {
    if (prefilter_.canBeSkipped(column[row])) {
      continue;
    }
    if (numKept != row) {
      for (auto col : table.getColumns()) {
        col[numKept] = col[row];
      }
    }
    ++numKept;
  }
  table.resize(numKept);
  checkCancellation();
}

// _____________________________________________________________________________
Result GeoRectangleRowFilter::computeResult(bool requestLaziness) {
  auto res = child_->getResult(requestLaziness);
  if (res->isFullyMaterialized()) {
    IdTable table = res->idTableView().clone();
    filterTable(table);
    return {std::move(table), resultSortedOn(), res->getSharedLocalVocab()};
  }
  return {Result::LazyResult{ad_utility::CachingTransformInputRange(
              res->idTables(),
              [this](auto& tableAndVocab) {
                filterTable(tableAndVocab.idTable_);
                return std::move(tableAndVocab);
              })},
          resultSortedOn()};
}
