// Copyright 2016, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include "./ScanningJoin.h"

// _____________________________________________________________________________
ScanningJoin::ScanningJoin(
    QueryExecutionContext* qec,
    const QueryExecutionTree& subtree,
    size_t subtreeJoinCol,
    IndexScan::ScanType scanType) : IndexScan(qec, scanType) {
  _subtree = new QueryExecutionTree(subtree);
  _subtreeJoinCol = subtreeJoinCol;
}

// _____________________________________________________________________________
ScanningJoin::ScanningJoin(const ScanningJoin& other) : IndexScan(
    other._executionContext, other._type) {
  _subtree = new QueryExecutionTree(*other._subtree);
  _subtreeJoinCol = other._subtreeJoinCol;
}

// _____________________________________________________________________________
ScanningJoin& ScanningJoin::operator=(const ScanningJoin& other) {
  delete _subtree;
  _subtree = new QueryExecutionTree(*other._subtree);
  _subtreeJoinCol = other._subtreeJoinCol;
  _subject = other._subject;
  _predicate = other._predicate;
  _object = other._object;
  _type = other._type;
  return *this;
}

// _____________________________________________________________________________
ScanningJoin::~ScanningJoin() {
  delete _subtree;
}

// _____________________________________________________________________________
string ScanningJoin::asString() const {
  AD_THROW(ad_semsearch::Exception::NOT_YET_IMPLEMENTED, "TODO");
  return "TODO";
}

// _____________________________________________________________________________
size_t ScanningJoin::getResultWidth() const {
  AD_THROW(ad_semsearch::Exception::NOT_YET_IMPLEMENTED, "TODO");
  return IndexScan::getResultWidth();
}

// _____________________________________________________________________________
unordered_map<string, size_t> ScanningJoin::getVariableColumns() const {
  AD_THROW(ad_semsearch::Exception::NOT_YET_IMPLEMENTED, "TODO");
  return std::unordered_map<std::__cxx11::string, size_t>();
}

// _____________________________________________________________________________
void ScanningJoin::computeResult(ResultTable* result) const {
  AD_THROW(ad_semsearch::Exception::NOT_YET_IMPLEMENTED, "TODO");
  IndexScan::computeResult(result);
}













