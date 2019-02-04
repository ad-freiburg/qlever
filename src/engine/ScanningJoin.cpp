// Copyright 2016, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include "./ScanningJoin.h"

// _____________________________________________________________________________
ScanningJoin::ScanningJoin(QueryExecutionContext* qec,
                           const QueryExecutionTree& subtree,
                           size_t subtreeJoinCol, IndexScan::ScanType scanType)
    : IndexScan(qec, scanType) {
  _subtree = new QueryExecutionTree(subtree);
  _subtreeJoinCol = subtreeJoinCol;
}

// _____________________________________________________________________________
ScanningJoin::ScanningJoin(const ScanningJoin& other)
    : IndexScan(other._executionContext, other._type) {
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
ScanningJoin::~ScanningJoin() { delete _subtree; }

// _____________________________________________________________________________
string ScanningJoin::asString(size_t indent) const {
  std::ostringstream os;
  os << "SCANNING JOIN for the result of " << _subtree << " on col "
     << _subtreeJoinCol
     << " and the equivalent of: " << IndexScan::asString(indent);
  return os.str();
}

// _____________________________________________________________________________
size_t ScanningJoin::getResultWidth() const {
  return IndexScan::getResultWidth() - 1 + _subtree->getResultWidth();
}

// _____________________________________________________________________________
void ScanningJoin::computeResult(ResultTable* result) {
  AD_THROW(ad_semsearch::Exception::NOT_YET_IMPLEMENTED, "TODO");
  IndexScan::computeResult(result);
}
