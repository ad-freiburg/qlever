// Copyright 2016, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include "./ScanningJoin.h"

// _____________________________________________________________________________
ScanningJoin::ScanningJoin(QueryExecutionContext* qec,
                           shared_ptr<QueryExecutionTree> subtree,
                           size_t subtreeJoinCol, IndexScan::ScanType scanType)
    : IndexScan(qec, scanType) {
  _subtree = subtree;
  _subtreeJoinCol = subtreeJoinCol;
}

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
