// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Florian Kramer (florian.kramer@mail.uni-freiburg.de)

#include "GroupBy.h"

GroupBy::GroupBy(QueryExecutionContext *qec,
                 std::shared_ptr<QueryExecutionTree> subtree) :
  Operation(qec),
  _subtree(subtree) {

}

string GroupBy::asString(size_t indent) const {
  std::ostringstream os;
  for (size_t i = 0; i < indent; ++i) { os << " "; }
  os << "GROUP_BY\n"
     << _subtree->asString(indent);
  return os.str();
}

size_t GroupBy::getResultWidth() const {
  //TODO(Florian): stub
  return 0;
}

size_t GroupBy::resultSortedOn() const {
  //TODO(Florian): stub
  return 0;
}

std::unordered_map<string, size_t> GroupBy::getVariableColumns() const {
  //TODO(Florian): stub
  return std::unordered_map<string, size_t>();
}

float GroupBy::getMultiplicity(size_t col) {
  //TODO(Florian): stub
  return 0;
}

size_t GroupBy::getSizeEstimate() {
  //TODO(Florian): stub
  return 0;
}

size_t GroupBy::getCostEstimate() {
  //TODO(Florian): stub
  return 0;
}

void GroupBy::computeResult(ResultTable* result) const {
  //TODO(Florian): stub
  AD_THROW(ad_semsearch::Exception::NOT_YET_IMPLEMENTED, "GroupBy hasn't been implemented so far.");
}
