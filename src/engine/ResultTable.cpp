// Copyright 2015 - 2023, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Björn Buchhold <b.buchhold@gmail.com>
//          Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>
//          Hannah Bast <bast@cs.uni-freiburg.de>

#include "engine/ResultTable.h"

#include <cassert>

#include "engine/LocalVocab.h"

// _____________________________________________________________________________
string ResultTable::asDebugString() const {
  std::ostringstream os;
  os << "First (up to) 5 rows of result with size:\n";
  for (size_t i = 0; i < std::min<size_t>(5, idTable().size()); ++i) {
    for (size_t j = 0; j < idTable().numColumns(); ++j) {
      os << idTable()(i, j) << '\t';
    }
    os << '\n';
  }
  return std::move(os).str();
}

// _____________________________________________________________________________
std::shared_ptr<const LocalVocab>
ResultTable::getSharedLocalVocabFromNonEmptyOf(
    const ResultTable& resultTable1, const ResultTable& resultTable2) {
  const auto& localVocab1 = resultTable1.localVocab_;
  const auto& localVocab2 = resultTable2.localVocab_;
  if (!localVocab1->empty() && !localVocab2->empty()) {
    throw std::runtime_error(
        "Merging of two non-empty local vocabularies is currently not "
        "supported, please contact the developers");
  }
  return localVocab2->empty() ? localVocab1 : localVocab2;
}

// _____________________________________________________________________________
std::shared_ptr<LocalVocab> ResultTable::getCopyOfLocalVocab() const {
  return std::make_shared<LocalVocab>(localVocab_->clone());
}
