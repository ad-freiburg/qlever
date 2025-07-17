// Copyright 2025, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include "parser/UpdateTriples.h"

namespace updateClause {

// _____________________________________________________________________________
UpdateTriples::UpdateTriples(Vec triples, LocalVocab localVocab)
    : triples_{std::move(triples)}, localVocab_{std::move(localVocab)} {}

// _____________________________________________________________________________
UpdateTriples::UpdateTriples(const UpdateTriples& rhs)
    : triples_{rhs.triples_}, localVocab_(rhs.localVocab_.clone()) {}

// _____________________________________________________________________________
UpdateTriples& UpdateTriples::operator=(const UpdateTriples& rhs) {
  if (this == &rhs) {
    return *this;
  }
  triples_ = rhs.triples_;
  localVocab_ = rhs.localVocab_.clone();
  return *this;
}
}  // namespace updateClause
