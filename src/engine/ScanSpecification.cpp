//  Copyright 2024, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include "engine/ScanSpecification.h"

#include "index/IndexImpl.h"

// ____________________________________________________________________________
std::optional<ScanSpecification>
ScanSpecificationAsTripleComponent::toScanSpecification(
    const IndexImpl& index) const {
  // TODO<C++23> Use `std::optional::transform`.
  // TODO<SPARQL UPDATE>: We can also have LocalVocab entries is the
  // ScanSpecification.
  std::optional<Id> col0Id = col0_.has_value()
                                 ? col0_.value().toValueId(index.getVocab())
                                 : std::nullopt;
  std::optional<Id> col1Id = col1_.has_value()
                                 ? col1_.value().toValueId(index.getVocab())
                                 : std::nullopt;
  std::optional<Id> col2Id = col2_.has_value()
                                 ? col2_.value().toValueId(index.getVocab())
                                 : std::nullopt;
  if (!col0Id.has_value() || (col1_.has_value() && !col1Id.has_value()) ||
      (col2_.has_value() && !col2Id.has_value())) {
    return std::nullopt;
  }
  return ScanSpecification{col0Id, col1Id, col2Id};
}

// ____________________________________________________________________________
ScanSpecificationAsTripleComponent::ScanSpecificationAsTripleComponent(T col0,
                                                                       T col1,
                                                                       T col2) {
  auto isUnbound = [](const T& t) {
    return !t.has_value() || t.value().isVariable();
  };
  if (isUnbound(col0)) {
    AD_CONTRACT_CHECK(isUnbound(col1));
  }
  if (isUnbound(col1)) {
    AD_CONTRACT_CHECK(isUnbound(col2));
  }
  auto toNulloptIfVariable = [](T& tc) -> std::optional<TripleComponent> {
    if (tc.has_value() && tc.value().isVariable()) {
      return std::nullopt;
    } else {
      return std::move(tc);
    }
  };
  col0_ = toNulloptIfVariable(col0);
  col1_ = toNulloptIfVariable(col1);
  col2_ = toNulloptIfVariable(col2);
}
