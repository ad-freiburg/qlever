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
  bool nonexistingVocabEntryFound = false;
  auto getId =
      [&index, &nonexistingVocabEntryFound](
          const std::optional<TripleComponent>& tc) -> std::optional<Id> {
    if (!tc.has_value()) {
      return std::nullopt;
    }
    auto id = tc.value().toValueId(index.getVocab());
    if (!id.has_value()) {
      nonexistingVocabEntryFound = true;
    }
    return id;
  };
  std::optional<Id> col0Id = getId(col0_);
  std::optional<Id> col1Id = getId(col1_);
  std::optional<Id> col2Id = getId(col2_);

  if (nonexistingVocabEntryFound) {
    return std::nullopt;
  }
  return ScanSpecification{col0Id, col1Id, col2Id};
}

// ____________________________________________________________________________
ScanSpecificationAsTripleComponent::ScanSpecificationAsTripleComponent(T col0,
                                                                       T col1,
                                                                       T col2) {
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

  if (!col0.has_value()) {
    AD_CONTRACT_CHECK(!col1.has_value());
  }
  if (!col1.has_value()) {
    AD_CONTRACT_CHECK(!col2.has_value());
  }
}
