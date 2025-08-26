//  Copyright 2024, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include "index/ScanSpecification.h"

#include "index/Index.h"
#include "index/IndexImpl.h"

// ____________________________________________________________________________
ScanSpecification ScanSpecificationAsTripleComponent::toScanSpecification(
    const Index& index) const {
  return toScanSpecification(index.getImpl());
}

// ____________________________________________________________________________
ScanSpecification ScanSpecificationAsTripleComponent::toScanSpecification(
    const IndexImpl& index) const {
  LocalVocab localVocab;
  auto getId =
      [&index, &localVocab](
          const std::optional<TripleComponent>& tc) -> std::optional<Id> {
    // TODO<C++23> Use `std::optional::transform`.
    if (!tc.has_value()) {
      return std::nullopt;
    }
    return TripleComponent{tc.value()}.toValueId(index.getVocab(), localVocab,
                                                 index.encodedIriManager());
  };
  std::optional<Id> col0Id = getId(col0_);
  std::optional<Id> col1Id = getId(col1_);
  std::optional<Id> col2Id = getId(col2_);

  ScanSpecification::Graphs graphsToFilter = std::nullopt;
  if (graphsToFilter_.has_value()) {
    graphsToFilter.emplace();
    for (const auto& graph : graphsToFilter_.value()) {
      graphsToFilter->insert(getId(graph).value());
    }
  }
  return {col0Id, col1Id, col2Id, std::move(localVocab),
          std::move(graphsToFilter)};
}

// ____________________________________________________________________________
ScanSpecificationAsTripleComponent::ScanSpecificationAsTripleComponent(
    T col0, T col1, T col2, Graphs graphsToFilter)
    : graphsToFilter_{std::move(graphsToFilter)} {
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

  if (!col0_.has_value()) {
    AD_CONTRACT_CHECK(!col1_.has_value());
  }
  if (!col1_.has_value()) {
    AD_CONTRACT_CHECK(!col2_.has_value());
  }
}

// ____________________________________________________________________________
size_t ScanSpecificationAsTripleComponent::numColumns() const {
  auto i = [](const auto& x) -> size_t {
    return static_cast<size_t>(x.has_value());
  };
  return 3 - i(col0_) - i(col1_) - i(col2_);
}

// _____________________________________________________________________________
void ScanSpecification::validate() const {
  auto forEach = [this](auto f) {
    f(col0Id_);
    f(col1Id_);
    f(col2Id_);
  };

  auto checkNulloptImpliesFollowingNullopt = [nulloptFound =
                                                  false](const T& id) mutable {
    if (nulloptFound) {
      AD_CONTRACT_CHECK(!id.has_value());
    }
    nulloptFound = !id.has_value();
  };
  forEach(checkNulloptImpliesFollowingNullopt);
}
