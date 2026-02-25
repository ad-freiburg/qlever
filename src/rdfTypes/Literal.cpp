// Copyright 2023, University of Freiburg,
//                 Chair of Algorithms and Data Structures.
// Author: Benedikt Maria Beckermann <benedikt.beckermann@dagstuhl.de>

#include "rdfTypes/Literal.h"

#include <utility>

static constexpr char quote{'"'};
static constexpr char at{'@'};
static constexpr char hat{'^'};

namespace ad_utility::triple_component {

// __________________________________________
template <bool isOwning>
BasicLiteral<isOwning>::BasicLiteral(StorageType content, size_t beginOfSuffix)
    : content_{std::move(content)}, beginOfSuffix_{beginOfSuffix} {
  AD_CORRECTNESS_CHECK(ql::starts_with(content_, quote));
  AD_CORRECTNESS_CHECK(beginOfSuffix_ >= 2);
  AD_CORRECTNESS_CHECK(content_[beginOfSuffix_ - 1] == quote);
  AD_CORRECTNESS_CHECK(beginOfSuffix_ == content_.size() ||
                       content_[beginOfSuffix] == at ||
                       content_[beginOfSuffix] == hat);
}

// __________________________________________
template <bool isOwning>
bool BasicLiteral<isOwning>::hasLanguageTag() const {
  return ql::starts_with(getSuffix(), at);
}

// __________________________________________
template <bool isOwning>
bool BasicLiteral<isOwning>::hasDatatype() const {
  return ql::starts_with(getSuffix(), hat);
}

// __________________________________________
template <bool isOwning>
NormalizedStringView BasicLiteral<isOwning>::getContent() const {
  return content().substr(1, beginOfSuffix_ - 2);
}

// __________________________________________
template <bool isOwning>
NormalizedStringView BasicLiteral<isOwning>::getDatatype() const {
  if (!hasDatatype()) {
    AD_THROW("The literal does not have an explicit datatype.");
  }
  // We don't return the enclosing <angle brackets>
  NormalizedStringView result = content();
  result.remove_prefix(beginOfSuffix_ + 3);
  result.remove_suffix(1);
  return result;
}

// __________________________________________
template <bool isOwning>
NormalizedStringView BasicLiteral<isOwning>::getLanguageTag() const {
  if (!hasLanguageTag()) {
    AD_THROW("The literal does not have an explicit language tag.");
  }
  return content().substr(beginOfSuffix_ + 1);
}

// __________________________________________
template <bool isOwning>
BasicLiteral<isOwning> BasicLiteral<isOwning>::fromStringRepresentation(
    StorageType internal) {
  // TODO<joka921> This is a little dangerous as there might be quotes in the
  // IRI which might lead to unexpected results here.
  AD_CORRECTNESS_CHECK(ql::starts_with(internal, '"'));
  auto endIdx = internal.rfind('"');
  AD_CORRECTNESS_CHECK(endIdx > 0);
  return BasicLiteral{std::move(internal), endIdx + 1};
}

// __________________________________________
template <bool isOwning>
bool BasicLiteral<isOwning>::isPlain() const {
  return beginOfSuffix_ == content_.size();
}

template class BasicLiteral<true>;
template class BasicLiteral<false>;

}  // namespace ad_utility::triple_component
