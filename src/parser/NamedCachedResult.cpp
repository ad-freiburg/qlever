// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//                  Chair of Algorithms and Data Structures.

#include "parser/NamedCachedResult.h"

namespace {
// Helper function for the constructor that takes an IRI. check that the IRI has
// the expected format and extract the query name.
std::string extractQueryNameFromIri(const TripleComponent::Iri& iri) {
  auto view = asStringViewUnsafe(iri.getContent());
  AD_CORRECTNESS_CHECK(
      view.starts_with(CACHED_RESULT_WITH_NAME_PREFIX),
      "The target IRI of a named cached query must start with `",
      CACHED_RESULT_WITH_NAME_PREFIX, "`, but was `", view, "`");
  // Remove the prefix
  view.remove_prefix(CACHED_RESULT_WITH_NAME_PREFIX.size());
  return std::string{view};
}
}  // namespace
namespace parsedQuery {

// _____________________________________________________________________________
NamedCachedResult::NamedCachedResult(const TripleComponent::Iri& iri)
    : identifier_{extractQueryNameFromIri(iri)} {}

// _____________________________________________________________________________
void NamedCachedResult::addParameter(
    [[maybe_unused]] const SparqlTriple& triple) {
  throwBecauseNotEmpty();
}

// _____________________________________________________________________________
void NamedCachedResult::addGraph(
    [[maybe_unused]] const GraphPatternOperation& childGraphPattern) {
  throwBecauseNotEmpty();
}
// _____________________________________________________________________________
const std::string& NamedCachedResult::identifier() const { return identifier_; }

// _____________________________________________________________________________
void NamedCachedResult::throwBecauseNotEmpty() {
  throw std::runtime_error{
      "The body of a named cache query request must be empty"};
}
}  // namespace parsedQuery
