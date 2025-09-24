// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//                  Chair of Algorithms and Data Structures.

#include "parser/NamedCachedQuery.h"

namespace {
// Helper function for the constructor that takes an IRI. check that the IRI has
// the expected format and extract the query name.
std::string extractQueryNameFromIri(const TripleComponent::Iri& iri) {
  auto view = asStringViewUnsafe(iri.getContent());
  AD_CORRECTNESS_CHECK(
      view.starts_with(NAMED_CACHED_QUERY_PREFIX),
      "The target IRI of a named cached query must start with `",
      NAMED_CACHED_QUERY_PREFIX, "`, but was `", view, "`");
  // Remove the prefix
  view.remove_prefix(NAMED_CACHED_QUERY_PREFIX.size());
  return std::string{view};
}
}  // namespace
namespace parsedQuery {

// _____________________________________________________________________________
NamedCachedQuery::NamedCachedQuery(const TripleComponent::Iri& iri)
    : identifier_{extractQueryNameFromIri(iri)} {}

// _____________________________________________________________________________
void NamedCachedQuery::addParameter(
    [[maybe_unused]] const SparqlTriple& triple) {
  throwBecauseNotEmpty();
}

// _____________________________________________________________________________
void NamedCachedQuery::addGraph(
    [[maybe_unused]] const GraphPatternOperation& childGraphPattern) {
  throwBecauseNotEmpty();
}
// _____________________________________________________________________________
const std::string& NamedCachedQuery::identifier() const { return identifier_; }

// _____________________________________________________________________________
void NamedCachedQuery::throwBecauseNotEmpty() {
  throw std::runtime_error{
      "The body of a named cache query request must be empty"};
}
}  // namespace parsedQuery
