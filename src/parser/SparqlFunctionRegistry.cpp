// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Artem <artem@rem.sh>

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "parser/SparqlFunctionRegistry.h"

#include "rdfTypes/Iri.h"
#include "util/Exception.h"

namespace parsedQuery {

// _____________________________________________________________________________
SparqlFunctionRegistry& SparqlFunctionRegistry::get() {
  static SparqlFunctionRegistry instance;
  return instance;
}

// _____________________________________________________________________________
void SparqlFunctionRegistry::addExact(std::string iri, Factory factory) {
  AD_CONTRACT_CHECK(
      !entries_.contains(iri),
      "A custom SPARQL function is already registered for: ", iri);
  // A valid IRI is unchanged by a round-trip through `Iri`, so it matches what
  // `lookup` receives.
  AD_CONTRACT_CHECK(
      iri.size() >= 2 && iri.front() == '<' && iri.back() == '>' &&
          ad_utility::triple_component::Iri::fromIriref(iri)
                  .toStringRepresentation() == iri,
      "A custom SPARQL function IRI must be a well-formed bracketed IRI, e.g. "
      "`<http://example.org/f>`, but got: ",
      iri);
  entries_.try_emplace(std::move(iri), std::move(factory));
}

// _____________________________________________________________________________
std::optional<SparqlFunctionRegistry::Factory> SparqlFunctionRegistry::lookup(
    std::string_view iri) const {
  auto it = entries_.find(iri);
  if (it == entries_.end()) {
    return std::nullopt;
  }
  return it->second;
}

}  // namespace parsedQuery
