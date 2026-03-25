// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR

// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_PARSER_EXTERNALVALUESQUERY_H
#define QLEVER_SRC_PARSER_EXTERNALVALUESQUERY_H

#include "parser/MagicServiceQuery.h"

class SparqlTriple;

namespace parsedQuery {
class ExternalValuesException : public std::runtime_error {
  // Constructors have to be explicitly inherited.
  using std::runtime_error::runtime_error;
};

// The `ExternalValuesQuery` object holds information for the
// `ExternallySpecifiedValues`. It is specified via the SERVICE
// <ql:external-values/> syntax. The `identifier` and `variables` are specified
// as configuration triples:
// SELECT * {
//   SERVICE <https://qlever.cs.uni-freiburg.de/external-values/> {
//     [] <identifier> "myId";
//        <variables> ?x, ?y.
//   }
// }
// Alternatively, the `identifier` can also specified directly in the IRI as
// `<ql:external-values-#identifier#>` but that syntax is deprecated as it is
// inconsistent with the other magic service IRIs and is only kept for backward
// compatibility with code already deployed by BMW.
struct ExternalValuesQuery : MagicServiceQuery {
  std::string identifier_;
  std::vector<Variable> variables_;

  explicit ExternalValuesQuery(const TripleComponent::Iri& serviceIri)
      : identifier_(extractIdentifier(serviceIri.toStringRepresentation())) {}

  // Default constructor, mainly used for testing.
  ExternalValuesQuery() = default;

  // See MagicServiceQuery - processes configuration triples.
  void addParameter(const SparqlTriple& triple) override;

  // Validate that the identifier and variables are set.
  void validate() const override;

  std::string_view name() const override { return "external values query"; }

  // Extract identifier from service IRI like
  // <https://qlever.cs.uni-freiburg.de/external-values-myid>, required for the
  // compatibility.
  static std::string extractIdentifier(const std::string& serviceIri);
};

}  // namespace parsedQuery

#endif  // QLEVER_SRC_PARSER_EXTERNALVALUESQUERY_H
