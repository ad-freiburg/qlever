// Copyright 2025, University of Freiburg
// Chair of Algorithms and Data Structures
// Author: Generated with Claude Code

#ifndef QLEVER_SRC_PARSER_EXTERNALVALUESQUERY_H
#define QLEVER_SRC_PARSER_EXTERNALVALUESQUERY_H

#include "parser/GraphPatternOperation.h"
#include "parser/MagicServiceQuery.h"

class SparqlTriple;

namespace parsedQuery {

class ExternalValuesException : public std::runtime_error {
  // Constructors have to be explicitly inherited
  using std::runtime_error::runtime_error;
};

// The ExternalValuesQuery object holds information for external values
// specification via the SERVICE ql:external-values-#identifier# syntax.
// It stores the identifier extracted from the service IRI and the variables
// that should be bound by the external values.
struct ExternalValuesQuery : MagicServiceQuery {
  std::string identifier_;
  std::vector<Variable> variables_;

  ExternalValuesQuery() = default;
  ExternalValuesQuery(ExternalValuesQuery&& other) noexcept = default;
  ExternalValuesQuery(const ExternalValuesQuery& other) noexcept = default;
  ExternalValuesQuery& operator=(const ExternalValuesQuery& other) = default;
  ExternalValuesQuery& operator=(ExternalValuesQuery&& a) noexcept = default;
  ~ExternalValuesQuery() noexcept override = default;

  // See MagicServiceQuery - processes configuration triples
  void addParameter(const SparqlTriple& triple) override;

  // Extract identifier from service IRI like
  // <https://qlever.cs.uni-freiburg.de/external-values-myid>
  static std::string extractIdentifier(const std::string& serviceIri);
};

}  // namespace parsedQuery

#endif  // QLEVER_SRC_PARSER_EXTERNALVALUESQUERY_H
