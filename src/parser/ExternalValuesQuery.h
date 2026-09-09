// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR

// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_PARSER_EXTERNALVALUESQUERY_H
#define QLEVER_SRC_PARSER_EXTERNALVALUESQUERY_H

#include "parser/MagicServiceQuery.h"
#include "util/Allocator.h"
#include "util/AllocatorTypes.h"

class SparqlTriple;

namespace parsedQuery {
class ExternalValuesException : public std::runtime_error {
  // Constructors have to be explicitly inherited.
  using std::runtime_error::runtime_error;
};

// The `ExternalValuesQuery` object holds information for the
// `ExternalValues`. It is specified via the SERVICE
// <ql:external-values/> syntax. The `name` and `variable` are specified
// as configuration triples:
// SELECT * {
//   SERVICE <https://qlever.cs.uni-freiburg.de/external-values/> {
//     [] <name> "myId";
//        <variable> ?x, ?y.
//   }
// }
// Alternatively, the `name` can also be specified directly in the IRI as
// `<ql:external-values-#name#>` but that syntax is deprecated as it is
// inconsistent with the other magic service IRIs and is only kept for backward
// compatibility with code already deployed by BMW.
struct ExternalValuesQuery : MagicServiceQuery {
  std::string name_;
  qlever::vector<Variable> variables_;

  // `allocator` is the real, query-execution-bound allocator that
  // `variables_` is routed through. Every caller that constructs an
  // `ExternalValuesQuery` must supply this explicitly (there is no implicit
  // unlimited-allocator fallback).
  ExternalValuesQuery(const TripleComponent::Iri& serviceIri,
                      qlever::Allocator<Id> allocator)
      : name_(extractName(serviceIri.toStringRepresentation())),
        variables_{std::move(allocator)} {}

  // Mainly used for testing.
  explicit ExternalValuesQuery(qlever::Allocator<Id> allocator)
      : variables_{std::move(allocator)} {}

  // See MagicServiceQuery - processes configuration triples.
  void addParameter(const SparqlTriple& triple) override;

  // Validate that the name and variables are set.
  void validate() const override;

  std::string_view name() const override { return "external values query"; }

  // Extract name from service IRI like
  // <https://qlever.cs.uni-freiburg.de/external-values-myid>, required for
  // backward compatibility.
  static std::string extractName(const std::string& serviceIri);
};

}  // namespace parsedQuery

#endif  // QLEVER_SRC_PARSER_EXTERNALVALUESQUERY_H
