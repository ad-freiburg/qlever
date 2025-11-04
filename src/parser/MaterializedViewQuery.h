// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Christoph Ullinger <ullingec@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#ifndef QLEVER_SRC_PARSER_MATERIALIZEDVIEWQUERY_H
#define QLEVER_SRC_PARSER_MATERIALIZEDVIEWQUERY_H

#include "parser/MagicServiceQuery.h"

namespace parsedQuery {

struct MaterializedViewQuery : MagicServiceQuery {
 public:
  std::optional<std::string> viewName_;
  std::optional<TripleComponent> scanCol_;
  ad_utility::HashMap<Variable, Variable> requestedVariables_;

  MaterializedViewQuery() = default;

  // Initialize using magic predicate
  explicit MaterializedViewQuery(const SparqlTriple& triple);

  void addParameter(const SparqlTriple& triple) override;

 private:
  void setViewName(const TripleComponent& object);
  void setScanCol(const TripleComponent& object);
  void addPayloadVariable(const Variable& column,
                          const TripleComponent& object);

  // TODO support special predicate "[Scan for index column Var or Lit/Iri]
  // matView:nameOfView:nameOfReqCol ?outVar"
};

}  // namespace parsedQuery

#endif  // QLEVER_SRC_PARSER_MATERIALIZEDVIEWQUERY_H
