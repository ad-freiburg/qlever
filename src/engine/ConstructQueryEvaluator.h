#ifndef QLEVER_CONSTRUCTQUERYEVALUATOR_H
#define QLEVER_CONSTRUCTQUERYEVALUATOR_H

#include <optional>
#include <string>

#include "index/IndexImpl.h"
#include "parser/data/BlankNode.h"
#include "rdfTypes/Iri.h"
#include "rdfTypes/Literal.h"
#include "rdfTypes/Variable.h"

class ConstructQueryEvaluator {
  using Iri = ad_utility::triple_component::Iri;
  using Literal = ad_utility::triple_component::Literal;
  using LiteralOrIri = ad_utility::triple_component::LiteralOrIri;

 public:
  static std::optional<std::string> evaluate(const Iri& iri);

  static std::optional<std::string> evaluate(const Literal& literal,
                                             PositionInTriple posInTriple);

  static std::optional<std::string> evaluate(
      const BlankNode& node, const ConstructQueryExportContext& context);

  static std::optional<std::string> evaluate(
      const Variable& var, const ConstructQueryExportContext& context);
};

#endif  // QLEVER_CONSTRUCTQUERYEVALUATOR_H
