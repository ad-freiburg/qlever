#ifndef QLEVER_CONSTRUCTQUERYEVALUATOR_H
#define QLEVER_CONSTRUCTQUERYEVALUATOR_H

#include <optional>
#include <string>

#include "index/IndexImpl.h"
#include "parser/data/BlankNode.h"
#include "parser/data/GraphTerm.h"

class ConstructQueryEvaluator {
  using Iri = ad_utility::triple_component::Iri;
  using Literal = ad_utility::triple_component::Literal;
  using LiteralOrIri = ad_utility::triple_component::LiteralOrIri;

 public:
  static std::optional<std::string> evaluate(const Iri& iri);

  static std::optional<std::string> evaluate(const Literal& literal,
                                             PositionInTriple role);

  static std::optional<std::string> evaluate(
      const BlankNode& node, const ConstructQueryExportContext& context);

  static std::optional<std::string> evaluateVar(
      const Variable& var, const ConstructQueryExportContext& context);

  static std::optional<std::string> evaluate(
      const GraphTerm& term, const ConstructQueryExportContext& context,
      PositionInTriple posInTriple);
};

#endif  // QLEVER_CONSTRUCTQUERYEVALUATOR_H
