#ifndef QLEVER_CONSTRUCTQUERYEVALUATOR_H
#define QLEVER_CONSTRUCTQUERYEVALUATOR_H


#include "rdfTypes/Iri.h"
#include "rdfTypes/Variable.h"
#include "rdfTypes/Literal.h"
#include "parser/data/BlankNode.h"
#include "index/IndexImpl.h"

#include <optional>
#include <string>


class ConstructQueryEvaluator {
  using Iri = ad_utility::triple_component::Iri;
  using Literal = ad_utility::triple_component::Literal;
  using LiteralOrIri = ad_utility::triple_component::LiteralOrIri;

 public:
  static std::optional<std::string> evaluate(const Iri& iri);

  static std::optional<std::string> evaluate(
      const Literal& literal,
      PositionInTriple posInTriple);

  static std::optional<std::string> evaluate(
        const BlankNode& node,
        const ConstructQueryExportContext& context);

 private:

  // Convert the `id` to a human-readable string. The `index` is used to resolve
  // `Id`s with datatype `VocabIndex` or `TextRecordIndex`. The `localVocab` is
  // used to resolve `Id`s with datatype `LocalVocabIndex`. The `escapeFunction`
  // is applied to the resulting string if it is not of a numeric type.
  //
  // Return value: If the `Id` encodes a numeric value (integer, double, etc.)
  // then the `string` (first element of the pair) will be the number as a
  // string without quotation marks, and the second element of the pair will
  // contain the corresponding XSD-datatype as an URI. For all other values and
  // datatypes, the second element of the pair will be empty and the first
  // element will have the format `"stringContent"^^datatypeUri`. If the `id`
  // holds the `Undefined` value, then `std::nullopt` is returned.
  //
  // Note: This function currently has to be public because the
  // `Variable::evaluate` function calls it for evaluating CONSTRUCT queries.
  //
  // TODO<joka921> Make it private again as soon as the evaluation of construct
  // queries is completely performed inside this module.
  template <bool removeQuotesAndAngleBrackets = false,
      bool returnOnlyLiterals = false,
      typename EscapeFunction = ql::identity>
  static std::optional<std::pair<std::string, const char*>> idToStringAndType(
      const Index& index, Id id, const LocalVocab& localVocab,
      EscapeFunction&& escapeFunction = EscapeFunction{});

  // Return the corresponding blank node string representation for the export if
  // this iri is a blank node iri. Otherwise, return std::nullopt.
  static std::optional<std::string> blankNodeIriToString(
      const ad_utility::triple_component::Iri& iri);


  static LocalVocabEntry::LiteralOrIri getLiteralOrIriFromVocabIndex(
      const IndexImpl& index,
      Id id,
      const LocalVocab& localVocab);

  static LiteralOrIri encodedIdToLiteralOrIri(Id id, const IndexImpl& index);

  static std::optional<std::pair<std::string, const char*>>
  idToStringAndTypeForEncodedValue(Id id);

 public:
  static std::optional<std::string> evaluate(
      const Variable& var,
      const ConstructQueryExportContext& context);
};

#endif  // QLEVER_CONSTRUCTQUERYEVALUATOR_H
