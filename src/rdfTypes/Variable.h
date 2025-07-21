// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Robin Textor-Falconi (textorr@informatik.uni-freiburg.de)

#ifndef QLEVER_SRC_PARSER_DATA_VARIABLE_H
#define QLEVER_SRC_PARSER_DATA_VARIABLE_H

#include <optional>
#include <string>
#include <utility>
#include <variant>

// Forward declaration because of cyclic dependencies
// TODO<joka921> The coupling of the `Variable` with its `evaluate` methods
// is not very clean and should be refactored.
struct ConstructQueryExportContext;
enum struct PositionInTriple : int;

class Variable {
 private:
  std::string _name;

 public:
  // Create the variable from the given `name` (which must include the leading ?
  // or $). If `checkName` is set, then the variable name will be validated by
  // the SPARQL parser and an `AD_CONTRACT_CHECK` will fail if the name is not
  // valid.
  explicit Variable(std::string name, bool checkName = true);

  // TODO<joka921> There are several similar variants of this function across
  // the codebase. Unify them!

  // The `evaluate` method, which is required for the export of CONSTRUCT query
  // results depends on a lot of other code (in particular the complete
  // `Index`). For the time being, To not be forced to link this class against
  // all these types, we use the following approach: The `evaluate` method
  // refers to a static function pointer, which is initially set to a dummy
  // function. The Export module (in `ExportQueryExecutionTree.cpp`) sets this
  // pointer to the actual implementation as part of the static initialization.
  // In the future, the evaluation should be completely done outside the
  // `Variable` class.
  // ___________________________________________________________________________
  using EvaluateFuncPtr = std::optional<std::string> (*)(
      const Variable&, const ConstructQueryExportContext& context,
      [[maybe_unused]] PositionInTriple positionInTriple);

  [[nodiscard]] std::optional<std::string> evaluate(
      const ConstructQueryExportContext& context,
      [[maybe_unused]] PositionInTriple positionInTriple) const;

  static EvaluateFuncPtr& decoupledEvaluateFuncPtr();

  // ___________________________________________________________________________
  [[nodiscard]] std::string toSparql() const { return _name; }

  // ___________________________________________________________________________
  [[nodiscard]] const std::string& name() const { return _name; }

  // Needed for consistency with the `Alias` class.
  [[nodiscard]] const std::string& targetVariable() const { return _name; }

  // Converts `?someTextVar` and `?someEntityVar` into
  // `?ql_someTextVar_score_var_someEntityVar`.
  // Converts `?someTextVar` and `someFixedEntity` into
  // `?ql_someTextVar_fixedEntity_someFixedEntity`.
  // Note that if the the fixed entity contains non ascii characters they are
  // converted to numbers and escaped.
  Variable getEntityScoreVariable(
      const std::variant<Variable, std::string>& varOrEntity) const;

  // Converts `?someTextVar` and `someWord` into
  // `?ql_score_word_someTextVar_someWord.
  // Converts `?someTextVar` and `somePrefix*` into
  // `?ql_score_prefix_someTextVar_somePrefix`.
  // Note that if the word contains non ascii characters they are converted to
  // numbers and escaped.
  Variable getWordScoreVariable(std::string_view word, bool isPrefix) const;

  // Convert `?someVariable` into `?ql_matchingword_someVariable_someTerm`
  Variable getMatchingWordVariable(std::string_view term) const;

  bool operator==(const Variable&) const = default;

  // The construction of PrefilterExpressions requires a defined < order.
  bool operator<(const Variable& other) const { return _name < other._name; };

  // Make the type hashable for absl, see
  // https://abseil.io/docs/cpp/guides/hash.
  template <typename H>
  friend H AbslHashValue(H h, const Variable& v) {
    return H::combine(std::move(h), v._name);
  }

  // Formatter for use in `absl::StrJoin` (we need this in several places).
  static void AbslFormatter(std::string* out, const Variable& variable) {
    out->append(variable.name());
  }

  static bool isValidVariableName(std::string_view var);

  // The method escapes all special chars in word to "_ASCIICODE_" and appends
  // it at the end of target.
  static void appendEscapedWord(std::string_view word, std::string& target);
};

#endif  // QLEVER_SRC_PARSER_DATA_VARIABLE_H
