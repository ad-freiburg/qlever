// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Robin Textor-Falconi (textorr@informatik.uni-freiburg.de)

#pragma once

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
  // ___________________________________________________________________________
  [[nodiscard]] std::optional<std::string> evaluate(
      const ConstructQueryExportContext& context,
      [[maybe_unused]] PositionInTriple positionInTriple) const;

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
  Variable getScoreVariable(
      const std::variant<Variable, std::string>& varOrEntity) const;

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
};
