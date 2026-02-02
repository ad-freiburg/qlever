// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Robin Textor-Falconi (textorr@informatik.uni-freiburg.de)

#ifndef QLEVER_SRC_PARSER_DATA_VARIABLE_H
#define QLEVER_SRC_PARSER_DATA_VARIABLE_H

#include <optional>
#include <string>
#include <utility>
#include <variant>

#include "backports/three_way_comparison.h"
#include "util/Serializer/SerializeString.h"
#include "util/Serializer/Serializer.h"

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
  std::string toSparql() const { return _name; }

  // ___________________________________________________________________________
  const std::string& name() const { return _name; }

  // Needed for consistency with the `Alias` class.
  const std::string& targetVariable() const { return _name; }

  // Converts `?someTextVar` and `?someEntityVar` into
  // `?ql_someTextVar_score_var_someEntityVar`.
  // Converts `?someTextVar` and `someFixedEntity` into
  // `?ql_someTextVar_fixedEntity_someFixedEntity`.
  // Note that if the fixed entity contains non ascii characters they are
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

  QL_DEFINE_DEFAULTED_EQUALITY_OPERATOR_LOCAL(Variable, _name)

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

  // Serialization for `Variable`s - just serialize the name.
  AD_SERIALIZE_FRIEND_FUNCTION(Variable) { serializer | arg._name; }
};

#endif  // QLEVER_SRC_PARSER_DATA_VARIABLE_H
