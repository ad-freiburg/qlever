//  Copyright 2021, University of Freiburg, Chair of Algorithms and Data
//  Structures. Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

#ifndef QLEVER_SPARQLEXPRESSIONPIMPL_H
#define QLEVER_SPARQLEXPRESSIONPIMPL_H

#include <memory>
#include <optional>
#include <vector>

#include "engine/VariableToColumnMap.h"
#include "engine/sparqlExpressions/PrefilterExpressionIndex.h"
#include "parser/data/Variable.h"
#include "util/HashSet.h"

namespace sparqlExpression {

class SparqlExpression;
struct EvaluationContext;

// Improve return type readability.
// Pair containing `PrefilterExpression` pointer and a `Variable`.
using PrefilterExprVariablePair =
    std::pair<std::unique_ptr<prefilterExpressions::PrefilterExpression>,
              Variable>;

// Hide the `SparqlExpression` implementation in a Pimpl class, so that code
// using this implementation only has to include the (small and therefore cheap
// to include) `SparqlExpressionPimpl.h`
class SparqlExpressionPimpl {
 public:
  [[nodiscard]] const std::string& getDescriptor() const;
  void setDescriptor(std::string descriptor);

  // Get the variables that are not aggregated by this expression. The variables
  // in the argument `groupedVariables` are deleted from the result (grouped
  // variables do not have to be aggregated).
  [[nodiscard]] std::vector<Variable> getUnaggregatedVariables(
      const ad_utility::HashSet<Variable>& groupedVariables = {}) const;

  // Does this expression aggregate over all variables that are not in
  // `groupedVariables`. For example, COUNT(<subex>) always returns true.
  // COUNT(?x) + ?m returns true if and only if ?m is in `groupedVariables`.
  [[nodiscard]] bool isAggregate(
      const ad_utility::HashSet<Variable>& groupedVariables) const {
    // TODO<joka921> This can be ql::ranges::all_of as soon as libc++ supports
    // it, or the combination of clang + libstdc++ + coroutines works.
    auto unaggregatedVariables = getUnaggregatedVariables();
    for (const auto& var : unaggregatedVariables) {
      if (!groupedVariables.contains(var)) {
        return false;
      }
    }
    return true;
  }

  // Returns true iff this expression contain one of the aggregate expressions
  // SUM, AVG, COUNT, etc. in any form.
  bool containsAggregate() const;

  struct VariableAndDistinctness {
    ::Variable variable_;
    bool isDistinct_;
  };
  // TODO<joka921> Comment out of sync.
  // If this expression is a non-distinct count of a single variable,
  // return that variable, else return std::nullopt. This is needed by the
  // pattern trick.
  [[nodiscard]] std::optional<VariableAndDistinctness> getVariableForCount()
      const;

  // If this expression is a single variable, return that variable, else return
  // std::nullopt. Knowing this enables some optimizations because we can
  // directly handle these trivial "expressions" without using the
  // `SparqlExpression` module.
  [[nodiscard]] std::optional<::Variable> getVariableOrNullopt() const;

  // The implementation of these methods is small and straightforward, but
  // has to be in the .cpp file because `SparqlExpression` is only forward
  // declared.
  [[nodiscard]] std::string getCacheKey(
      const VariableToColumnMap& variableToColumnMap) const;
  SparqlExpressionPimpl(std::shared_ptr<SparqlExpression>&& pimpl,
                        std::string descriptor);
  ~SparqlExpressionPimpl();
  SparqlExpressionPimpl(SparqlExpressionPimpl&&) noexcept;
  SparqlExpressionPimpl& operator=(SparqlExpressionPimpl&&) noexcept;
  SparqlExpressionPimpl(const SparqlExpressionPimpl&);
  SparqlExpressionPimpl& operator=(const SparqlExpressionPimpl&);

  std::vector<const Variable*> containedVariables() const;

  // Return true iff the `Variable` is used inside the expression.
  bool isVariableContained(const Variable&) const;

  // If `this` is an expression of the form `LANG(?variable) = "language"`,
  // return the variable and the language. Else return `std::nullopt`.
  struct LangFilterData {
    Variable variable_;
    std::string language_;
  };
  std::optional<LangFilterData> getLanguageFilterExpression() const;

  // Return true iff the `LANG()` function is used inside this expression.
  bool containsLangExpression() const;

  // Return the size and cost estimate for this expression if it is used as the
  // expression of a `FILTER` clause given that the input has `inputSize` many
  // elements and the input is sorted by the variable `firstSortedVariable`.
  // `std::nullopt` for the second argument means, that the input is not sorted
  // at all.
  struct Estimates {
    size_t sizeEstimate;
    size_t costEstimate;
  };
  Estimates getEstimatesForFilterExpression(
      uint64_t inputSizeEstimate,
      const std::optional<Variable>& primarySortKeyVariable);

  // For a concise description of this method and its functionality, refer to
  // the corresponding declaration in SparqlExpression.h.
  std::vector<PrefilterExprVariablePair> getPrefilterExpressionForMetadata()
      const;

  SparqlExpression* getPimpl() { return _pimpl.get(); }
  [[nodiscard]] const SparqlExpression* getPimpl() const {
    return _pimpl.get();
  }

  // Create a `SparqlExpressionPimpl` from a single variable.
  static SparqlExpressionPimpl makeVariableExpression(const Variable& variable);

  // Convenience functions, that delegate to the respective `SparqlExpression`.
  std::vector<const SparqlExpression*> getExistsExpressions() const;
  std::vector<SparqlExpression*> getExistsExpressions();

 private:
  // TODO<joka921> Why can't this be a unique_ptr.
  std::shared_ptr<SparqlExpression> _pimpl;
};
}  // namespace sparqlExpression

#endif  // QLEVER_SPARQLEXPRESSIONPIMPL_H
