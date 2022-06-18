//  Copyright 2021, University of Freiburg, Chair of Algorithms and Data
//  Structures. Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

#ifndef QLEVER_SPARQLEXPRESSIONPIMPL_H
#define QLEVER_SPARQLEXPRESSIONPIMPL_H

#include <memory>
#include <vector>

#include "../../util/HashMap.h"
#include "../../util/HashSet.h"

namespace sparqlExpression {

using VariableColumnMap = ad_utility::HashMap<std::string, size_t>;

class SparqlExpression;
struct EvaluationContext;

// Hide the `SparqlExpression` implementation in a Pimpl class, so that code
// using this implementation only has to include the (small and therefore cheap
// to include) `SparqlExpressionPimpl.h`
class SparqlExpressionPimpl {
 public:
  [[nodiscard]] const std::string& getDescriptor() const;

  // Get the variables that are not aggregated by this expression.
  [[nodiscard]] std::vector<std::string> getUnaggregatedVariables() const;

  // Does this expression aggregate over all variables that are not in
  // `groupedVariables`. For example, COUNT(<subex>) always returns true.
  // COUNT(?x) + ?m returns true if and only if ?m is in `groupedVariables`.
  [[nodiscard]] bool isAggregate(
      const ad_utility::HashSet<string>& groupedVariables) const {
    // TODO<joka921> This can be std::ranges::all_of as soon as libc++ supports
    // it, or the combination of clang + libstdc++ + coroutines works.
    auto unaggregatedVariables = getUnaggregatedVariables();
    for (const auto& var : unaggregatedVariables) {
      if (!groupedVariables.contains(var)) {
        return false;
      }
    }
    return true;
  }

  // If this expression is a non-distinct count of a single variable,
  // return that variable, else return std::nullopt. This is needed by the
  // pattern trick.
  [[nodiscard]] std::optional<std::string>
  getVariableForNonDistinctCountOrNullopt() const;

  // If this expression is a single variable, return that variable, else return
  // std::nullopt. Knowing this enables some optimizations because we can
  // directly handle these trivial "expressions" without using the
  // `SparqlExpression` module.
  [[nodiscard]] std::optional<std::string> getVariableOrNullopt() const;

  // The implementation of these methods is small and straightforward, but
  // has to be in the .cpp file because `SparqlExpression` is only forward
  // declared.
  [[nodiscard]] std::string getCacheKey(
      const VariableColumnMap& variableColumnMap) const;
  explicit SparqlExpressionPimpl(std::shared_ptr<SparqlExpression>&& pimpl);
  ~SparqlExpressionPimpl();
  SparqlExpressionPimpl(SparqlExpressionPimpl&&) noexcept;
  SparqlExpressionPimpl& operator=(SparqlExpressionPimpl&&) noexcept;
  SparqlExpressionPimpl(const SparqlExpressionPimpl&);
  SparqlExpressionPimpl& operator=(const SparqlExpressionPimpl&);

  std::vector<std::string*> strings();

  SparqlExpression* getPimpl() { return _pimpl.get(); }
  [[nodiscard]] const SparqlExpression* getPimpl() const {
    return _pimpl.get();
  }

 private:
  // TODO<joka921> Why can't this be a unique_ptr.
  std::shared_ptr<SparqlExpression> _pimpl;
};
}  // namespace sparqlExpression

#endif  // QLEVER_SPARQLEXPRESSIONPIMPL_H
