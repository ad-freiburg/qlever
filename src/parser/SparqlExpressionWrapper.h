//
// Created by johannes on 13.05.21.
//

#ifndef QLEVER_SPARQLEXPRESSIONWRAPPER_H
#define QLEVER_SPARQLEXPRESSIONWRAPPER_H

#include <memory>
#include <vector>
#include "../util/HashMap.h"
#include "../util/HashSet.h"
#include "../util/Random.h"

namespace sparqlExpression {

using VariableColumnMap = ad_utility::HashMap<std::string, size_t>;

class SparqlExpression;
struct EvaluationInput;
class SparqlExpressionWrapper {
 public:
  static constexpr const char* Name = "ComplexArithmeticExpression";
  std::string getDescriptor() const { return "Arithmetic Bind"; }

  std::vector<std::string> getUnaggregatedVariables() const;

  bool isAggregate(
      const ad_utility::HashSet<string> groupedVariables = {}) const {
    auto unaggregatedVariables = getUnaggregatedVariables();
    for (const auto& var : unaggregatedVariables) {
      if (!groupedVariables.contains(var)) {
        return false;
      }
    }
    return true;
  }

  // Returns the variable which is counted. needed by the pattern trick.
  std::optional<std::string> isNonDistinctCountOfSingleVariable() const;

  std::string asString(const VariableColumnMap& varColMap) const;
  SparqlExpressionWrapper(std::shared_ptr<SparqlExpression>&& pimpl);
  ~SparqlExpressionWrapper();
  SparqlExpressionWrapper(SparqlExpressionWrapper&&) noexcept;
  SparqlExpressionWrapper& operator=(SparqlExpressionWrapper&&) noexcept;
  SparqlExpressionWrapper(const SparqlExpressionWrapper&);
  SparqlExpressionWrapper& operator=(const SparqlExpressionWrapper&);

  std::vector<std::string*> strings();

  SparqlExpression* getImpl() { return _pimpl.get(); }
  const SparqlExpression* getImpl() const { return _pimpl.get(); }

 private:
  std::shared_ptr<SparqlExpression> _pimpl;
};
}  // namespace sparqlExpression

#endif  // QLEVER_SPARQLEXPRESSIONWRAPPER_H
