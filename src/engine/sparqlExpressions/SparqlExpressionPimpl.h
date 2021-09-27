//
// Created by johannes on 13.05.21.
//

#ifndef QLEVER_SPARQLEXPRESSIONPIMPL_H
#define QLEVER_SPARQLEXPRESSIONPIMPL_H

#include <memory>
#include <vector>

#include "../../util/HashMap.h"
#include "../../util/HashSet.h"
#include "../../util/Random.h"

namespace sparqlExpression {

using VariableColumnMap = ad_utility::HashMap<std::string, size_t>;

class SparqlExpression;
struct EvaluationContext;
class SparqlExpressionPimpl {
 public:
  static constexpr const char* Name = "ComplexArithmeticExpression";
  const std::string& getDescriptor() const;

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
  SparqlExpressionPimpl(std::shared_ptr<SparqlExpression>&& pimpl);
  ~SparqlExpressionPimpl();
  SparqlExpressionPimpl(SparqlExpressionPimpl&&) noexcept;
  SparqlExpressionPimpl& operator=(SparqlExpressionPimpl&&) noexcept;
  SparqlExpressionPimpl(const SparqlExpressionPimpl&);
  SparqlExpressionPimpl& operator=(const SparqlExpressionPimpl&);

  std::vector<std::string*> strings();

  SparqlExpression* getImpl() { return _pimpl.get(); }
  const SparqlExpression* getImpl() const { return _pimpl.get(); }

 private:
  std::shared_ptr<SparqlExpression> _pimpl;
};
}  // namespace sparqlExpression

#endif  // QLEVER_SPARQLEXPRESSIONPIMPL_H
