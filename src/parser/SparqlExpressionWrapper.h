//
// Created by johannes on 13.05.21.
//

#ifndef QLEVER_SPARQLEXPRESSIONWRAPPER_H
#define QLEVER_SPARQLEXPRESSIONWRAPPER_H

#include <memory>
#include <vector>
#include "../util/Random.h"

namespace sparqlExpression {

class SparqlExpression;
class SparqlExpressionWrapper {
 public:
  static constexpr const char* Name = "ComplexArithmeticExpression";
  std::string getDescriptor() const { return "Arithmetic Bind"; }

  std::string asString() const {
    // Random implementation to prevent caching. TODO<joka921> :fix this
    FastRandomIntGenerator<size_t> r;
    std::string result;
    for (size_t i = 0; i < 5; ++i) {
      result += std::to_string(r());
    }
    return result;
  }
  SparqlExpressionWrapper(std::shared_ptr<SparqlExpression>&& pimpl);
  ~SparqlExpressionWrapper();
  SparqlExpressionWrapper(SparqlExpressionWrapper&&) noexcept;
  SparqlExpressionWrapper& operator=(SparqlExpressionWrapper&&) noexcept;
  SparqlExpressionWrapper(const SparqlExpressionWrapper&);
  SparqlExpressionWrapper& operator=(const SparqlExpressionWrapper&);

  std::vector<std::string*> strings();

  SparqlExpression* getImpl() { return _pimpl.get(); }

 private:
  std::shared_ptr<SparqlExpression> _pimpl;
};
}  // namespace sparqlExpression

#endif  // QLEVER_SPARQLEXPRESSIONWRAPPER_H
