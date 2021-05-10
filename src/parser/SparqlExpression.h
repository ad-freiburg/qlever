//
// Created by johannes on 09.05.21.
//

#ifndef QLEVER_SPARQLEXPRESSION_H
#define QLEVER_SPARQLEXPRESSION_H

#include <vector>
#include "../engine/ResultTable.h"
#include "../global/Id.h"

struct

class conditionalOrExpression;
class conditionalAndExpression;
class relationalExpression;
class multiplicativeExpression {
 public:
  enum class Type {MULTIPLY, DIVIDE};
  std::vector<unaryExpression> _children;
  auto evaluate() {
    if (_children.size() == 1) {
      return _chilren[0].evaluate();
    }
  }

};
class additiveExpression;


class SparqlExpression {};

#endif  // QLEVER_SPARQLEXPRESSION_H
