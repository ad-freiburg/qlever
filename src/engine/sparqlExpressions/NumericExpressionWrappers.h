// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Prashanth Premakumar <prashanthp0703@gmail.com>, UFR

// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_NUMERICEXPRESSIONWRAPPERS_H
#define QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_NUMERICEXPRESSIONWRAPPERS_H

#include <type_traits>
#include <variant>

#include "backports/concepts.h"
#include "engine/sparqlExpressions/SparqlExpressionValueGetters.h"
#include "util/Forward.h"

namespace sparqlExpression::detail {

// Common wrappers for numeric SPARQL expressions.
// They convert primitive numeric functions into the `Id`- and
// `NumericValue`-based interfaces used by the expression framework.

// Takes a `Function` that returns a numeric value (integral or floating
// point) and converts it to a function, that takes the same arguments and
// returns the same result, but the return type is the `NumericValue` variant.
template <typename Function, bool nanToUndef = false>
struct NumericIdWrapper {
  Function function_{};

  template <typename... Args>
  Id operator()(Args&&... args) const {
    return makeNumericId<nanToUndef>(function_(AD_FWD(args)...));
  }
};

// Takes a `Function` that takes and returns numeric values (integral or
// floating point) and converts it to a function, that takes the same
// arguments and returns the same result, but the arguments and the return
// type are the `NumericValue` variant.
template <typename Function, bool NanOrInfToUndef = false>
struct MakeNumericExpression {
  template <typename... Args>
  Id operator()(const Args&... args) const {
    CPP_assert((concepts::same_as<std::decay_t<Args>, NumericValue> && ...));

    auto visitor = [](const auto&... t) {
      if constexpr ((... ||
                     std::is_same_v<NotNumeric, std::decay_t<decltype(t)>>)) {
        return Id::makeUndefined();
      } else {
        return makeNumericId<NanOrInfToUndef>(Function{}(t...));
      }
    };

    return std::visit(visitor, args...);
  }
};

}  // namespace sparqlExpression::detail

#endif  // QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_NUMERICEXPRESSIONWRAPPERS_H
