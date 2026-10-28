// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.
//
// Copyright 2025, Bayerische Motoren Werke Aktiengesellschaft (BMW AG)

// The `SUBSTR`, `STRAFTER`, and `STRBEFORE` expressions, which all return a
// substring of their input literal. They are split from `StringExpressions.cpp`
// to reduce the memory needed for compiling and statically analyzing a single
// translation unit.

#include "engine/sparqlExpressions/StringExpressionsHelper.h"
#include "util/StringUtils.h"

namespace sparqlExpression {
namespace detail::string_expressions {

// SUBSTR
class SubstrImpl {
 private:
  static bool isNan(NumericValue n) {
    auto ptr = std::get_if<double>(&n);
    return ptr != nullptr && std::isnan(*ptr);
  }

  // Round an integer or floating point to the nearest integer according to the
  // SPARQL standard. This means that -1.5 is rounded to -1.
  static constexpr auto round = [](const auto& value) -> int64_t {
    using T = std::decay_t<decltype(value)>;
    if constexpr (ql::concepts::floating_point<T>) {
      if (value < 0) {
        return static_cast<int64_t>(-std::round(-value));
      } else {
        return static_cast<int64_t>(std::round(value));
      }
    } else if constexpr (concepts::integral<T>) {
      return static_cast<int64_t>(value);
    } else {
      AD_FAIL();
    }
  };

 public:
  IdOrLiteralOrIri operator()(
      std::optional<ad_utility::triple_component::Literal> s,
      NumericValue start, NumericValue length) const {
    if (!s.has_value() || std::holds_alternative<NotNumeric>(start) ||
        std::holds_alternative<NotNumeric>(length)) {
      return Id::makeUndefined();
    }

    if (isNan(start) || isNan(length)) {
      return toLiteral("");
    }

    // In SPARQL indices are 1-based, but the implementation we use is 0 based,
    // hence the `- 1`.
    int64_t startInt = std::visit(round, start) - 1;
    int64_t lengthInt = std::visit(round, length);

    // If the starting position is negative, we have to correct the length.
    if (startInt < 0) {
      lengthInt += startInt;
      startInt = 0;
    }
    if (lengthInt < 0) {
      lengthInt = 0;
    }
    const auto& str = s.value().getContent();
    auto substr = ad_utility::getUTF8Substring(str, startInt, lengthInt);

    s.value().setSubstr(substr.data() - str.data(), substr.size());
    return LiteralOrIri(std::move(s.value()));
  }
};

using SubstrExpression =
    LiteralExpressionImpl<3, SubstrImpl, NumericValueGetter,
                          NumericValueGetter>;

// STRAFTER / STRBEFORE
template <bool isStrAfter>
struct StrAfterOrBeforeImpl {
  IdOrLiteralOrIri operator()(
      std::optional<ad_utility::triple_component::Literal> optLiteral,
      std::optional<ad_utility::triple_component::Literal> optPattern) const {
    if (!optPattern.has_value() || !optLiteral.has_value()) {
      return Id::makeUndefined();
    }
    auto& literal = optLiteral.value();
    const auto& patternLit = optPattern.value();
    // Check if arguments are compatible with their language tags.
    if (patternLit.hasLanguageTag() &&
        (!literal.hasLanguageTag() ||
         literal.getLanguageTag() != patternLit.getLanguageTag())) {
      return Id::makeUndefined();
    }
    const auto& pattern = optPattern.value().getContent();
    //  Required by the SPARQL standard.
    if (pattern.empty()) {
      if (isStrAfter) {
        return LiteralOrIri(std::move(literal));
      } else {
        literal.setSubstr(0, 0);
        return LiteralOrIri(std::move(literal));
      }
    }
    auto literalContent = literal.getContent();
    auto pos = literalContent.find(pattern);
    if (pos >= literalContent.size()) {
      return toLiteral("");
    }
    if constexpr (isStrAfter) {
      literal.setSubstr(pos + pattern.size(),
                        literalContent.size() - pos - pattern.size());
    } else {
      // STRBEFORE
      literal.setSubstr(0, pos);
    }
    return LiteralOrIri(std::move(literal));
  }
};

using StrAfterExpression =
    LiteralExpressionImpl<2, StrAfterOrBeforeImpl<true>,
                          LiteralValueGetterWithoutStrFunction>;

using StrBeforeExpression =
    LiteralExpressionImpl<2, StrAfterOrBeforeImpl<false>,
                          LiteralValueGetterWithoutStrFunction>;

}  // namespace detail::string_expressions
using namespace detail::string_expressions;
using Expr = SparqlExpression::Ptr;

Expr makeSubstrExpression(Expr string, Expr start, Expr length) {
  return make<SubstrExpression>(string, start, length);
}

Expr makeStrAfterExpression(Expr child1, Expr child2) {
  return make<StrAfterExpression>(child1, child2);
}
Expr makeStrBeforeExpression(Expr child1, Expr child2) {
  return make<StrBeforeExpression>(child1, child2);
}

}  // namespace sparqlExpression
