//   Copyright 2025, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include "engine/sparqlExpressions/BlankNodeExpression.h"

#include <absl/strings/str_cat.h>

#include "engine/sparqlExpressions/LiteralExpression.h"
#include "engine/sparqlExpressions/NaryExpressionImpl.h"
#include "engine/sparqlExpressions/SparqlExpressionValueGetters.h"
#include "util/ChunkedForLoop.h"

namespace sparqlExpression {

namespace detail {
// If the char is allowed inside the blank node label. A little bit stricter
// than the specification for simplicity.
bool isAllowedChar(char c) {
  return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || c == '_' ||
         (c >= '0' && c <= '9');
}

// Escape a string for use as a blank node label.
std::string escapeStringForBlankNode(std::string_view input) {
  std::string output;
  for (char c : input) {
    if (isAllowedChar(c)) {
      output += c;
    } else {
      // Since we don't allow the '.' char in our `isAllowedChar` check, we can
      // safely use it to denote escape sequences.
      output += ".";
      output += std::to_string(c);
    }
  }
  return output;
}

// SparqlExpression representing the term "BNODE()".
class BlankNodeExpression : public SparqlExpression {
  std::variant<size_t, Ptr> label_;
  // Counter that is incremented for each blank node created to ensure its
  // uniqueness.
  mutable std::atomic_uint64_t counter_ = 0;
  // Counter that is incremented for each cache key to ensure its uniqueness.
  // This needs to be separate from `counter_` to ensure that two
  // `BlankNodeExpression`s with the same label return the same blank node
  // during evaluation regardless of how many times `getCacheKey` is called.
  mutable std::atomic_uint64_t cacheBreaker_ = 0;

 public:
  // ___________________________________________________________________________
  explicit BlankNodeExpression(std::variant<size_t, Ptr> label)
      : label_{std::move(label)} {}
  BlankNodeExpression(const BlankNodeExpression&) = delete;
  BlankNodeExpression& operator=(const BlankNodeExpression&) = delete;

  // __________________________________________________________________________
  ExpressionResult evaluateImpl(EvaluationContext* context,
                                auto getNextLabel) const {
    std::string_view blankNodePrefix =
        std::holds_alternative<size_t>(label_) ? "bn" : "un";

    VectorWithMemoryLimit<IdOrLiteralOrIri> result{context->_allocator};
    const size_t numElements = context->size();
    result.reserve(numElements);

    ad_utility::chunkedForLoop<1000>(
        0, numElements,
        [this, &result, &blankNodePrefix, &getNextLabel](size_t) {
          const auto& label = getNextLabel();
          // TODO<RobinTF> Encoding blank nodes as IRIs is very
          // memory-inefficient given that we only need to ensure distinctness.
          // But for now this is the easiest way to implement it without
          // changing large parts of the code.
          if (label.has_value()) {
            auto uniqueIri = absl::StrCat(QLEVER_INTERNAL_BLANK_NODE_IRI_PREFIX,
                                          "_:", blankNodePrefix, label.value(),
                                          "_", counter_.load(), ">");
            result.push_back(LiteralOrIri{
                ad_utility::triple_component::Iri::fromStringRepresentation(
                    std::move(uniqueIri))});
          } else {
            result.push_back(Id::makeUndefined());
          }
          // TODO<RobinTF> Using a counter during value generation assumes that
          // the row index won't change after the value is generated. This is
          // not guaranteed and could lead to inconsistencies, but fixing this
          // behaviour would require a larger refactoring.
          ++counter_;
        },
        [context]() { context->cancellationHandle_->throwIfCancelled(); });
    return result;
  }

  // __________________________________________________________________________
  ExpressionResult evaluate(EvaluationContext* context) const override {
    if (std::holds_alternative<size_t>(label_)) {
      return evaluateImpl(context, [this]() -> std::optional<size_t> {
        return std::get<size_t>(label_);
      });
    }
    auto childResult = std::get<Ptr>(label_)->evaluate(context);
    return std::visit(
        [this, context]<typename T>(T&& element) -> ExpressionResult {
          if constexpr (isConstantResult<T>) {
            const auto& value = StringValueGetter{}(AD_FWD(element), context);
            if (!value.has_value()) {
              // Increment the counter for every element in the context for
              // consistency.
              counter_ += context->size();
              return Id::makeUndefined();
            }
            auto escapedValue = escapeStringForBlankNode(value.value());
            return evaluateImpl(
                context, [&escapedValue]() -> std::optional<std::string_view> {
                  return escapedValue;
                });
          } else {
            auto generator = detail::makeGenerator(AD_FWD(element),
                                                   context->size(), context);
            return evaluateImpl(
                context,
                [it = generator.begin(), &generator,
                 context]() mutable -> std::optional<std::string> {
                  AD_CORRECTNESS_CHECK(it != generator.end());
                  const auto& value =
                      StringValueGetter{}(std::move(*it), context);
                  ++it;
                  if (!value.has_value()) {
                    return std::nullopt;
                  }
                  return escapeStringForBlankNode(value.value());
                });
          }
        },
        std::move(childResult));
  }

  // ___________________________________________________________________________
  std::string getCacheKey(const VariableToColumnMap& map) const override {
    if (std::holds_alternative<size_t>(label_)) {
      return absl::StrCat("#BlankNode#", std::get<size_t>(label_), "_",
                          cacheBreaker_++);
    }
    return absl::StrCat("#BlankNode#", std::get<Ptr>(label_)->getCacheKey(map),
                        "_", cacheBreaker_++);
  }

 private:
  std::span<Ptr> childrenImpl() override {
    if (std::holds_alternative<Ptr>(label_)) {
      return {&std::get<Ptr>(label_), 1};
    }
    return {};
  }
};
}  // namespace detail

SparqlExpression::Ptr makeBlankNodeExpression(SparqlExpression::Ptr child) {
  return std::make_unique<detail::BlankNodeExpression>(std::move(child));
}
SparqlExpression::Ptr makeUniqueBlankNodeExpression(size_t label) {
  return std::make_unique<detail::BlankNodeExpression>(label);
}
}  // namespace sparqlExpression
