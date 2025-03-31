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
  output.reserve(input.size());
  for (char c : input) {
    if (isAllowedChar(c)) {
      output += c;
    } else {
      // Since we don't allow the '.' char in our `isAllowedChar` check, we can
      // safely use it to denote escape sequences.
      absl::StrAppend(&output, ".", static_cast<int>(c));
    }
  }
  return output;
}

// SparqlExpression representing the term "BNODE()" and "BNODE(?x)". Currently
// this is implemented by creating special IRIs with a specific prefix and
// encoding the blank node within. This prefix is then stripped when converting
// back to a string.
// TODO<RobinTF> Using a counter during value generation assumes that
// the row index won't change after the value is generated. This is
// not guaranteed and could lead to inconsistencies, but fixing this
// behaviour would require a larger refactoring.
class BlankNodeExpression : public SparqlExpression {
  std::optional<Ptr> label_;
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
  explicit BlankNodeExpression(Ptr label) : label_{std::move(label)} {}

  // Constructor for the case of no arguments.
  BlankNodeExpression() = default;
  BlankNodeExpression(const BlankNodeExpression&) = delete;
  BlankNodeExpression& operator=(const BlankNodeExpression&) = delete;

  // Evaluate function for the case where no argument is given and each row gets
  // a new unique blank node index.
  ExpressionResult evaluateWithoutArguments(EvaluationContext* context) const {
    VectorWithMemoryLimit<Id> result{context->_allocator};
    const size_t numElements = context->size();
    result.reserve(numElements);

    ad_utility::chunkedForLoop<1000>(
        0, numElements,
        [&result, context](size_t) {
          result.push_back(
              Id::makeFromBlankNodeIndex(context->_localVocab.getBlankNodeIndex(
                  context->_qec.getIndex().getBlankNodeManager())));
        },
        [context]() { context->cancellationHandle_->throwIfCancelled(); });
    return result;
  }

  // Perform the actual evaluation of the expression. This creates a blank node
  // based on the result of `getNextLabel`.
  CPP_template(typename Printable, typename Func)(
      requires ad_utility::InvocableWithConvertibleReturnType<
          Func, std::optional<Printable>>
          CPP_and std::is_constructible_v<absl::AlphaNum, Printable>)
      ExpressionResult
      evaluateImpl(EvaluationContext* context, Func getNextLabel) const {
    std::string_view blankNodePrefix = "un";

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
                                          "_", counter_++, ">");
            result.push_back(LiteralOrIri{
                ad_utility::triple_component::Iri::fromStringRepresentation(
                    std::move(uniqueIri))});
          } else {
            result.push_back(Id::makeUndefined());
            ++counter_;
          }
        },
        [context]() { context->cancellationHandle_->throwIfCancelled(); });
    return result;
  }

  // __________________________________________________________________________
  ExpressionResult evaluate(EvaluationContext* context) const override {
    if (!label_.has_value()) {
      return evaluateWithoutArguments(context);
    }
    auto childResult = label_.value()->evaluate(context);
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
            return evaluateImpl<std::string_view>(
                context,
                [escapedValue = std::optional<std::string_view>{escapedValue}]()
                    -> const std::optional<std::string_view>& {
                  return escapedValue;
                });
          } else {
            auto generator = detail::makeGenerator(AD_FWD(element),
                                                   context->size(), context);
            return evaluateImpl<std::string>(
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
    if (!label_.has_value()) {
      return absl::StrCat("#BlankNode#", "_", cacheBreaker_++);
    }
    return absl::StrCat("#BlankNode#", label_.value()->getCacheKey(map), "_",
                        cacheBreaker_++);
  }

 private:
  std::span<Ptr> childrenImpl() override {
    if (label_.has_value()) {
      return {&label_.value(), 1};
    }
    return {};
  }
};
}  // namespace detail

SparqlExpression::Ptr makeBlankNodeExpression(SparqlExpression::Ptr child) {
  return std::make_unique<detail::BlankNodeExpression>(std::move(child));
}
SparqlExpression::Ptr makeUniqueBlankNodeExpression() {
  return std::make_unique<detail::BlankNodeExpression>();
}
}  // namespace sparqlExpression
