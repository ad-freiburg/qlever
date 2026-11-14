// Copyright 2026 The QLever Authors, in particular:
//
// 2023 - 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.
//
// Copyright 2025, Bayerische Motoren Werke Aktiengesellschaft (BMW AG)

// The `REPLACE` expression and the helper expression that merges the flags of
// a regex into the regex pattern. They are split from `StringExpressions.cpp`
// to reduce the memory needed for compiling and statically analyzing a single
// translation unit.

#include <charconv>
#include <ctre-unicode.hpp>

#include "engine/sparqlExpressions/NaryExpression.h"
#include "engine/sparqlExpressions/StringExpressionsHelper.h"
#include "util/Exception.h"

namespace sparqlExpression {
namespace detail::string_expressions {

struct MergeFlagsIntoRegex {
  IdOrLiteralOrIri operator()(std::optional<std::string> regex,
                              const std::optional<std::string>& flags) const {
    if (!flags.has_value() || !regex.has_value()) {
      return Id::makeUndefined();
    }
    auto firstInvalidFlag = flags.value().find_first_not_of("imsU");
    if (firstInvalidFlag != std::string::npos) {
      return Id::makeUndefined();
    }

    // In Google RE2 the flags are directly part of the regex.
    std::string result =
        flags.value().empty()
            ? std::move(regex.value())
            : absl::StrCat("(?", flags.value(), ":", regex.value() + ")");
    return toLiteral(std::move(result));
  }
};

using MergeRegexPatternAndFlagsExpression =
    StringExpressionImpl<2, MergeFlagsIntoRegex, LiteralFromIdGetter>;

// Each token of a re2 rewrite string is either literal text, an escaped `\`, or
// a reference to a group. Regex pattern at namespace scope for C++17
// compatibility.
constexpr ctll::fixed_string rewriteTokenRegex = "[^\\\\]+|\\\\\\\\|\\\\[0-9]";

// Remove all references to groups that do not exist in `pattern` from the re2
// rewrite string `rewrite`. In XPath, these are replaced by the empty string,
// whereas re2 refuses to rewrite at all.
std::string removeReferencesToMissingGroups(std::string_view rewrite,
                                            const RE2& pattern) {
  std::string result;
  result.reserve(rewrite.size());
  size_t numConsumed = 0;
  for (auto match : ctre::tokenize<rewriteTokenRegex>(rewrite)) {
    std::string_view token = match.to_view();
    numConsumed += token.size();
    int group = 0;
    bool isReference =
        token.front() == '\\' &&
        std::from_chars(token.data() + 1, token.data() + token.size(), group)
                .ec == std::errc{};
    if (!isReference || group <= pattern.NumberOfCapturingGroups()) {
      result.append(token);
    }
  }
  // `ReplacementStringGetter` only produces valid rewrite strings.
  AD_CORRECTNESS_CHECK(numConsumed == rewrite.size());
  return result;
}

struct ReplaceImpl {
  IdOrLiteralOrIri operator()(
      std::optional<ad_utility::triple_component::Literal> s,
      const std::shared_ptr<RE2>& pattern,
      const std::optional<std::string>& replacement) const {
    if (!s.has_value() || !pattern || !replacement.has_value()) {
      return Id::makeUndefined();
    }
    std::string in(s.value().getContent());
    const auto& pat = *pattern;
    // Check for invalid regexes.
    if (!pat.ok()) {
      return Id::makeUndefined();
    }
    RE2::GlobalReplace(
        &in, pat, removeReferencesToMissingGroups(replacement.value(), pat));
    s.value().replaceContent(in);
    return LiteralOrIri(std::move(s.value()));
  }
};

using ReplaceExpression =
    LiteralExpressionImpl<3, ReplaceImpl, RegexValueGetter,
                          ReplacementStringGetter>;

}  // namespace detail::string_expressions
using namespace detail::string_expressions;
using Expr = SparqlExpression::Ptr;

Expr makeMergeRegexPatternAndFlagsExpression(Expr pattern, Expr flags) {
  return make<MergeRegexPatternAndFlagsExpression>(pattern, flags);
}

Expr makeReplaceExpression(Expr input, Expr pattern, Expr repl, Expr flags) {
  if (flags) {
    pattern = makeMergeRegexPatternAndFlagsExpression(std::move(pattern),
                                                      std::move(flags));
  }
  return make<ReplaceExpression>(input, pattern, repl);
}

}  // namespace sparqlExpression
