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

#include <absl/strings/str_cat.h>

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

// Each token of the output of `ReplacementStringGetter` is either literal
// text, an escaped `\`, or a reference to a group (with all the digits that
// follow the `\`). Regex pattern at namespace scope for C++17 compatibility.
constexpr ctll::fixed_string rewriteTokenRegex = "[^\\\\]+|\\\\\\\\|\\\\[0-9]+";

// Turn the output of `ReplacementStringGetter` into a valid RE2 rewrite string
// for `pattern`, following the XPath rules for the group references. Return
// `std::nullopt` for a reference to group 10 or higher, which RE2 does not
// support.
//
// NOTE 1: In XPath, a reference `$N` takes as many of the following digits as
// possible, as long as the number still names an existing group or is at most
// 9. The remaining digits are literal text. For example, if `pattern` has 10
// groups, `$10` is group 10, but `$11` is group 1 followed by a literal `1`.
//
// NOTE 2: A reference to a group that does not exist is replaced by the empty
// string (RE2 would refuse to rewrite at all).
std::optional<std::string> adaptRewriteToPattern(std::string_view rewrite,
                                                 const RE2& pattern) {
  const int numGroups = pattern.NumberOfCapturingGroups();
  const int maxGroup = std::max(numGroups, 9);
  std::string result;
  result.reserve(rewrite.size());
  size_t numConsumed = 0;
  for (auto match : ctre::tokenize<rewriteTokenRegex>(rewrite)) {
    std::string_view token = match.to_view();
    numConsumed += token.size();
    if (token == "\\\\" || token.front() != '\\') {
      result.append(token);
      continue;
    }
    // Take digits into `group` as long as the number is at most `maxGroup`
    // (see NOTE 1 above). The first digit is always taken.
    std::string_view digits = token.substr(1);
    int group = 0;
    size_t numDigits = 0;
    for (char digit : digits) {
      int next = group * 10 + (digit - '0');
      if (next > maxGroup) {
        break;
      }
      group = next;
      ++numDigits;
    }
    if (group <= numGroups) {
      if (group > 9) {
        return std::nullopt;
      }
      absl::StrAppend(&result, "\\", group);
    }
    result.append(digits.substr(numDigits));
  }
  // `ReplacementStringGetter` only produces strings that can be tokenized.
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
    auto rewrite = adaptRewriteToPattern(replacement.value(), pat);
    if (!rewrite.has_value()) {
      return Id::makeUndefined();
    }
    RE2::GlobalReplace(&in, pat, rewrite.value());
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
