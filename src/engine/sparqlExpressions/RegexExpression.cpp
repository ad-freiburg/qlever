// Copyright 2022 - 2024
// University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>
//          Hannah Bast <bast@cs.uni-freiburg.de>

#include "engine/sparqlExpressions/RegexExpression.h"

#include <re2/re2.h>

#include <utility>

#include "backports/StartsWithAndEndsWith.h"
#include "engine/sparqlExpressions/LiteralExpression.h"
#include "engine/sparqlExpressions/NaryExpression.h"
#include "engine/sparqlExpressions/RegexExpressionHelpers.h"
#include "engine/sparqlExpressions/SparqlExpressionValueGetters.h"
#include "engine/sparqlExpressions/StringExpressionsHelper.h"

using namespace std::literals;

namespace sparqlExpression::detail {

// _____________________________________________________________________________
void ensureIsValidRegexIfConstant(const SparqlExpression& expression) {
  const auto* stringLiteralExpression =
      dynamic_cast<const StringLiteralExpression*>(&expression);
  if (stringLiteralExpression) {
    const auto& literal = stringLiteralExpression->value();
    const auto& string = asStringViewUnsafe(literal.getContent());
    RE2 regex{string, RE2::Quiet};
    if (!regex.ok()) {
      throw std::runtime_error{absl::StrCat(
          "The regex \"", string,
          "\" is not supported by QLever (which uses Google's RE2 library); "
          "the error from RE2 is: ",
          regex.error())};
    }
  }
}

// _____________________________________________________________________________
void ensureIsValidFlagIfConstant(const SparqlExpression& expression) {
  const auto* stringLiteralExpression =
      dynamic_cast<const StringLiteralExpression*>(&expression);
  if (stringLiteralExpression) {
    const auto& literal = stringLiteralExpression->value();
    const auto& string = asStringViewUnsafe(literal.getContent());
    auto firstInvalidFlag = string.find_first_not_of("imsU");
    if (firstInvalidFlag != std::string::npos) {
      throw std::runtime_error{absl::StrCat(
          "Invalid regex flag '", string.substr(firstInvalidFlag, 1),
          "' found in \"", string,
          "\". The only supported flags are 'i', 'm', 's', 'U', and any "
          "combination of them")};
    }
  }
}

// _____________________________________________________________________________
struct RegexImpl {
  Id operator()(const std::optional<std::string>& input,
                const std::shared_ptr<RE2>& pattern) const {
    if (!input.has_value() || !pattern) {
      return Id::makeUndefined();
    }
    // Check for invalid regexes.
    if (!pattern->ok()) {
      return Id::makeUndefined();
    }
    return Id::makeFromBool(RE2::PartialMatch(input.value(), *pattern));
  }
};

// The standard `REGEX` expression. It always evaluates the actual regex (via
// Google's RE2 library) by delegating to the string-expression machinery.
class RegexExpressionBase
    : public string_expressions::StringExpressionImpl<2, RegexImpl,
                                                      RegexValueGetter> {
  using Base =
      string_expressions::StringExpressionImpl<2, RegexImpl, RegexValueGetter>;
  using Base::Base;
};

// A `RegexExpressionBase` that additionally supports prefiltering when the
// regex is a prefix regex (e.g. `^prefix`) applied to a plain variable. The
// prefilter only restricts the blocks that are scanned; the actual regex is
// still evaluated on the remaining rows by the base class.
class RegexExpression : public RegexExpressionBase {
 private:
  // The variable and the guaranteed literal prefix (see `getPrefixRegex`) if
  // the regex is a prefix regex on a plain variable, `std::nullopt` otherwise.
  std::optional<std::pair<Variable, std::string>> prefix_;

 public:
  RegexExpression(Ptr child, Ptr regex,
                  std::optional<std::pair<Variable, std::string>> prefix)
      : RegexExpressionBase(std::move(child), std::move(regex)),
        prefix_{std::move(prefix)} {}

  std::vector<PrefilterExprVariablePair> getPrefilterExpressionForMetadata(
      [[maybe_unused]] const LocalVocabContext& context,
      [[maybe_unused]] bool isNegated) const override {
    if (!prefix_.has_value()) {
      return {};
    }
    std::vector<PrefilterExprVariablePair> prefilterVec;
    prefilterVec.emplace_back(
        std::make_unique<prefilterExpressions::PrefixRegexExpression>(
            TripleComponent::Literal::literalWithNormalizedContent(
                asNormalizedStringViewUnsafe(prefix_->second))),
        prefix_->first);
    return prefilterVec;
  }
};

// If `string` is a plain variable and `regex` is a constant simple-literal
// prefix regex (see `getPrefixRegex`), return the variable and the extracted
// prefix, which enables prefiltering. Return `std::nullopt` otherwise. Throws
// if the regex is a constant literal with a datatype or language tag (which the
// `REGEX` function forbids).
//
// Note: Prefiltering `STR(?var)` is deliberately not supported, since we would
// not only have to match "Bob", but also "Bob"@en, "Bob"^^<iri>, and so on. The
// current prefilter expressions do not consider this matching logic.
std::optional<std::pair<Variable, std::string>> getRegexPrefilterInfo(
    const SparqlExpression::Ptr& string, const SparqlExpression& regex) {
  bool childIsStrExpression = string->isStrExpression();
  const auto* variableExpression = dynamic_cast<const VariableExpression*>(
      childIsStrExpression ? string->children()[0].get() : string.get());
  const auto* stringLiteralExpression =
      dynamic_cast<const StringLiteralExpression*>(&regex);
  if (!variableExpression || !stringLiteralExpression) {
    return std::nullopt;
  }
  const auto& stringLiteral = stringLiteralExpression->value();
  ensureIsSimpleLiteral(stringLiteral);
  if (childIsStrExpression) {
    return std::nullopt;
  }
  std::optional<std::string> prefixRegex = getPrefixRegex(
      std::string{asStringViewUnsafe(stringLiteral.getContent())});
  if (!prefixRegex.has_value()) {
    return std::nullopt;
  }
  return std::pair{variableExpression->value(), std::move(prefixRegex.value())};
}

}  // namespace sparqlExpression::detail

namespace sparqlExpression {

namespace {
// The regex characters that have a special meaning. An unescaped occurrence
// terminates a literal prefix, and an escaped occurrence (e.g. `\.`) denotes
// the corresponding literal character (the backslash is included so that `\\`
// denotes a literal backslash).
constexpr std::string_view regexSpecialChars = R"(\^$.|?*+()[]{})";

// Return true iff `regex` (which must be a valid regex) contains an alternation
// (`|`) at the top level, i.e. outside of any group `(...)` or character class
// `[...]`. Such an alternation invalidates any prefix guarantee, e.g. `^ab|cd`
// also matches "cd...".
bool hasTopLevelAlternation(std::string_view regex) {
  size_t parenDepth = 0;
  bool inCharClass = false;
  for (size_t i = 0; i < regex.size(); ++i) {
    char c = regex[i];
    if (c == '\\') {
      ++i;  // Skip the escaped character.
      continue;
    }
    if (inCharClass) {
      if (c == ']') {
        inCharClass = false;
      }
      continue;
    }
    switch (c) {
      case '[':
        inCharClass = true;
        break;
      case '(':
        ++parenDepth;
        break;
      case ')':
        if (parenDepth > 0) {
          --parenDepth;
        }
        break;
      case '|':
        if (parenDepth == 0) {
          return true;
        }
        break;
      default:
        break;
    }
  }
  return false;
}
}  // namespace

// _____________________________________________________________________________
std::optional<std::string> getPrefixRegex(std::string regex) {
  if (!ql::starts_with(regex, '^')) {
    return std::nullopt;
  }
  // A top-level alternation means that not every match starts with the leading
  // literal (e.g. `^abc.*|def` also matches "Xdef"), so we cannot derive a
  // prefix. This has to be a full scan of the whole regex up front: the
  // accumulation loop below stops at the first special character, so it would
  // not notice a `|` that appears later (as in `^abc.*|def`).
  if (hasTopLevelAlternation(std::string_view{regex}.substr(1))) {
    return std::nullopt;
  }

  // Accumulate the guaranteed literal prefix, starting after the leading `^`.
  std::string prefix;
  for (size_t i = 1; i < regex.size();) {
    char c = regex[i];
    // The literal character that is a candidate for the prefix, and the index
    // of the character following it (used to peek for a following quantifier).
    char literal = '\0';
    size_t next = 0;
    if (c == '\\') {
      // A trailing backslash cannot occur in a valid regex.
      if (i + 1 >= regex.size()) {
        break;
      }
      char escaped = regex[i + 1];
      // Only escapes of special characters (e.g. `\.` or `\\`) denote a literal
      // character. Other escapes (e.g. `\d`, `\b`) are character classes or
      // assertions and terminate the prefix.
      if (regexSpecialChars.find(escaped) == std::string_view::npos) {
        break;
      }
      literal = escaped;
      next = i + 2;
    } else if (regexSpecialChars.find(c) != std::string_view::npos) {
      // An unescaped special character terminates the literal prefix.
      break;
    } else {
      literal = c;
      next = i + 1;
    }

    // Peek at the following character to correctly handle quantifiers, which
    // bind to the preceding (literal) character.
    char quantifier = next < regex.size() ? regex[next] : '\0';
    if (quantifier == '?' || quantifier == '*' || quantifier == '{') {
      // The preceding character is optional or repeated a variable number of
      // times (possibly zero), so it is not part of the guaranteed prefix.
      break;
    }
    prefix.push_back(literal);
    if (quantifier == '+') {
      // The character is guaranteed at least once, but the repetition means we
      // cannot extend the prefix any further.
      break;
    }
    i = next;
  }

  if (prefix.empty()) {
    return std::nullopt;
  }
  return prefix;
}

// _____________________________________________________________________________
SparqlExpression::Ptr makeRegexExpression(SparqlExpression::Ptr string,
                                          SparqlExpression::Ptr regex,
                                          SparqlExpression::Ptr flags) {
  detail::ensureIsValidRegexIfConstant(*regex);
  if (flags) {
    if (auto* stringLiteralExpression =
            dynamic_cast<const StringLiteralExpression*>(flags.get())) {
      detail::ensureIsSimpleLiteral(stringLiteralExpression->value());
    }
    detail::ensureIsValidFlagIfConstant(*flags);
    // Merge the flags into the regex. The result is no longer a plain string
    // literal, so a regex with flags will never allow prefiltering (see
    // `getRegexPrefilterInfo`).
    regex = makeMergeRegexPatternAndFlagsExpression(std::move(regex),
                                                    std::move(flags));
  }
  // Compute the prefilter information (if the regex is a prefix regex) before
  // moving the arguments into the expression. The actual regex is always
  // evaluated by `RegexExpression`; the prefilter only restricts the scanned
  // blocks.
  auto prefilterInfo = detail::getRegexPrefilterInfo(string, *regex);
  return std::make_unique<detail::RegexExpression>(
      std::move(string), std::move(regex), std::move(prefilterInfo));
}

}  // namespace sparqlExpression
