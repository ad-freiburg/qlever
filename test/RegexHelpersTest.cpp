// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>
#include <re2/re2.h>

#include <random>
#include <string>
#include <string_view>
#include <vector>

#include "engine/sparqlExpressions/RegexHelpers.h"

using sparqlExpression::detail::getLiteralPrefixOfRegex;
using ::testing::AllOf;
using ::testing::Gt;
using ::testing::IsEmpty;
using ::testing::Le;
using ::testing::SizeIs;
using ::testing::StartsWith;

namespace {

// A regex without a leading anchor matches anywhere in the string, so nothing
// can be said about the beginning of the matched values.
TEST(RegexHelpers, unanchoredRegexesHaveNoPrefix) {
  EXPECT_THAT(getLiteralPrefixOfRegex("alpha"), IsEmpty());
  EXPECT_THAT(getLiteralPrefixOfRegex("abc$"), IsEmpty());
  EXPECT_THAT(getLiteralPrefixOfRegex(".abc"), IsEmpty());
  EXPECT_THAT(getLiteralPrefixOfRegex(""), IsEmpty());
  // `$` alone does not anchor at the start either.
  EXPECT_THAT(getLiteralPrefixOfRegex("a$"), IsEmpty());
}

// The `^` anchor (and its explicit spelling `\A`) makes the leading literal a
// guaranteed prefix.
TEST(RegexHelpers, anchoredRegexes) {
  EXPECT_EQ("alpha", getLiteralPrefixOfRegex("^alpha"));
  EXPECT_EQ("abc", getLiteralPrefixOfRegex("^abc$"));
  EXPECT_EQ("abc", getLiteralPrefixOfRegex(R"(^\Aabc)"));
  EXPECT_EQ("abc", getLiteralPrefixOfRegex("^^abc"));
  // The regex does not have to *start* with the `^` character, it only has to
  // be anchored.
  EXPECT_EQ("abc", getLiteralPrefixOfRegex("(?:^abc)"));
  // A prefix consisting of the anchor alone is empty and hence useless.
  EXPECT_THAT(getLiteralPrefixOfRegex("^"), IsEmpty());
}

// Special regex syntax after the literal prefix only shortens the prefix, it
// does not disable the optimization completely.
TEST(RegexHelpers, prefixIsShortenedBySpecialSyntax) {
  EXPECT_EQ("al", getLiteralPrefixOfRegex("^al.ha"));
  EXPECT_EQ("al", getLiteralPrefixOfRegex("^alh*"));
  EXPECT_EQ("Abc", getLiteralPrefixOfRegex("^Abc[def]"));
  EXPECT_EQ("a", getLiteralPrefixOfRegex("^a.*"));
  EXPECT_EQ("abc", getLiteralPrefixOfRegex(R"(^abc\d)"));
  EXPECT_EQ("Q", getLiteralPrefixOfRegex("^Q[0-9]+"));
  EXPECT_EQ("a", getLiteralPrefixOfRegex("^a[[:alpha:]]"));
  // If the very first element is not a literal, there is no prefix at all.
  EXPECT_THAT(getLiteralPrefixOfRegex("^[a-c]bc"), IsEmpty());
  EXPECT_THAT(getLiteralPrefixOfRegex("^[[:alpha:]]bc"), IsEmpty());
  EXPECT_THAT(getLiteralPrefixOfRegex(R"(^\d abc)"), IsEmpty());
}

// Quantifiers bind to the preceding element. An element that may occur zero
// times is not part of the guaranteed prefix, whereas a fixed or minimum
// repetition count still contributes to it.
TEST(RegexHelpers, quantifiers) {
  EXPECT_EQ("abc", getLiteralPrefixOfRegex("^abcd?"));
  EXPECT_EQ("abc", getLiteralPrefixOfRegex("^abcd*"));
  EXPECT_EQ("abcd", getLiteralPrefixOfRegex("^abcd+"));
  EXPECT_EQ("a", getLiteralPrefixOfRegex("^a+b"));
  EXPECT_EQ("abbc", getLiteralPrefixOfRegex("^ab{2}c"));
  EXPECT_EQ("abb", getLiteralPrefixOfRegex("^ab{2,3}c"));
  EXPECT_EQ("ab", getLiteralPrefixOfRegex("^ab{1,3}c"));
  // A leading element that may be absent leaves no guaranteed prefix.
  EXPECT_THAT(getLiteralPrefixOfRegex("^a?b"), IsEmpty());
  EXPECT_THAT(getLiteralPrefixOfRegex("^a*b"), IsEmpty());
  // `a{0}` matches the empty string, so this is effectively `^b`.
  EXPECT_EQ("b", getLiteralPrefixOfRegex("^a{0}b"));
}

// Escaped characters and `\Q...\E` denote literal text and are part of the
// prefix, with the escaping undone.
TEST(RegexHelpers, escaping) {
  EXPECT_EQ(R"(\al*ph.a()", getLiteralPrefixOfRegex(R"(^\\al\*ph\.a\()"));
  EXPECT_EQ("abc.def", getLiteralPrefixOfRegex(R"(^abc\.def)"));
  EXPECT_EQ("\"", getLiteralPrefixOfRegex(R"(^\")"));
  EXPECT_EQ("\"foo", getLiteralPrefixOfRegex(R"(^"foo)"));
  EXPECT_EQ("a.b", getLiteralPrefixOfRegex(R"(^\Qa.b\E)"));
  EXPECT_EQ("ab\n", getLiteralPrefixOfRegex(R"(^ab\n)"));
  EXPECT_EQ("ABC", getLiteralPrefixOfRegex(R"(^\x41BC)"));
  // Multi-byte UTF-8 characters are returned as their UTF-8 encoding.
  EXPECT_EQ("über", getLiteralPrefixOfRegex("^über"));
}

// Groups are "seen through", so a literal inside a group still contributes to
// the prefix.
TEST(RegexHelpers, groups) {
  EXPECT_EQ("alh", getLiteralPrefixOfRegex("^a(lh)"));
  EXPECT_EQ("abc", getLiteralPrefixOfRegex("^(?:ab)c"));
  EXPECT_EQ("abcd", getLiteralPrefixOfRegex("^ab(c)d"));
  EXPECT_EQ("abcdef", getLiteralPrefixOfRegex("^(abc)def"));
}

// An alternation only shortens the prefix to the common part of its branches.
// If, however, the alternation is at the top level, the anchor applies to the
// first branch only and there is no prefix at all.
TEST(RegexHelpers, alternations) {
  EXPECT_EQ("abc", getLiteralPrefixOfRegex("^abc(d|e)"));
  EXPECT_EQ("a", getLiteralPrefixOfRegex("^(?:ab|ac)"));
  EXPECT_EQ("a", getLiteralPrefixOfRegex("^(ab|ac)d"));
  // Every branch is anchored here, so a common prefix can still be derived.
  EXPECT_EQ("a", getLiteralPrefixOfRegex("^ab|^ac"));
  // `^abc|def` also matches "Xdef", so there is no prefix.
  EXPECT_THAT(getLiteralPrefixOfRegex("^abc|def"), IsEmpty());
  EXPECT_THAT(getLiteralPrefixOfRegex("^abc.*|def"), IsEmpty());
  EXPECT_THAT(getLiteralPrefixOfRegex("^a$|b"), IsEmpty());
  // Inside `\Q...\E` a `)` may appear without a matching `(`; this must not
  // confuse the analysis of the (top-level) alternation.
  EXPECT_THAT(getLiteralPrefixOfRegex(R"(^a\Qb)c\E|d)"), IsEmpty());
}

// Inline flags are taken into account.
TEST(RegexHelpers, inlineFlags) {
  // A case-insensitive literal is not a fixed prefix.
  EXPECT_THAT(getLiteralPrefixOfRegex("^(?i)abc"), IsEmpty());
  // ... but a flag that only affects a later part of the regex is harmless.
  EXPECT_EQ("abc", getLiteralPrefixOfRegex("^abc(?i)d"));
  EXPECT_EQ("a", getLiteralPrefixOfRegex("^(?s)a.b"));
  // With the `m` flag, `^` also matches after a newline, so `(?m)^abc` also
  // matches "X\nabc". Note that in `^(?m)abc` the anchor is parsed *before* the
  // flag takes effect, so it still anchors at the beginning of the text.
  EXPECT_THAT(getLiteralPrefixOfRegex("(?m)^abc"), IsEmpty());
  EXPECT_EQ("abc", getLiteralPrefixOfRegex("^(?m)abc"));
}

// The zero-width word-boundary assertions `\b` and `\B` are excluded, because
// `RE2::PossibleMatchRange` reports wrong bounds for them (see the comment on
// `containsWordBoundary` in `RegexHelpers.cpp`).
TEST(RegexHelpers, wordBoundariesAreExcluded) {
  EXPECT_THAT(getLiteralPrefixOfRegex(R"(^a\bc)"), IsEmpty());
  EXPECT_THAT(getLiteralPrefixOfRegex(R"(^a\Bc)"), IsEmpty());
  // Without the exclusion, RE2 would report the prefix "a" here, although the
  // regex also matches "b" via the empty match of `\b` at the beginning.
  EXPECT_THAT(getLiteralPrefixOfRegex(R"(^(?:a|\b))"), IsEmpty());
  EXPECT_TRUE(RE2::PartialMatch("b", R"(^(?:a|\b))"));
}

// Invalid regexes are rejected instead of being analysed. In production such a
// regex never reaches this function (`makeRegexExpression` rejects it first),
// but the function must not misbehave for them either.
TEST(RegexHelpers, invalidRegexesHaveNoPrefix) {
  // A trailing backslash.
  EXPECT_THAT(getLiteralPrefixOfRegex(R"(^ab\)"), IsEmpty());
  EXPECT_THAT(getLiteralPrefixOfRegex("^ab("), IsEmpty());
  EXPECT_THAT(getLiteralPrefixOfRegex("^ab[c"), IsEmpty());
  EXPECT_THAT(getLiteralPrefixOfRegex("^ab**"), IsEmpty());
  // `\K` is a Perl feature that RE2 does not support.
  EXPECT_THAT(getLiteralPrefixOfRegex(R"(^ab\Kc)"), IsEmpty());
}

// Very long prefixes are truncated (see `maxPrefixLength`), which is sound
// because a shorter prefix is always a valid answer.
TEST(RegexHelpers, longPrefixesAreTruncated) {
  std::string longLiteral(500, 'a');
  std::string prefix = getLiteralPrefixOfRegex("^" + longLiteral);
  EXPECT_THAT(prefix, SizeIs(AllOf(Gt(0u), Le(500u))));
  EXPECT_EQ(prefix, longLiteral.substr(0, prefix.size()));
}

// _____________________________________________________________________________
// The randomized soundness test below is the most important test in this file:
// a prefix that is too long silently produces wrong query results, so we check
// the actual guarantee ("every matched string starts with the prefix") against
// RE2 itself for a large number of generated regexes.

// A small random regex generator over the alphabet {a, b, c}. The building
// blocks are chosen such that all the constructs that the prefix analysis has
// to reason about (escapes, character classes, quantifiers, groups,
// alternations, anchors, inline flags, word boundaries) appear frequently.
class RegexGenerator {
 private:
  std::mt19937 randomEngine_;
  int remainingBudget_ = 0;

  int pick(int numAlternatives) {
    return std::uniform_int_distribution<int>{
        0, numAlternatives - 1}(randomEngine_);
  }

  std::string atom() {
    static const std::vector<std::string> atoms = {
        "a",     "b",     "c",       "ab",    ".",          "[ab]",
        "[^a]",  "[a-c]", R"(\d)",   R"(\w)", R"(\W)",      R"(\S)",
        R"(\.)", R"(\\)", R"(\x61)", R"(\n)", "\n",         R"(\p{L})",
        "$",     R"(\z)", "^",       R"(\A)", R"(\b)",      R"(\B)",
        "(?i)",  "(?s)",  "(?m)",    "(?U)",  R"(\Qa.b\E)", "[[:word:]]"};
    return atoms[pick(static_cast<int>(atoms.size()))];
  }

 public:
  explicit RegexGenerator(unsigned seed) : randomEngine_{seed} {}

  // Generate a random regex with the given maximum nesting `depth`.
  std::string generate(int depth) {
    if (depth <= 0 || remainingBudget_-- <= 0) {
      return atom();
    }
    switch (pick(7)) {
      case 0:
        return generate(depth - 1) + generate(depth - 1);
      case 1:
        return generate(depth - 1) + "|" + generate(depth - 1);
      case 2:
        return "(" + generate(depth - 1) + ")";
      case 3:
        return "(?:" + generate(depth - 1) + ")";
      case 4: {
        static const std::vector<std::string> quantifiers = {
            "?", "*", "+", "{2}", "{1,3}", "{0,2}", "{2,}"};
        return "(?:" + generate(depth - 1) + ")" +
               quantifiers[pick(static_cast<int>(quantifiers.size()))];
      }
      case 5:
        return atom() + generate(depth - 1);
      default:
        return atom();
    }
  }

  // Generate a random regex, half of which are explicitly anchored so that the
  // interesting code path is exercised often.
  std::string generateRegex(bool anchored) {
    remainingBudget_ = 12;
    return (anchored ? "^" : "") + generate(3);
  }
};

// All strings over `alphabet` with a length of at most `maxLength`.
void addAllStrings(std::string_view alphabet, size_t maxLength,
                   std::vector<std::string>& result,
                   const std::string& current = "") {
  result.push_back(current);
  if (current.size() == maxLength) {
    return;
  }
  for (char c : alphabet) {
    addAllStrings(alphabet, maxLength, result, current + c);
  }
}

// For a large number of randomly generated regexes, assert the property that
// `getLiteralPrefixOfRegex` promises: every string that the regex matches (with
// the partial-match semantics that QLever's `REGEX` uses) starts with the
// returned prefix.
TEST(RegexHelpers, randomizedSoundness) {
  std::vector<std::string> testStrings;
  addAllStrings("abc\n", 3, testStrings);
  for (const char* string :
       {"", "aaaaaaaaaa", "abcabc", "ABC", "AbC", "a.b", R"(a\b)", " a", "a ",
        "a\tb", "über", "\xff", "a\xff b"}) {
    testStrings.push_back(string);
  }

  RegexGenerator generator{481516234u};
  size_t numRegexesWithPrefix = 0;
  // Note: This number is a compromise between coverage and test runtime. The
  // property was additionally verified for a much larger number of regexes
  // offline; if this test ever fails, increase the number to reproduce.
  for (size_t i = 0; i < 1000; ++i) {
    std::string regexString = generator.generateRegex(i % 2 == 0);
    RE2 regex{regexString, RE2::Quiet};
    // Invalid regexes never reach the prefix analysis in production.
    if (!regex.ok()) {
      continue;
    }
    std::string prefix = getLiteralPrefixOfRegex(regexString);
    if (prefix.empty()) {
      continue;
    }
    ++numRegexesWithPrefix;
    for (const std::string& string : testStrings) {
      if (!RE2::PartialMatch(string, regex)) {
        continue;
      }
      EXPECT_THAT(string, StartsWith(prefix))
          << "The regex \"" << regexString << "\" matches the string \""
          << string << "\", which does not start with the derived prefix \""
          << prefix << "\"";
    }
  }
  // Guard against the test silently becoming vacuous, e.g. because the analysis
  // or the generator changed in a way that no prefixes are derived at all.
  EXPECT_GT(numRegexesWithPrefix, 100u);
}

}  // namespace
