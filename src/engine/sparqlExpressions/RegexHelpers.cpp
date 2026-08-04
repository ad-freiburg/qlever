// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "engine/sparqlExpressions/RegexHelpers.h"

#include <absl/strings/str_cat.h>
#include <re2/prog.h>
#include <re2/re2.h>
#include <re2/regexp.h>

#include <cstddef>
#include <memory>
#include <stdexcept>

#include "backports/algorithm.h"
#include "backports/span.h"
#include "util/Exception.h"
#include "util/StringUtils.h"

namespace sparqlExpression::detail {

namespace {
// The maximum length (in bytes) of the derived prefix. It bounds the work of
// `PossibleMatchRange` below, which per byte of the prefix walks one DFA step
// that tries each of the 256 possible byte values. The concrete value is
// generous enough that even long IRI prefixes (e.g.
// `^http://www\.wikidata\.org/entity/Q`) fit into it, and beyond that a longer
// prefix hardly narrows the scanned blocks any further. Note that a smaller
// bound can only shorten the resulting prefix, it can never make it unsound.
constexpr int maxPrefixLength = 128;

// Return the position of the first character of `flags` that is not one of the
// `supportedRegexFlags`, or `std::string_view::npos` if all of them are
// supported. Shared by `mergeFlagsIntoRegex` and `ensureIsValidRegexFlags`,
// which only differ in how they report an unsupported flag.
size_t findFirstUnsupportedFlag(std::string_view flags) {
  return flags.find_first_not_of(supportedRegexFlags);
}

// Return true iff `regex` or any of its subexpressions is one of the zero-width
// word-boundary assertions `\b` or `\B`.
//
// Such regexes have to be excluded, because `PossibleMatchRange` walks the DFA
// byte by byte without keeping track of whether the previous byte was a word
// character, and therefore reports bounds that are simply wrong. For example
// for
// `^(?:a|\b)` it reports the range ["a", "a"], although that regex also matches
// "b" (via the empty match of `\b` at the very beginning).
bool containsWordBoundary(re2::Regexp* regex) {
  if (regex->op() == re2::kRegexpWordBoundary ||
      regex->op() == re2::kRegexpNoWordBoundary) {
    return true;
  }
  ql::span subexpressions{regex->sub(), static_cast<size_t>(regex->nsub())};
  return ql::ranges::any_of(subexpressions, containsWordBoundary);
}

// Return true iff the `regex` may be used to derive a prefix for the prefilter.
// Note that this is *not* the same as the regex starting with the character
// `^`: in `^ab|cd` the anchor only applies to the first alternative, so "Xcd"
// is matched as well. Conversely, regexes like `(?:^ab)c` are recognized as
// anchored although they do not start with a `^`.
bool isSuitableForPrefixExtraction(re2::Regexp* regex) {
  if (regex->op() != re2::kRegexpConcat) {
    return false;
  }
  // RE2's parser never creates a concatenation with fewer than two
  // subexpressions: shorter ones are collapsed into their single subexpression
  // or into `kRegexpEmptyMatch` (see `Regexp::ConcatOrAlternate`).
  AD_CORRECTNESS_CHECK(regex->nsub() > 0);
  return regex->sub()[0]->op() == re2::kRegexpBeginText &&
         !containsWordBoundary(regex);
}

}  // namespace

// _____________________________________________________________________________
std::string getLiteralPrefixOfRegex(std::string_view regex) {
  // Parse the regex once. Note that we deliberately do not go through `RE2`
  // here: that would parse the regex a second time, because the structural
  // check below needs the parsed representation, which `RE2` does not expose.
  //
  // Parse with exactly the flags that `RE2` would use, so that constructs like
  // `^` are guaranteed to be interpreted the same way here and during the
  // actual evaluation of the regex.
  re2::RegexpStatus status;
  auto parseFlags = static_cast<re2::Regexp::ParseFlags>(
      RE2::Options{RE2::Quiet}.ParseFlags());
  // A `re2::Regexp` is reference-counted and has a private destructor, so our
  // reference has to be released via `Decref()` instead of `delete`. Note that
  // `std::unique_ptr` doesn't run the deleter if the stored pointer is
  // `nullptr`, which is what `Parse` returns for an invalid regex.
  auto decref = [](re2::Regexp* parsedRegex) { parsedRegex->Decref(); };
  std::unique_ptr<re2::Regexp, decltype(decref)> parsed{
      re2::Regexp::Parse(regex, parseFlags, &status), decref};
  // This is also the case for an invalid regex.
  if (parsed == nullptr) {
    return "";
  }

  // Do the structural check before compiling: most regexes are rejected here
  // (e.g. because they are not anchored) and then never have to be compiled.
  if (!isSuitableForPrefixExtraction(parsed.get())) {
    return "";
  }

  // `PossibleMatchRange` needs a compiled program, because it works by walking
  // the resulting DFA. Two thirds of the memory budget is what `RE2` also
  // grants the forward program. Note that compiling can fail even for a regex
  // that parses successfully, for example if the program would be too large.
  std::unique_ptr<re2::Prog> program{
      parsed->CompileToProg(RE2::Options{RE2::Quiet}.max_mem() * 2 / 3)};
  if (program == nullptr) {
    return "";
  }

  // `PossibleMatchRange` computes bounds such that every matched string `s`
  // satisfies `lower <= s <= upper`. Every such `s` then has to start with the
  // longest common prefix `p` of the two bounds: if `s` deviated from `p` at
  // some position, then `s` would be smaller than `lower` (if it is smaller at
  // that position, or ends before it) or larger than `upper` (if it is larger
  // at that position), because both bounds start with `p`.
  std::string lower;
  std::string upper;

  // `PossibleMatchRange` fails if there is no upper bound that it could report,
  // which is the case if the regex matches arbitrary bytes (e.g. `^\C*`).
  if (!program->PossibleMatchRange(&lower, &upper, maxPrefixLength)) {
    return "";
  }
  // `RE2` works in UTF-8 mode, so the bounds consist of complete characters,
  // but they are compared (and truncated at `maxPrefixLength`) byte by byte, so
  // their common prefix may end in the middle of a character. For example for
  // `^Ä[ÄÖ]` the bounds are "ÄÄ" and "ÄÖ", which agree on the first byte of
  // their second character.
  //
  // Such a prefix must not be handed out: the vocabulary interprets the prefix
  // as text, and a dangling byte becomes U+FFFD there, which sorts *before* all
  // letters and hence yields a range that excludes the actual matches. Dropping
  // the incomplete character is always sound, as any prefix of a valid prefix
  // is itself a valid prefix.
  return std::string{ad_utility::removeIncompleteUtf8Character(
      ad_utility::commonPrefix(lower, upper))};
}

// _____________________________________________________________________________
std::optional<std::string> mergeFlagsIntoRegex(std::string regex,
                                               std::string_view flags) {
  if (findFirstUnsupportedFlag(flags) != std::string_view::npos) {
    return std::nullopt;
  }
  if (flags.empty()) {
    return regex;
  }
  // In Google RE2 the flags are directly part of the regex.
  return absl::StrCat("(?", flags, ":", regex, ")");
}

// _____________________________________________________________________________
void ensureIsValidRegexFlags(std::string_view flags) {
  size_t firstUnsupportedFlag = findFirstUnsupportedFlag(flags);
  if (firstUnsupportedFlag == std::string_view::npos) {
    return;
  }
  // Spell out the `supportedRegexFlags` as `'i', 'm', 's', 'U'`, so that the
  // message stays in sync with them.
  auto quoted = ql::views::transform(supportedRegexFlags, [](char flag) {
    return absl::StrCat("'", std::string_view{&flag, 1}, "'");
  });
  throw std::runtime_error{absl::StrCat(
      "Invalid regex flag '", flags.substr(firstUnsupportedFlag, 1),
      "' found in \"", flags, "\". The only supported flags are ",
      ad_utility::lazyStrJoin(quoted, ", "), ", and any combination of them")};
}

// _____________________________________________________________________________
void ensureIsValidRegex(std::string_view regex) {
  RE2 compiledRegex{regex, RE2::Quiet};
  if (!compiledRegex.ok()) {
    throw std::runtime_error{absl::StrCat(
        "The regex \"", regex,
        "\" is not supported by QLever (which uses Google's RE2 library); "
        "the error from RE2 is: ",
        compiledRegex.error())};
  }
}

}  // namespace sparqlExpression::detail
