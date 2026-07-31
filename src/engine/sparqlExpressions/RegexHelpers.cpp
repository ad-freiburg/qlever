// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "engine/sparqlExpressions/RegexHelpers.h"

#include <re2/prog.h>
#include <re2/re2.h>
#include <re2/regexp.h>

#include <memory>

#include "backports/algorithm.h"
#include "backports/span.h"

namespace sparqlExpression::detail {

namespace {
// Return the longest common prefix of `a` and `b`.
std::string_view longestCommonPrefix(std::string_view a, std::string_view b) {
  auto mismatchInA = ql::ranges::mismatch(a, b).in1;
  return a.substr(0, mismatchInA - a.begin());
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
  bool isAnchoredAtStart = regex->op() == re2::kRegexpConcat &&
                           regex->nsub() > 0 &&
                           regex->sub()[0]->op() == re2::kRegexpBeginText;
  return isAnchoredAtStart && !containsWordBoundary(regex);
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
  // the deleter is not run if the pointer is `nullptr`.
  auto decref = [](re2::Regexp* parsedRegex) { parsedRegex->Decref(); };
  std::unique_ptr<re2::Regexp, decltype(decref)> parsed{
      re2::Regexp::Parse(regex, parseFlags, &status), decref};
  // This is also the case for an invalid regex.
  if (parsed == nullptr) {
    return {};
  }

  // Do the structural check before compiling: most regexes are rejected here
  // (e.g. because they are not anchored) and then never have to be compiled.
  if (!isSuitableForPrefixExtraction(parsed.get())) {
    return {};
  }

  // `PossibleMatchRange` needs a compiled program, because it works by walking
  // the resulting DFA. Two thirds of the memory budget is what `RE2` also
  // grants the forward program. Note that compiling can fail even for a regex
  // that parses successfully, for example if the program would be too large.
  std::unique_ptr<re2::Prog> program{
      parsed->CompileToProg(RE2::Options{RE2::Quiet}.max_mem() * 2 / 3)};
  if (program == nullptr) {
    return {};
  }

  // `PossibleMatchRange` computes bounds such that every matched string `s`
  // satisfies `lower <= s <= upper`. Every such `s` then has to start with the
  // longest common prefix `p` of the two bounds: if `s` deviated from `p` at
  // some position, then `s` would be smaller than `lower` (if it is smaller at
  // that position, or ends before it) or larger than `upper` (if it is larger
  // at that position), because both bounds start with `p`.
  std::string lower;
  std::string upper;

  // Hardcode the maximum length of the derived prefix. Longer prefixes bring
  // diminishing benefit for prefiltering. Note that a smaller bound can only
  // shorten the resulting prefix, it can never make it unsound.
  if (!program->PossibleMatchRange(&lower, &upper, 16)) {
    return {};
  }
  return std::string{longestCommonPrefix(lower, upper)};
}

}  // namespace sparqlExpression::detail
