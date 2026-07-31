// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_REGEXHELPERS_H
#define QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_REGEXHELPERS_H

#include <string>
#include <string_view>

namespace sparqlExpression::detail {

// Return the longest literal prefix such that every string matched by `regex`
// is guaranteed to start with it. Return the empty string if no non-trivial
// prefix can be proven; this needs no special handling by the caller, as every
// string trivially starts with the empty prefix.
//
// This is used for prefiltering: a `REGEX` filter can never match a value that
// does not start with the prefix, so blocks that only contain values outside of
// the range of the prefix can be skipped. The actual regex is always evaluated
// on the remaining values afterwards. A prefix that is shorter than the
// theoretical optimum therefore only costs performance, while a prefix that is
// too long silently yields wrong results. The analysis is conservative
// accordingly: it returns the empty string whenever it cannot prove a prefix.
//
// The reasoning is delegated to RE2 (see the implementation), so escaping,
// character classes, quantifiers, `\Q...\E`, inline flags like `(?i)`, and
// UTF-8 are all handled by the same library that later evaluates the regex.
//
// Examples: `^abc` -> "abc", `^abc[0-9]` -> "abc", `^ab{2}c` -> "abbc",
// `^a\.b` -> "a.b", `^(?:ab)c` -> "abc", `abc` -> "" (not anchored),
// `^abc|def` -> "" (the `^` only applies to the first alternative, so "Xdef" is
// matched too), `^(?i)abc` -> "" (matches "ABC" as well).
std::string getLiteralPrefixOfRegex(std::string_view regex);

}  // namespace sparqlExpression::detail

#endif  // QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_REGEXHELPERS_H
