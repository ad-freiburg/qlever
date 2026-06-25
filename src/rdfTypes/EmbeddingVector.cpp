// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Sebastian Walter <sebastian.walter98@gmail.com>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "rdfTypes/EmbeddingVector.h"

#include <charconv>
#include <cmath>

#include "backports/StartsWithAndEndsWith.h"
#include "global/Constants.h"

namespace ad_utility {

namespace {
// True for ASCII whitespace that may appear between array tokens.
constexpr bool isAsciiSpace(char c) {
  return c == ' ' || c == '\t' || c == '\n' || c == '\r';
}
}  // namespace

// ____________________________________________________________________________
std::optional<std::vector<float>> parseFloatVectorArrayBody(
    std::string_view body) {
  // Trim surrounding whitespace and require enclosing brackets.
  while (!body.empty() && isAsciiSpace(body.front())) {
    body.remove_prefix(1);
  }
  while (!body.empty() && isAsciiSpace(body.back())) {
    body.remove_suffix(1);
  }
  if (body.size() < 2 || body.front() != '[' || body.back() != ']') {
    return std::nullopt;
  }
  body.remove_prefix(1);
  body.remove_suffix(1);

  const char* p = body.data();
  const char* end = body.data() + body.size();
  auto skipWhitespace = [&p, end]() {
    while (p < end && isAsciiSpace(*p)) {
      ++p;
    }
  };

  // An empty array (no elements) is not a valid embedding (dimension >= 1).
  skipWhitespace();
  if (p == end) {
    return std::nullopt;
  }

  std::vector<float> result;
  while (true) {
    skipWhitespace();
    float value = 0.0f;
    auto [ptr, ec] = std::from_chars(p, end, value);
    if (ec != std::errc{} || !std::isfinite(value)) {
      return std::nullopt;
    }
    result.push_back(value);
    p = ptr;
    skipWhitespace();
    if (p == end) {
      break;
    }
    if (*p != ',') {
      return std::nullopt;
    }
    ++p;
  }
  return result;
}

// ____________________________________________________________________________
std::optional<std::vector<float>> parseFp32VectorLiteral(
    std::string_view word) {
  if (!ql::starts_with(word, "\"") ||
      !ql::ends_with(word, EMBEDDING_FP32_LITERAL_SUFFIX)) {
    return std::nullopt;
  }
  // `word` is `"` + body + `"^^<...fp32Vector>`; strip both ends to get `body`.
  word.remove_prefix(1);
  word.remove_suffix(EMBEDDING_FP32_LITERAL_SUFFIX.size());
  return parseFloatVectorArrayBody(word);
}

}  // namespace ad_utility
