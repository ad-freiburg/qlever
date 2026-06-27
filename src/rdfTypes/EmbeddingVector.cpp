// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Sebastian Walter <swalter@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "rdfTypes/EmbeddingVector.h"

#include <cmath>

#include "util/json.h"

namespace ad_utility {

// ____________________________________________________________________________
std::optional<std::vector<float>> parseFloatVectorArrayBody(
    std::string_view body) {
  // The body mirrors a JSON array of numbers (`[0.1, -0.2, 1.0]`), so parse it
  // with the JSON library used elsewhere in QLever. `allow_exceptions = false`
  // makes a malformed body return a discarded value rather than throwing.
  auto json = nlohmann::json::parse(body.begin(), body.end(),
                                    /*callback=*/nullptr,
                                    /*allow_exceptions=*/false);
  if (json.is_discarded() || !json.is_array() || json.empty()) {
    return std::nullopt;
  }

  std::vector<float> result;
  result.reserve(json.size());
  for (const auto& element : json) {
    if (!element.is_number()) {
      return std::nullopt;
    }
    // Narrow to `float`; reject values that are non-finite or that overflow the
    // `float` range (a finite JSON number too large for `float` becomes `inf`).
    auto value = element.get<float>();
    if (!std::isfinite(value)) {
      return std::nullopt;
    }
    result.push_back(value);
  }
  return result;
}

}  // namespace ad_utility
