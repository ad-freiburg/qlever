// Copyright 2022 - 2026 The QLever Authors, in particular:
//
// 2022 - 2024 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// 2022 - 2024 Hannah Bast <bast@cs.uni-freiburg.de>, UFR
// 2026 Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_SIMPLELITERALHELPERS_H
#define QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_SIMPLELITERALHELPERS_H

#include <absl/strings/str_cat.h>

#include <stdexcept>
#include <string_view>

#include "rdfTypes/Literal.h"

namespace sparqlExpression::detail {

// Throw unless `literal` is a simple literal (no datatype and no language tag).
// Simple literals are the only literals accepted by functions like `REGEX` and
// `ql:prefix-match`. The `functionName` (e.g. `"REGEX"`) is only used to make
// the error message point at the function the user actually called.
inline void ensureIsSimpleLiteral(
    const ad_utility::triple_component::Literal& literal,
    std::string_view functionName) {
  if (literal.hasDatatype() || literal.hasLanguageTag()) {
    throw std::runtime_error{absl::StrCat(
        "The ", functionName,
        " function only accepts simple literals (literals without a language "
        "tag or a datatype)")};
  }
}

}  // namespace sparqlExpression::detail

#endif  // QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_SIMPLELITERALHELPERS_H
