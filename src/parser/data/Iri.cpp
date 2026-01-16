//  Copyright 2022, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

#include "parser/data/Iri.h"

#include <ctre-unicode.hpp>

namespace {
// CTRE regex pattern for C++17 compatibility
constexpr ctll::fixed_string iriValidationRegex =
    "(?:@[a-zA-Z]+(?:-(?:[a-zA-Z]|\\d)+)*@)?"
    "<[^<>\"{}|^\\\\`\\0- ]*>";
}  // namespace

// ____________________________________________________________________________
Iri::Iri(std::string str) : _string{std::move(str)} {
  AD_CONTRACT_CHECK(ctre::match<iriValidationRegex>(_string));
}
