//  Copyright 2021, University of Freiburg, Chair of Algorithms and Data
//  Structures. Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>
#include "TokenTestCtreHelper.h"

#include "parser/TokenizerCtre.h"

using T = TurtleTokenCtre;

// ___________________________________
bool TokenTestCtreHelper::matchInteger(std::string_view s) {
  return ctre::match<T::Integer>(s);
}

// ___________________________________________________
bool TokenTestCtreHelper::matchDecimal(std::string_view s) {
  return ctre::match<T::Decimal>(s);
}

// ________________________________________________________
bool TokenTestCtreHelper::matchDouble(std::string_view s) {
  return ctre::match<T::Double>(s);
}

// ____________________________________________________________
bool TokenTestCtreHelper::matchStringLiteralQuoteString(std::string_view s) {
  return ctre::match<T::StringLiteralQuoteString>(s);
}

// __________________________________________________________________________-
bool TokenTestCtreHelper::matchStringLiteralSingleQuoteString(
    std::string_view s) {
  return ctre::match<T::StringLiteralSingleQuoteString>(s);
}

// ___________________________________________________________________________
bool TokenTestCtreHelper::matchStringLiteralLongQuoteString(
    std::string_view s) {
  return ctre::match<T::StringLiteralLongQuoteString>(s);
}

// ____________________________________________________________________________
bool TokenTestCtreHelper::matchStringLiteralLongSingleQuoteString(
    std::string_view s) {
  return ctre::match<T::StringLiteralLongSingleQuoteString>(s);
}

// _________________________________________________________________________
bool TokenTestCtreHelper::matchIriref(std::string_view s) {
  return ctre::match<T::Iriref>(s);
}

// _________________________________________________________________________
bool TokenTestCtreHelper::matchPnCharsBaseString(std::string_view s) {
  // The result of `cls(...)` has to be bound to a named `static constexpr`
  // variable before being used as a template argument, because a `const
  // auto&` non-type template parameter can only bind to an object with
  // static storage duration, not to a temporary (unlike in C++20, which
  // allows literal-class-type template arguments directly).
  static constexpr auto pattern = cls(T::PnCharsBaseString);
  return ctre::match<pattern>(s);
}
