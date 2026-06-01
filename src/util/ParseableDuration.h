// Copyright 2026 The QLever Authors, in particular:
//
// 2023 Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>, UFR
// 2025, Bayerische Motoren Werke Aktiengesellschaft (BMW AG)
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR

// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_PARSEABLEDURATION_H
#define QLEVER_PARSEABLEDURATION_H

#include <chrono>
#include <iostream>

#include "backports/keywords.h"
#include "backports/three_way_comparison.h"
#include "util/Exception.h"

namespace ad_utility {

// Wrapper type for std::chrono::duration<> that adds the functionality to
// convert a duration from/to `string`, `istream` and `ostream`.
// Note: Most of the member functions of this templated class are defined inside
// `ParseableDuration.cpp` together with explicit instantiations for the most
// common duration types.
template <typename DT>
class ParseableDuration {
 public:
  using DurationType = DT;

 private:
  DurationType duration_{};

  template <typename Other>
  friend class ParseableDuration;

 public:
  ParseableDuration() = default;
  // Implicit conversion is on purpose!
  QL_EXPLICIT(false)
  ParseableDuration(DurationType duration) : duration_{duration} {}
  QL_EXPLICIT(false) operator DurationType() const { return duration_; }

  // TODO default this implementation (and remove explicit equality) once libc++
  // supports it.
  auto compareThreeWay(const ParseableDuration& other) const noexcept {
    return ql::compareThreeWay(duration_.count(), other.duration_.count());
  }
  QL_DEFINE_CUSTOM_THREEWAY_OPERATOR_LOCAL(ParseableDuration)

  template <typename Other>
  auto compareThreeWay(const ParseableDuration<Other>& other) const noexcept {
    using CommonType = std::common_type_t<DurationType, Other>;
    return ql::compareThreeWay(CommonType{duration_}.count(),
                               CommonType{other.duration_}.count());
  }
  QL_DEFINE_CUSTOM_THREEWAY_OPERATOR_LOCAL_TEMPLATE(template <typename Other>,
                                                    ParseableDuration<Other>)

  QL_DEFINE_DEFAULTED_EQUALITY_OPERATOR(ParseableDuration, duration_)

  // The following two functions use an `Impl`method s.t. they at the same time
  // can
  // 1. use the hidden friend idiom.
  // 2. Still can be mostly implemented in the .cpp file.
  friend std::istream& operator>>(std::istream& is, ParseableDuration& result) {
    return operatorIstreamImpl(is, result);
  }

  // ___________________________________________________________________________
  friend std::ostream& operator<<(std::ostream& os,
                                  const ParseableDuration& duration) {
    return operatorOstreamImpl(os, duration);
  }

  // ___________________________________________________________________________
  static ParseableDuration fromString(std::string_view arg);

  // ___________________________________________________________________________
  std::string toString() const;

 private:
  // ___________________________________________________________________________
  static std::ostream& operatorOstreamImpl(std::ostream& os,
                                           const ParseableDuration& duration);

  // ___________________________________________________________________________
  static std::istream& operatorIstreamImpl(std::istream& is,
                                           ParseableDuration& result);
};
}  // namespace ad_utility

#endif  // QLEVER_PARSEABLEDURATION_H
