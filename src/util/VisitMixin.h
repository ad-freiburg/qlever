// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Julian Mundhahs (mundhahj@informatik.uni-freiburg.de)

#ifndef QLEVER_VISITMIXIN_H
#define QLEVER_VISITMIXIN_H

#include <variant>

#include "util/Forward.h"

// TODO add requires (BaseVariant is a Variant, Derived derives from
// BaseVariant)
/*
 * A Mixin for classes that are derived from std::variant that provides a visit
 * member function. This visit function behaves like std::visit.
 * std::visit does not work with classes derived from std::variant in C++17 -
 * C++20 See also https://cplusplus.github.io/LWG/issue3052
 */
template <typename Derived, typename BaseVariant>
class VisitMixin {
 public:
  // TODO<C++23> use the `deducing this` feature.
  template <typename F>
  decltype(auto) visit(F&& f) {
    return std::visit(AD_FWD(f),
                      static_cast<BaseVariant&>(static_cast<Derived&>(*this)));
  }

  template <typename F>
  decltype(auto) visit(F&& f) const {
    return std::visit(AD_FWD(f), static_cast<const BaseVariant&>(
                                     static_cast<const Derived&>(*this)));
  }
};

#endif  // QLEVER_VISITMIXIN_H
