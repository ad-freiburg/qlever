// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Julian Mundhahs (mundhahj@informatik.uni-freiburg.de)

#ifndef QLEVER_VISITMIXIN_H
#define QLEVER_VISITMIXIN_H

#include <variant>

#include "../util/Forward.h"

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
  // TODO<C++23> deducing this
  auto visit(auto&& f)
      -> decltype(std::visit(AD_FWD(f), std::declval<BaseVariant&>())) {
    return std::visit(AD_FWD(f), static_cast<const BaseVariant&>(
                                     static_cast<Derived&>(*this)));
  }
  auto visit(auto&& f) const
      -> decltype(std::visit(AD_FWD(f), std::declval<const BaseVariant&>())) {
    return std::visit(AD_FWD(f), static_cast<const BaseVariant&>(
                                     static_cast<const Derived&>(*this)));
  }
};

#endif  // QLEVER_VISITMIXIN_H
