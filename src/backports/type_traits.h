// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#ifndef QLEVER_SRC_BACKPORTS_TYPE_TRAITS_H
#define QLEVER_SRC_BACKPORTS_TYPE_TRAITS_H

#include <range/v3/utility/common_type.hpp>
#include <type_traits>

// This defines several traits from the `<type_traits>` header from C++20 inside
// the `ql::` namespace, s.t. they can be used also in C++17. In particular:
// `std::type_identity`, `std::remove_cvref`.
// For `std::common_reference` these are aliases to the `std` implementations in
// C+20 and aliases to the `range-v3` implementation in C++17.
namespace ql {
template <typename T>
struct type_identity {
  using type = T;
};

template <typename T>
using type_identity_t = typename type_identity<T>::type;

template <typename T>
struct remove_cvref {
  using type = std::remove_cv_t<std::remove_reference_t<T>>;
};

template <typename T>
using remove_cvref_t = typename remove_cvref<T>::type;

#ifdef QLEVER_CPP_17
using ::ranges::common_reference;
using ::ranges::common_reference_t;
#else
using std::common_reference;
using std::common_reference_t;
#endif

}  // namespace ql

#endif  // QLEVER_SRC_BACKPORTS_TYPE_TRAITS_H
