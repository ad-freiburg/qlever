// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_ONDISKLAYOUT_H
#define QLEVER_SRC_UTIL_ONDISKLAYOUT_H

#include <type_traits>

#include "backports/reflection.h"

// This file defines the macro `QL_ASSERT_ON_DISK_LAYOUT(Type, layout, hint)`
// for types that are written to disk by simply copying their bytes (for example
// because they are trivially serializable, see `util/Serializer/Serializer.h`).
// For such a type, the layout in memory is a file format, so adding, removing,
// reordering, or retyping a data member silently changes that format. The macro
// states the layout redundantly, such that a change of the type doesn't compile
// until the stated layout is also changed, and the `hint` then tells the
// developer what else has to be done (for example to update the version of the
// file format).
//
// The `layout` is a string literal that consists of the comma-separated data
// members of the type in declaration order, each as `name:type`. Whitespace is
// ignored. The `type` is one of the following:
//
// - `bool`, `char`, `u8`, `i32`, `f64`, etc. for the builtin types (unsigned
//    and signed integers and floating point numbers with their size in bits).
// - `enum<type>` for an enum with the given underlying type.
// - `[N]type` for an array with `N` elements (a C array or a `std::array`).
// - The unqualified name of the type for a class for which the layout is
//   asserted by another `QL_ASSERT_ON_DISK_LAYOUT`.
// - `{layout}` for all other classes.
//
// A bit-field is stated as `name:type:numBits`. For example:
//
// struct Entry {
//   uint64_t offset_;
//   float score_;
//   std::array<char, 4> tag_;
// };
// QL_ASSERT_ON_DISK_LAYOUT(Entry, "offset_:u64, score_:f32, tag_:[4]char",
//                          "Update the version of the entry file format");
//
// Additionally, the macro asserts that the type is trivially copyable and has
// no implicit padding (neither between its members, nor at its end), because
// the padding bytes would be written to disk with arbitrary content. A type
// that needs padding has to state it as an explicit data member that is
// initialized to zero. Types with base classes, unions, pointers, and
// references are not supported.
//
// The macro has to be used at namespace scope, in the namespace of the type
// (for a nested class, in the namespace of the outermost enclosing class), and
// directly after the definition of the type.
//
// NOTE: The layout is only checked when compiling with C++26 reflection (see
// `backports/reflection.h`), which is the case for at least one of the builds
// of the CI. Without reflection, the macro only asserts that the type is
// trivially copyable.

#ifdef QLEVER_HAS_REFLECTION

#include <array>
#include <string>
#include <string_view>

namespace ad_utility::onDiskLayout {

// True iff the layout of `T` is asserted by a `QL_ASSERT_ON_DISK_LAYOUT`
// (which declares the function `qleverOnDiskLayoutIsAsserted`, that is found
// here via argument-dependent lookup). Such a type is referred to by its name
// in the layout of the classes that contain it.
template <typename T>
constexpr bool hasAssertedLayout =
    requires { qleverOnDiskLayoutIsAsserted(std::type_identity<T>{}); };

// The description of the layout of a type, or an error message if the type is
// not suitable for being written to disk.
struct LayoutOrError {
  std::string layout_;
  std::string error_;
};

consteval LayoutOrError describeMembers(std::meta::info type);

// Return the decimal representation of `number`.
// NOTE: `std::to_string` is not yet `constexpr` in the standard library of
// GCC 16.
consteval std::string toString(size_t number) {
  std::string result;
  do {
    result.insert(result.begin(), static_cast<char>('0' + number % 10));
    number /= 10;
  } while (number != 0);
  return result;
}

// Return the name of `entity` (for example a type or a data member) for the
// layout and for error messages.
consteval std::string nameOf(std::meta::info entity) {
  return std::string{std::meta::has_identifier(entity)
                         ? std::meta::identifier_of(entity)
                         : std::meta::display_string_of(entity)};
}

// Return the description of the layout of a single `type` as documented at the
// beginning of this file.
consteval LayoutOrError describeType(std::meta::info type) {
  using namespace std::meta;
  type = remove_cv(dealias(type));
  auto bits = [&type]() { return toString(8 * size_of(type)); };
  auto error = [&type](std::string_view message) {
    return LayoutOrError{
        {},
        "The type `" + std::string{display_string_of(type)} + "` " + message};
  };
  if (type == ^^bool) {
    return {"bool", {}};
  }
  if (type == ^^char) {
    return {"char", {}};
  }
  if (is_integral_type(type)) {
    return {(is_signed_type(type) ? "i" : "u") + bits(), {}};
  }
  if (is_floating_point_type(type)) {
    return {"f" + bits(), {}};
  }
  if (is_enum_type(type)) {
    auto underlying = describeType(underlying_type(type));
    return {"enum<" + underlying.layout_ + ">", underlying.error_};
  }
  if (is_array_type(type)) {
    auto element = describeType(remove_extent(type));
    return {"[" + toString(extent(type)) + "]" + element.layout_,
            element.error_};
  }
  if (has_template_arguments(type) && template_of(type) == ^^std::array) {
    auto arguments = template_arguments_of(type);
    auto element = describeType(arguments[0]);
    return {
        "[" + toString(extract<size_t>(arguments[1])) + "]" + element.layout_,
        element.error_};
  }
  if (is_union_type(type)) {
    return error("is a union, which is not supported");
  }
  if (is_class_type(type)) {
    if (extract<bool>(substitute(^^hasAssertedLayout, {
                                                          type}))) {
      return {nameOf(type), {}};
    }
    auto members = describeMembers(type);
    return {"{" + members.layout_ + "}", members.error_};
  }
  return error(
      "cannot be written to disk (for example, because it is a pointer or a "
      "reference)");
}

// Return the description of the layout of the data members of the class
// `type` as documented at the beginning of this file. Report an error if the
// class has implicit padding or base classes.
consteval LayoutOrError describeMembers(std::meta::info type) {
  using namespace std::meta;
  constexpr auto ctx = access_context::unchecked();
  const std::string className{display_string_of(type)};
  if (!bases_of(type, ctx).empty()) {
    return {{},
            "The class `" + className +
                "` has base classes, which is not supported"};
  }
  auto paddingError = [&className](size_t numBits, std::string_view where) {
    return LayoutOrError{
        {},
        "The class `" + className + "` has " + toString(numBits) +
            " bits of implicit padding " + std::string{where} +
            ", which would be written to disk with arbitrary content. Add "
            "an explicit data member that is initialized to zero instead"};
  };
  std::string layout;
  // The offset in bits directly after the previous member.
  size_t endOfPrevious = 0;
  // NOTE: The members are deliberately stored in a named variable. GCC 16
  // rejects a recursive `consteval` function that directly iterates over the
  // temporary vector ("accessing outside its lifetime").
  auto members = nonstatic_data_members_of(type, ctx);
  for (auto member : members) {
    auto name = nameOf(member);
    auto offset = offset_of(member);
    size_t begin = 8 * offset.bytes + offset.bits;
    if (begin != endOfPrevious) {
      return paddingError(begin - endOfPrevious,
                          "before the member `" + name + "`");
    }
    auto memberType = describeType(type_of(member));
    if (!memberType.error_.empty()) {
      return memberType;
    }
    if (!layout.empty()) {
      layout += ", ";
    }
    layout += name + ":" + memberType.layout_;
    if (is_bit_field(member)) {
      layout += ":" + toString(bit_size_of(member));
    }
    endOfPrevious = begin + bit_size_of(member);
  }
  if (size_t size = 8 * size_of(type); size != endOfPrevious) {
    return paddingError(size - endOfPrevious, "at its end");
  }
  return {std::move(layout), {}};
}

// Return `text` with all whitespace removed.
consteval std::string removeWhitespace(std::string_view text) {
  std::string result;
  for (char c : text) {
    if (c != ' ' && c != '\t' && c != '\n' && c != '\r') {
      result += c;
    }
  }
  return result;
}

// Return the empty string iff `T` is suitable for being written to disk (see
// above) and its layout is the `expectedLayout`. Otherwise return an error
// message that includes the `hint`.
template <typename T>
consteval std::string_view checkLayout(std::string_view expectedLayout,
                                       std::string_view hint) {
  const std::string className{std::meta::display_string_of(^^T)};
  auto error = [&className](std::string message) {
    return std::string_view{std::define_static_string(
        "The on-disk layout of `" + className +
        "` (see `util/OnDiskLayout.h`) is invalid: " + message)};
  };
  if (!std::is_trivially_copyable_v<T>) {
    return error("The class is not trivially copyable");
  }
  auto [layout, layoutError] = describeMembers(^^T);
  if (!layoutError.empty()) {
    return error(std::move(layoutError));
  }
  if (removeWhitespace(layout) != removeWhitespace(expectedLayout)) {
    return error("The layout of the class is `" + layout +
                 "`, but the asserted layout is `" +
                 std::string{expectedLayout} + "`. " + std::string{hint} + ".");
  }
  return {};
}
}  // namespace ad_utility::onDiskLayout

#define QL_ASSERT_ON_DISK_LAYOUT(Type, expectedLayout, hint)                \
  static_assert(                                                            \
      ::ad_utility::onDiskLayout::checkLayout<Type>(expectedLayout, hint)   \
          .empty(),                                                         \
      ::ad_utility::onDiskLayout::checkLayout<Type>(expectedLayout, hint)); \
  void qleverOnDiskLayoutIsAsserted(std::type_identity<Type>)

#else

#define QL_ASSERT_ON_DISK_LAYOUT(Type, expectedLayout, hint) \
  static_assert(std::is_trivially_copyable_v<Type>,          \
                "A type with an on-disk layout has to be trivially copyable")

#endif

#endif  // QLEVER_SRC_UTIL_ONDISKLAYOUT_H
