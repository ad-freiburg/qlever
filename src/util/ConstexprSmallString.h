// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach<joka921> (johannes.kalmbach@gmail.com)

#pragma once

#include <array>
#include <cstring>
#include <exception>
#include <string>
#include <string_view>

namespace ad_utility {
/// A String/character array that can be constructed at compile time. It can
/// hold at most `MaxSize` characters. The string is null-terminated and the
/// '\0' at the end counts towards the maximum Size.
template <size_t MaxSize>
struct ConstexprSmallString {
  // Data members have to be public, else we cannot use ConstexprSmallStrings as
  // template parameters
  std::array<char, MaxSize> _characters = {0};
  std::size_t _size = 0;

  /// Construct (possibly at compile time) from input char array or input const
  /// char*. The input must be null-terminated, else the behavior is undefined.
  /// Example usage: `constexpr ConstexprSmallString<8> example = "short"'.
  template <std::size_t N>
  constexpr ConstexprSmallString(const char (&input)[N]) : _size(N - 1) {
    if (N > MaxSize) {
      throw std::runtime_error{
          "ConstexprSmallString can only be constructed from strings with "
          "input maximum size of " +
          std::to_string(MaxSize - 1)};
    }
    for (size_t i = 0; i < N; ++i) {
      _characters[i] = input[i];
    }
  }

  /// Construct at runtime from a string_view
  ConstexprSmallString(std::string_view input) : _size(input.size()) {
    if (input.size() >= MaxSize) {
      throw std::runtime_error{
          "ConstexprSmallString can only be constructed from strings with a "
          "maximum size of " +
          std::to_string(MaxSize - 1)};
    }
    for (size_t i = 0; i < input.size(); ++i) {
      _characters[i] = input[i];
    }
    // The '\0' at the end is already there because of the initialization of
    // _characters
  }

  /// Access the n-th character
  constexpr char operator[](std::size_t n) const {
    return n < _size ? _characters[n] : throw std::out_of_range("");
  }

  /// Return the size without counting the '\0' at the end.
  constexpr std::size_t size() const { return _size; }

  // TODO<C++20, joka921> implement operator<=> as soon as it works
  // on std::array.
  constexpr bool operator==(const ConstexprSmallString& rhs) const {
    return _characters == rhs._characters;
  }

  constexpr bool operator<(const ConstexprSmallString& rhs) const {
    return _characters < rhs._characters;
  }

  /// Implicit conversion to std::string_view
  operator std::string_view() const { return {_characters.data(), _size}; }
};
}  // namespace ad_utility

namespace std {
template <size_t MaxSize>
struct hash<ad_utility::ConstexprSmallString<MaxSize>> {
  auto operator()(const ad_utility::ConstexprSmallString<MaxSize>& string) {
    return std::hash<std::string_view>{}(
        std::string_view{string._characters, string._size});
  }
};
}  // namespace std
