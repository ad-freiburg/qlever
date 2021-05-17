// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach<joka921> (johannes.kalmbach@gmail.com)

#pragma once

/// A String/character array that can be constructed at compile time. It can hold at most `MaxSize` characters. The string is null-terminated and the '\0' at the end counts towards the maximum Size.
template <size_t MaxSize>
struct conststr
{

  char _characters[MaxSize] = {0};
  std::size_t sz = 0;
 public:
  template<std::size_t N>
  constexpr conststr(const char(&a)[N]) : sz(N - 1) {
    if (N > MaxSize) {
      throw std::runtime_error{"conststr can only be constructed from strings with a maximum size of " + std::to_string(MaxSize - 1)};
    }
    // TODO: enforce proper zero-termination
    for (size_t i = 0; i < N; ++i) {
      _characters[i] = a[i];
    }
  }

  conststr(std::string_view input) {
    if (input.size() >= MaxSize) {
      throw std::runtime_error{"conststr can only be constructed from strings with a maximum size of " + std::to_string(MaxSize - 1)};
    }
    for (size_t i = 0; i < input.size(); ++i) {
      _characters[i] = input[i];
    }
  }

  constexpr char operator[](std::size_t n) const
  {
    return n < sz ? _characters[n] : throw std::out_of_range("");
  }
  constexpr std::size_t size() const { return sz; }

  bool operator==(const conststr& rhs) const {
    return !std::strcmp(_characters, rhs._characters);
  }

};


