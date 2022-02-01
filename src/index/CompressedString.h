// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach<joka921> (johannes.kalmbach@gmail.com)

#pragma once

#include <functional>
#include <string>
#include <NamedType/named_type.hpp>

using std::string;

// Class to store strings that have been compressed.
// Forbids automatic conversion from the compressed strings in the vocabulary to
// "ordinary" strings to avoid bugs.
// only implements/inherits functionality from std::string that is actually used
// TODO<niklas> is there a better way to do this?
/*
class CompressedString : private string {
 public:
  CompressedString() : string() {}

  // explicit conversions from strings
  static CompressedString fromString(const string& other) { return other; }

  // ______________________________________________________________
  static CompressedString fromString(string&& other) {
    return std::move(other);
  }

  // explicit conversions to strings and string_views
  string toString() const { return *this; }

  // ______________________________________________________
  std::string_view toStringView() const { return *this; }

  //  _______________________________________________________
  bool empty() const { return string::empty(); }

  // __________________________________________________________
  const char& operator[](size_t pos) const { return string::operator[](pos); }

 private:
  // private constructors and assignments internally used by the to and from
  // string conversions
  CompressedString(string&& other) : string(std::move(other)){};

  // _____________________________________________________________
  CompressedString(const string& other) : string(other){};

  // _____________________________________________________________
  CompressedString& operator=(string&& other) {
    *this = CompressedString(std::move(other));
    return *this;
  }

  // _______________________________________________________________
  CompressedString& operator=(const string& other) {
    *this = CompressedString(other);
    return *this;
  }

};
*/
using CompressedChar = fluent::NamedType<char, struct CompressedCharTag>;
using CompressedStringView = std::basic_string_view<CompressedChar>;
class CompressedString : public std::basic_string<CompressedChar> {
 public:
  // TODO<joka921>:: is this used in a performance-critical loop?
  // Then we should optimize it again.
  static CompressedString fromString(const std::string &s) {
    CompressedString result;
    result.reserve(s.size());
    for (const char c : s) {
      result.push_back(CompressedChar{c});
    }
    return result;
  }
};
