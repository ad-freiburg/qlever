// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Florian Kramer (florian.kramer@mail.uni-freiburg.de)

#pragma once
#include <cstdint>
#include <cstdlib>
#include <string>

// TODO(florian): tidy up the pattern class
struct Pattern {
  static constexpr size_t MAX_NUM_RELATIONS = 64;

  Id& operator[](const size_t pos) {
    return _data[pos];
  }

  const Id& operator[](const size_t pos) const {
    return _data[pos];
  }

  bool operator==(const Pattern& other) const {
    if (_length != other._length) {
      return false;
    }
    for (size_t i = 0; i < _length; i++) {
      if (other._data[i] != _data[i]) {
        return false;
      }
    }
    return true;
  }

  bool operator!=(const Pattern& other) const {
    if (_length != other._length) {
      return true;
    }
    for (size_t i = 0; i < _length; i++) {
      if (other._data[i] != _data[i]) {
        return true;
      }
    }
    return false;
  }

  bool operator<(const Pattern &other) const {
    if (_length == 0) {
      return true;
    }
    if (other._length == 0) {
      return false;
    }
    return _data[0] < other._data[0];
  }

  bool operator>(const Pattern &other) const {
    if (other._length == 0) {
      return true;
    }
    if (_length == 0) {
      return false;
    }
    return _data[0] > other._data[0];
  }

  size_t size() const {
    return _length;
  }

  /**
   * @brief fromBuffer
   * @param buf
   * @return The number of bytes read
   */
  size_t fromBuffer(const unsigned char *buf) {
    std::memcpy(&_length, buf, sizeof(uint16_t));
    for (uint16_t i = 0; i < _length; i++) {
      std::memcpy(_data + i, buf + sizeof(uint16_t) + i * sizeof(Id), sizeof(Id));
    }
    return sizeof(uint16_t) + _length * sizeof(Id);
  }

  void setSize(uint16_t size) {
    _length = size;
  }

  uint16_t _length;
  Id _data[MAX_NUM_RELATIONS];
};

namespace std {
template <>
struct hash<Pattern> {
  std::size_t operator()(const Pattern& p) const {
    string s = string(reinterpret_cast<const char*>(p._data), sizeof(Id) * p.size());
    return hash<string>()(s);
  }
};
}

inline std::ostream& operator<<(std::ostream& o, const Pattern &p) {
  for (size_t i = 0; i + 1< p.size(); i++) {
   o << p[i] << ", ";
  }
  if (p.size() > 0) {
    o << p[p.size() - 1];
  }
  return o;
}
