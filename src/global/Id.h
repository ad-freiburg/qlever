// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)
#pragma once

#include <cstdint>
#include <cstring>
#include <limits>
#include <cassert>
#include <array>

#include "../util/Exception.h"
template< class To, class From >
To bit_cast(const From& from) noexcept {
  static_assert(sizeof(To) == sizeof(From));
  To t;
  std::memcpy(&t, &from, sizeof(From));
  return t;
}

/// when we really just need an Id
using Id = uint64_t;
static constexpr Id ID_NO_VALUE = std::numeric_limits<Id>::max() - 2;



class FancyId {
 public:
  enum Type : uint8_t {
    VOCAB = 0,
    LOCAL_VOCAB = 1,
    DATE = 2,
    INTEGER = 3,
    FLOAT = 7
  };
  static constexpr uint64_t INTERNAL_MAX_VAL =~ (7ull << 61);
  static constexpr int64_t INTEGER_MAX_VAL =~ (15ull << 60);
  static constexpr int64_t INTEGER_MIN_VAL = (15ull << 60);
  static constexpr uint64_t MAX_VAL = INTERNAL_MAX_VAL - 1;
  static constexpr uint32_t TAG_MASK = 7ull << 29;
  static constexpr uint32_t SIGN_MASK = 1ull << 28;
// A value to use when the result should be empty (e.g. due to an optional join)
  static FancyId NoValue () { return FancyId(VOCAB, INTERNAL_MAX_VAL);}
  // TODO<joka921, C++20> with std::bit_cast this can be constexpr
  static uint32_t nan() {
    return bit_cast<uint32_t>(std::numeric_limits<float>::quiet_NaN());
  }

  // unchecked, undefined behavior if we don't hold a float
  constexpr float getFloat() const noexcept {
    assert(type() == FLOAT);
    return value_.un.f;
  }
  // unchecked, undefined behavior if we don't hold any of the uint64_t based types
  constexpr uint64_t getUnsigned() const noexcept {
    assert(type() != FLOAT);
    // get the low order bits
    uint64_t res = value_.un.rest;
    res |= static_cast<uint64_t>(value_.tagAndHigh & (~TAG_MASK)) << 32u;
    return res;
  }

  constexpr uint64_t getInteger() const noexcept {
    assert(type() == INTEGER);
    bool isneg = value_.tagAndHigh & SIGN_MASK;
    uint64_t res = value_.un.rest;
    res |= static_cast<uint64_t>(value_.tagAndHigh & (~TAG_MASK)) << 32u;
    if (isneg) {
      res |= static_cast<uint64_t>(TAG_MASK) << 32u;
    }
    return res;

  }

  /// This constructor leaves the FancyId unitialized for performance reasons and is thus unsafe.
  /// TODO<joka921> measure, if initializing to some kind of zero makes a difference
  FancyId() noexcept = default;

  explicit FancyId(float f) {
    value_.un.f = f;
    value_.tagAndHigh = std::numeric_limits<uint32_t>::max();
  }



  constexpr FancyId(Type t, uint64_t val) : value_() {
    // low bits
    value_.un.rest = static_cast<uint32_t>(val);
    value_.tagAndHigh = val >> 32u;


    if (t == FLOAT) {
      throw std::runtime_error("Wrong fancyId constructor used, should never happen, please report");
    }
    if (t != INTEGER && val >= INTERNAL_MAX_VAL) {
      throw std::runtime_error("Value is too big to be represented by a fancy Id");
    }
    auto intval = static_cast<int64_t>(val);
    if (t == INTEGER && (intval > INTEGER_MAX_VAL || intval < INTEGER_MIN_VAL) ) {
      throw std::runtime_error("Integer Value is out of bounds for QLever's internal representation");

    }
    value_.tagAndHigh &= ~TAG_MASK;  // clear the tag bits
    value_.tagAndHigh |= static_cast<uint32_t>(t) << 29;
    auto t2 = value_.tagAndHigh >> 29u;
    if (t2 != t) {
      throw std::runtime_error("wrong type in constructor");
    }
  }

  template <typename F>
  static FancyId binFloatOp(FancyId a, FancyId b, F f) {
  int isNan = a.type() != FLOAT || b.type() != FLOAT;
  uint32_t bitmask = 0;
  // set all bits either to 0 or to one;
  for (size_t i = 0; i < 32u; ++i) {
    bitmask |= isNan << i;
  }

  FancyId res;
  res.value_.tagAndHigh = std::numeric_limits<uint32_t>::max();
  res.value_.un.f = f(a.value_.un.f, b.value_.un.f);
  auto nan = FancyId::nan();
  res.value_.un.f = bit_cast<float>((bitmask & nan) | ((~bitmask) & bit_cast<uint32_t>(res.value_.un.f)));
  return res;
  }

  constexpr Type type() const {
    return static_cast<Type>(value_.tagAndHigh >> 29u);
  }

  friend FancyId operator+(FancyId a, FancyId b) {
    return FancyId::binFloatOp(a, b, [](float x, float y){return x + y;});
  }

  friend FancyId operator-(FancyId a, FancyId b) {
    return FancyId::binFloatOp(a, b, [](float x, float y){return x - y;});
  }

  friend FancyId operator*(FancyId a, FancyId b) {
    return FancyId::binFloatOp(a, b, [](float x, float y){return x * y;});
  }

  friend FancyId operator/(FancyId a, FancyId b) {
    return FancyId::binFloatOp(a, b, [](float x, float y){return x / y;});
  }

  template <typename F>
  static bool compare(FancyId a, FancyId b, F f) {
    if (a.type() != b.type()) {
      return f(a.type(), b.type());
    }
    if (a.type() == FLOAT) {
      return f(a.getFloat(), b.getFloat());
    }
    return f(a.getUnsigned(), b.getUnsigned());
  }

  friend bool operator==(FancyId a, FancyId b) {
    if (a.type() != b.type()) {
      return false;
    }

    if (a.type() == FLOAT) {
      return a.getFloat() == b.getFloat();
    }
    return a.getUnsigned() == b.getUnsigned();
  }

  friend bool operator!=(FancyId a, FancyId b) {
    if (a.type() != b.type()) {
      return true;
    }

    if (a.type() == FLOAT) {
      return a.getFloat() != b.getFloat();
    }
    return a.getUnsigned() != b.getUnsigned();
  }

  friend bool operator<(FancyId a, FancyId b) {
    return FancyId::compare(a, b, [](const auto& a, const auto& b){return a < b;});
  }
  friend bool operator<=(FancyId a, FancyId b) {
    return FancyId::compare(a, b, [](const auto& a, const auto& b){return a <= b;});
  }
  friend bool operator>(FancyId a, FancyId b) {
    return FancyId::compare(a, b, [](const auto& a, const auto& b){return a > b;});
  }
  friend bool operator>=(FancyId a, FancyId b) {
    return FancyId::compare(a, b, [](const auto& a, const auto& b){return a >= b;});
  }

  friend decltype(auto) operator<<(std::ostream& str, const FancyId id) {
    auto tp = [](const FancyId::Type& t) {
      switch (t) {
        case FancyId::VOCAB : return "voc";
        case FancyId::LOCAL_VOCAB : return "local";
        case FancyId::DATE : return "date";
        case FancyId::FLOAT : return "float";
        case FancyId::INTEGER : return "int";
        default: AD_CHECK(false);
      }
    };
    if (id.type() == FLOAT) {
      return str << id.getFloat() << 'f';
    } else if (id.type() == INTEGER) {
      return str << id.getInteger();
    } else {
      return str << id.getUnsigned() << tp(id.type());
    }
  }


 private:
  struct {
  union {
    uint32_t rest;
    float f;
  } un;
  uint32_t tagAndHigh;
  } value_;


};

// helper function
template<size_t I>
std::array<FancyId, I> makeFancyArray(std::array<size_t, I> arr) {
  std::array<FancyId, I> res;
  for (size_t i = 0; i < I; ++i) {
    res[i] = FancyId(FancyId::VOCAB, arr[i]);
  }
  return res;
}

// another temporary helper function
inline FancyId fancy(size_t i) {
  return FancyId(FancyId::VOCAB, i);
}

typedef uint16_t Score;


namespace std {
  template <> struct hash<FancyId> {
    size_t operator()(const FancyId& x) {
      return std::hash<uint64_t>{}(bit_cast<uint64_t>(x));
    }
  };
}
