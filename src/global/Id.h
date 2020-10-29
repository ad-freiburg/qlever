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

constexpr uint64_t  extractBits(uint64_t input, uint8_t lower, uint8_t upper) {
  input >>= lower;
  uint64_t mask = (1ull << (upper - lower)) - 1;
  return input & mask;
}




class alignas(alignof(uint64_t)) FancyId {
 public:
  enum Type : uint8_t {
    VOCAB = 0,
    LOCAL_VOCAB = 1,
    DATE = 2,
    TEXT = 3,
    // IMPORTANT: Integer and float must be consecutive for the ordering to work...
    INTEGER = 4,
    FLOAT = 7
  };
  static constexpr uint64_t INTERNAL_MAX_VAL =~ (7ull << 61);
  static constexpr int64_t INTEGER_MAX_VAL =~ (15ull << 60);
  static constexpr int64_t INTEGER_MIN_VAL = (15ull << 60);
  static constexpr uint64_t MAX_VAL = INTERNAL_MAX_VAL - 1;
  static constexpr uint32_t TAG_MASK = 7ull << 29;
  static constexpr uint32_t SIGN_MASK = 1ull << 28;
// A value to use when the result should be empty (e.g. due to an optional join)
  static constexpr FancyId NoValue () { return FancyId(VOCAB, INTERNAL_MAX_VAL, std::true_type{});}
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
    assert(type()!= INTEGER);
    // get the low order bits
    uint64_t res = value_.un.rest;
    res |= static_cast<uint64_t>(value_.tagAndHigh & (~TAG_MASK)) << 32u;
    return res;
  }

  // unchecked, unexpected behavior if no integer is held.
  constexpr int64_t getInteger() const noexcept {
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

  // construct from a float
  explicit FancyId(float f) {
    value_.un.f = f;
    value_.tagAndHigh = std::numeric_limits<uint32_t>::max();
  }

  // convert to an xsd-value-string. Only supported for types FLOAT and INTEGER
  std::string toXSDValue() const {
    switch (type()) {
      case FLOAT:
        return std::to_string(getFloat()) + "^^xsd:float";
      case INTEGER:
        return std::to_string(getInteger()) + "^^xsd:integer";
      default:
        throw std::runtime_error("Tried to get xsd-Value from FancyId that is neither Integer nor Float. This is not supported, please report this");
    }
  }

  // safe conversion to float. Cast INTEGER and FLOAT, throw exception otherwise
  float convertToFloat() {
    switch (type()) {
      case FLOAT:
        return getFloat();
      case INTEGER:
        return static_cast<float>(getInteger());
      default:
        throw std::runtime_error("Tried to convert a non-numeric type  to float in FancyId");
    }
  }


  // construct a Date.
  static constexpr FancyId Date(int year, int month = 1, int day = 1, int hour = 0, int min = 0, float sec = .0, bool signTimezone = false, int hTimezone = 0, int mTimezone = 0 ) {
    const auto raise = [](){throw std::runtime_error("input value for date out of range");};
    uint64_t v = year < 0; // start with sign bit of year
    v <<= 60;
    year = year < 0 ? -year : year;
    if (year > 9999) { raise();}
    v |= static_cast<uint64_t>(year) << 46;

    if (month < 1 || month > 12) raise();
    v |= static_cast<uint64_t>(month) << 42;

    if (day < 1 || day > 31) raise();
    v |= static_cast<uint64_t>(day) << 37;

    if (hour < 0 || hour > 23) raise();
    v |= static_cast<uint64_t>(hour) << 32;


    if (min < 0 || min > 59) raise();
    v |= static_cast<uint64_t>(min) << 26;

    if (sec < 0.0f || sec >= 60.0f) raise();
    v |= static_cast<uint64_t>(sec * 250) << 12;

    v |= static_cast<uint64_t>(signTimezone) << 11;

    if (hTimezone < 0 || hTimezone > 23) raise();
    v |= static_cast<uint64_t>(hTimezone) << 6;

    if (mTimezone < 0 || mTimezone > 59) raise();
    v |= static_cast<uint64_t>(mTimezone);

    return FancyId(FancyId::DATE, v);
  }

  struct DateValue {
    int year = 0;
    int month = 1;
    int day = 1;
    int hour = 0;
    int min = 0;
    float sec = .0;
    bool signTimezone = false;
    int hTimezone = 0;
    int mTimezone = 0;

    template<typename Comp>
    static constexpr bool compare(DateValue a, DateValue b, Comp f) {
      if (a.year != b.year) {
        return f(a.year, b.year);
      }
      if (a.month != b.month) {
        return f(a.month, b.month);
      }
      if (a.day != b.day) {
        return f(a.day, b.day);
      }
      if (a.hour != b.hour) {
        return f(a.hour, b.hour);
      }
      if (a.min != b.min) {
        return f(a.min, b.min);
      }
      if (a.sec != b.month) {
        return f(a.sec, b.month);
      }
      if (a.signTimezone != b.signTimezone) {
        // "1" in sign bit means negative
        return f(!a.signTimezone, !b.signTimezone);
      }
      // if timezonesx are negative, comparison order changes
      const auto&x = a.signTimezone? b : a;
      const auto&y = a.signTimezone? a : b;
      if (a.hTimezone != b.hTimezone) {
        return f(x.hTimezone, y.hTimezone);
      }
      return f(x.mTimezone, y.mTimezone);

    }
  };

  // unchecked conversion to date
  constexpr DateValue getDate() const {
    assert(type()==DATE);
    DateValue res;
    auto v = getUnsigned();
    res.year = extractBits(v, 46, 60);
    if (extractBits(v, 60, 61)) {
      res.year = - res.year;
    }
    res.month = extractBits(v, 42, 46);
    res.day = extractBits(v, 37, 42);
    res.hour = extractBits(v, 32, 37);
    res.min = extractBits(v, 26, 32);
    res.sec = static_cast<float>(extractBits(v, 12, 26) / 250.0f);
    res.signTimezone = extractBits(v, 11, 12);
    res.hTimezone = extractBits(v, 6, 11);
    res.mTimezone = extractBits(v, 0, 6);
    return res;
  }


  // construct from a type and a 64 bit payload
  template <typename AllowNoValue = std::false_type>
  constexpr FancyId(Type t, uint64_t val, [[maybe_unused]] AllowNoValue ttt = AllowNoValue{}) : value_() {
    // low bits
    value_.un.rest = static_cast<uint32_t>(val);
    value_.tagAndHigh = val >> 32u;


    if (t == FLOAT) {
      throw std::runtime_error("Wrong fancyId constructor used, should never happen, please report");
    }
    if constexpr (AllowNoValue::value) {
      if (t != INTEGER && val > INTERNAL_MAX_VAL) {
        throw std::runtime_error(
            "Value is too big to be represented by a fancy Id");
      }

    } else {
      if (t != INTEGER && val >= INTERNAL_MAX_VAL) {
        throw std::runtime_error(
            "Value is too big to be represented by a fancy Id");
      }
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
    int isInt = a.type() == INTEGER && b.type() == INTEGER;

    int isNan = (a.type() != FLOAT && a.type() != INTEGER) || (b.type() != FLOAT && b.type() != INTEGER);
    int isFloat = !isNan && !isInt;

    // TODO: performance improvement by branchless programming?
    if (isFloat) {
      return FancyId(f(a.convertToFloat(), b.convertToFloat()));
    } else if (isInt) {
      return FancyId(INTEGER, f(a.getInteger(), b.getInteger()));
    } else {
      return  FancyId(std::numeric_limits<float>::quiet_NaN());
    }
  }

  constexpr Type type() const {
    return static_cast<Type>(value_.tagAndHigh >> 29u);
  }

  friend FancyId operator+(FancyId a, FancyId b) {
    return FancyId::binFloatOp(a, b, [](auto x, auto y){return x + y;});
  }

  friend FancyId operator-(FancyId a, FancyId b) {
    return FancyId::binFloatOp(a, b, [](auto x, auto y){return x - y;});
  }

  friend FancyId operator*(FancyId a, FancyId b) {
    return FancyId::binFloatOp(a, b, [](auto x, auto y){return x * y;});
  }

  friend FancyId operator/(FancyId a, FancyId b) {
    return FancyId::binFloatOp(a, b, [](auto x, auto y){return x / y;});
  }

  template <typename F>
  constexpr static bool compare(FancyId a, FancyId b, F f) {
    int isNumeric = (a.type() == FLOAT  || a.type() == INTEGER) && (b.type() == FLOAT || b.type() == INTEGER);
    int isInteger = a.type() == INTEGER && b.type() == INTEGER;
    int isDate = a.type() == DATE && b.type() == DATE;
    if (!isNumeric) {
      if (isDate) {
        return DateValue::compare(a.getDate(), b.getDate(), f);
      }
      if (a.type() != b.type()) {
        return f(a.type(), b.type());
      }
      // same type, compare the held values.
      // TODO: Make this work for the dates!
      return f(a.getUnsigned(), b.getUnsigned());
    }
    if (isInteger) {
      return f(a.getInteger(), b.getInteger());
    }
    // both are numeric, but not both integers
    return f(a.convertToFloat(), b.convertToFloat());
  }

  friend bool operator==(FancyId a, FancyId b) {
    return compare(a, b, std::equal_to<>{});
  }

  friend bool operator!=(FancyId a, FancyId b) {
    return compare(a, b, std::not_equal_to<>{});
  }

  friend bool operator<(FancyId a, FancyId b) {
    return FancyId::compare(a, b, std::less<>{});
  }
  friend bool operator<=(FancyId a, FancyId b) {
    return FancyId::compare(a, b, std::less_equal<>{});
  }
  friend bool operator>(FancyId a, FancyId b) {
    return FancyId::compare(a, b, std::greater<>{});
  }
  friend bool operator>=(FancyId a, FancyId b) {
    return FancyId::compare(a, b, std::greater_equal<>{});
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

/// when we really just need an Id
using Id = FancyId;
static constexpr Id ID_NO_VALUE = Id::NoValue();

using SimpleId = uint64_t;
static constexpr SimpleId SIMPLE_ID_NO_VALUE = std::numeric_limits<SimpleId>::max() - 1;
