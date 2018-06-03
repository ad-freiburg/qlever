// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)
#pragma once

#include <array>
#include <tuple>
#include "../global/Id.h"

using std::array;
using std::tuple;

struct SortByPSO {
  // comparison function
  bool operator()(const array<Id, 3>& a, const array<Id, 3>& b) const {
    if (a[1] == b[1]) {
      if (a[0] == b[0]) {
        return a[2] < b[2];
      }
      return a[0] < b[0];
    }
    return a[1] < b[1];
  }

  // min sentinel = value which is strictly smaller that any input element
  static array<Id, 3> min_value() { return array<Id, 3>{{0, 0, 0}}; }

  // max sentinel = value which is strictly larger that any input element
  static array<Id, 3> max_value() {
    Id max = std::numeric_limits<Id>::max();
    return array<Id, 3>{{max, max, max}};
  }
};

struct SortByPOS {
  // comparison function
  bool operator()(const array<Id, 3>& a, const array<Id, 3>& b) const {
    if (a[1] == b[1]) {
      if (a[2] == b[2]) {
        return a[0] < b[0];
      }
      return a[2] < b[2];
    }
    return a[1] < b[1];
  }

  // min sentinel = value which is strictly smaller that any input element
  static array<Id, 3> min_value() { return array<Id, 3>{{0, 0, 0}}; }

  // max sentinel = value which is strictly larger that any input element
  static array<Id, 3> max_value() {
    Id max = std::numeric_limits<Id>::max();
    return array<Id, 3>{{max, max, max}};
  }
};

struct SortBySPO {
  // comparison function
  bool operator()(const array<Id, 3>& a, const array<Id, 3>& b) const {
    if (a[0] == b[0]) {
      if (a[1] == b[1]) {
        return a[2] < b[2];
      }
      return a[1] < b[1];
    }
    return a[0] < b[0];
  }

  // min sentinel = value which is strictly smaller that any input element
  static array<Id, 3> min_value() { return array<Id, 3>{{0, 0, 0}}; }

  // max sentinel = value which is strictly larger that any input element
  static array<Id, 3> max_value() {
    Id max = std::numeric_limits<Id>::max();
    return array<Id, 3>{{max, max, max}};
  }
};

struct SortBySOP {
  // comparison function
  bool operator()(const array<Id, 3>& a, const array<Id, 3>& b) const {
    if (a[0] == b[0]) {
      if (a[2] == b[2]) {
        return a[1] < b[1];
      }
      return a[2] < b[2];
    }
    return a[0] < b[0];
  }

  // min sentinel = value which is strictly smaller that any input element
  static array<Id, 3> min_value() { return array<Id, 3>{{0, 0, 0}}; }

  // max sentinel = value which is strictly larger that any input element
  static array<Id, 3> max_value() {
    Id max = std::numeric_limits<Id>::max();
    return array<Id, 3>{{max, max, max}};
  }
};

struct SortByOSP {
  // comparison function
  bool operator()(const array<Id, 3>& a, const array<Id, 3>& b) const {
    if (a[2] == b[2]) {
      if (a[0] == b[0]) {
        return a[1] < b[1];
      }
      return a[0] < b[0];
    }
    return a[2] < b[2];
  }

  // min sentinel = value which is strictly smaller that any input element
  static array<Id, 3> min_value() { return array<Id, 3>{{0, 0, 0}}; }

  // max sentinel = value which is strictly larger that any input element
  static array<Id, 3> max_value() {
    Id max = std::numeric_limits<Id>::max();
    return array<Id, 3>{{max, max, max}};
  }
};

struct SortByOPS {
  // comparison function
  bool operator()(const array<Id, 3>& a, const array<Id, 3>& b) const {
    if (a[2] == b[2]) {
      if (a[1] == b[1]) {
        return a[0] < b[0];
      }
      return a[1] < b[1];
    }
    return a[2] < b[2];
  }

  // min sentinel = value which is strictly smaller that any input element
  static array<Id, 3> min_value() { return array<Id, 3>{{0, 0, 0}}; }

  // max sentinel = value which is strictly larger that any input element
  static array<Id, 3> max_value() {
    Id max = std::numeric_limits<Id>::max();
    return array<Id, 3>{{max, max, max}};
  }
};

struct SortText {
  // comparison function
  bool operator()(const tuple<Id, Id, Id, Score, bool>& a,
                  const tuple<Id, Id, Id, Score, bool>& b) const {
    if (std::get<0>(a) == std::get<0>(b)) {
      if (std::get<4>(a) == std::get<4>(b)) {
        if (std::get<1>(a) == std::get<1>(b)) {
          if (std::get<2>(a) == std::get<2>(b)) {
            return std::get<3>(a) < std::get<3>(b);
          } else {
            return std::get<2>(a) < std::get<2>(b);
          }
        } else {
          return std::get<1>(a) < std::get<1>(b);
        }
      } else {
        return !std::get<4>(a);
      }
    } else {
      return std::get<0>(a) < std::get<0>(b);
    }
  }

  // min sentinel = value which is strictly smaller that any input element
  static tuple<Id, Id, Id, Score, bool> min_value() {
    return tuple<Id, Id, Id, Score, bool>{0, 0, 0, 0, false};
  }

  // max sentinel = value which is strictly larger that any input element
  static tuple<Id, Id, Id, Score, bool> max_value() {
    Id max = std::numeric_limits<Id>::max();
    Score maxScore = std::numeric_limits<Score>::max();
    return tuple<Id, Id, Id, Score, bool>{max, max, max, maxScore, true};
  }
};