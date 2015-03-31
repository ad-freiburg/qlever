// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)
#pragma once

#include <array>
#include "../global/Id.h"

using std::array;

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
  static array<Id, 3> min_value()
  {
    return array<Id, 3>{{0 , 0, 0}};
  }

  // max sentinel = value which is strictly larger that any input element
  static array<Id, 3> max_value()
  {
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
  static array<Id, 3> min_value()
  {
    return array<Id, 3>{{0 , 0, 0}};
  }

  // max sentinel = value which is strictly larger that any input element
  static array<Id, 3> max_value()
  {
    Id max = std::numeric_limits<Id>::max();
    return array<Id, 3>{{max, max, max}};
  }
};