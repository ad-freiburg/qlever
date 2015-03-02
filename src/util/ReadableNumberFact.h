// Copyright 2011, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold <buchholb>

#pragma once

#include <locale>
#include <string>

using std::string;
namespace ad_utility {
class ReadableNumberFacet: public std::numpunct<char> {
  public:
    explicit ReadableNumberFacet(size_t refs = 0) :
      std::numpunct<char>(refs) {
    }

    virtual char do_thousands_sep() const {
      return ',';
    }
    virtual string do_grouping() const {
      return "\003";
    }
};
}

