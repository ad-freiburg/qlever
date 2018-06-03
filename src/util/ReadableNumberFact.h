// Copyright 2011, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold <buchholb>

#pragma once

#include <locale>
#include <string>

using std::string;
namespace ad_utility {

// Facet for number formatting.
// CAREFUL!! SOMEHOW THIS IS THE CASE:
//
// From 22.3.1.1.2 Class locale::facet
//
//   The refs argument to the constructor is used for lifetime management.
//
//     — For refs == 0, the implementation performs delete static_cast(f)
//       (where f is a pointer to the facet) when the last locale object
//       containing the facet is destroyed; for refs == 1,
//       the implementation never destroys the facet.
//
// Not sure why but the implication are: better create objects
// using RIIA as ReadableNumberFacet rnf(1);
class ReadableNumberFacet : public std::numpunct<char> {
 public:
  explicit ReadableNumberFacet(size_t refs = 0) : std::numpunct<char>(refs) {}

  virtual ~ReadableNumberFacet() {}

  virtual char do_thousands_sep() const { return ','; }

  virtual string do_grouping() const { return "\003"; }
};
}  // namespace ad_utility
