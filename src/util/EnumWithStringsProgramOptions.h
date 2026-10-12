// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// 2026 Robin Textor-Falconi <textorr@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_ENUMWITHSTRINGSPROGRAMOPTIONS_H
#define QLEVER_SRC_UTIL_ENUMWITHSTRINGSPROGRAMOPTIONS_H

// This is deliberately not part of `EnumWithStrings.h`: that header is
// (via `Log.h`) included in almost every translation unit, but only the few
// executables that parse command-line options need
// `<boost/program_options.hpp>`, which is expensive to parse.
#include <boost/program_options.hpp>
#include <string>
#include <vector>

#include "util/EnumWithStrings.h"
#include "util/Exception.h"

namespace ad_utility {

// The following function enables support for the `EnumWithStrings`
// classes within `boost::program_options`. The values are parsed via the
// `fromString` method.
CPP_template(typename E)(
    requires std::is_base_of_v<ad_utility::EnumWithStringsBaseTag,
                               E>) void validate(boost::any& v,
                                                 const std::vector<std::string>&
                                                     values,
                                                 E*, int) {
  // First parse as the command line argument as a string.
  // Note: `validate` stores the result in `v`.
  std::string* dummy = nullptr;
  using namespace boost::program_options;
  boost::program_options::validate(v, values, dummy, 0);

  // Convert from string to enum.
  AD_CONTRACT_CHECK(!v.empty());
  v = E::fromString(boost::any_cast<std::string>(v));
}

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_ENUMWITHSTRINGSPROGRAMOPTIONS_H
