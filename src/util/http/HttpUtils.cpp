//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Authors: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>
//           Hannah Bast <bast@cs.uni-freiburg.de>

#include "./HttpUtils.h"

#include <ctre-unicode.hpp>

// TODO: Which other implementations that are currently still in `HttpUtils.h`
// should we move here, to `HttpUtils.cpp`?

namespace ad_utility::httpUtils {

// The regex for parsing the components of a URL. We need it also as a string
// (for error messaging) and CTRE has no function for printing a regex as
// a string, hence the two variables.
static constexpr char urlRegexString[] =
    "^(http|https)://([^:/]+)(:([0-9]+))?(/.*)?$";
static constexpr auto urlRegex = ctll::fixed_string(urlRegexString);

// ____________________________________________________________________________
Url::Url(std::string_view url) {
  auto match = ctre::search<urlRegex>(url);
  if (!match) {
    throw std::runtime_error(absl::StrCat(
        "URL \"", url, "\" malformed, must match regex ", urlRegexString));
  }
  protocol_ =
      match.get<1>().to_view() == "http" ? Protocol::HTTP : Protocol::HTTPS;
  host_ = match.get<2>().to_string();
  port_ = match.get<4>().to_string();
  if (port_.empty()) {
    port_ = protocol_ == Protocol::HTTP ? "80" : "443";
  }
  target_ = match.get<5>().to_string();
  if (target_.empty()) {
    target_ = "/";
  }
}

}  // namespace ad_utility::httpUtils
