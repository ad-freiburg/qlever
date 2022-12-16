//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Authors: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>
//           Hannah Bast <bast@cs.uni-freiburg.de>

#include "./HttpUtils.h"

#include "ctre/ctre.h"

// TODO: Which other implementations that are currently still in `HttpUtils.h`
// should we move here, to `HttpUtils.cpp`?

namespace ad_utility::httpUtils {

// The regex for parsing the components of a URL. We need it also as a string
// (for error messagin) and CTRE has no function for printing a regex as a
// a string, hence the two variables.
static constexpr char urlRegexString[] =
    "^(http|https)://([^:/]+)(:([0-9]+))?(/.*)?$";
static constexpr auto urlRegex = ctll::fixed_string(urlRegexString);

// ____________________________________________________________________________
UrlComponents::UrlComponents(const std::string_view url) {
  auto match = ctre::search<urlRegex>(url);
  if (!match) {
    throw std::runtime_error(
        absl::StrCat("URL malformed, must match regex ", urlRegexString));
  }
  protocol =
      match.get<1>().to_string() == "http" ? Protocol::HTTP : Protocol::HTTPS;
  host = match.get<2>().to_string();
  port = match.get<4>().to_string();
  if (port.empty()) {
    port = protocol == Protocol::HTTP ? "80" : "443";
  }
  target = match.get<5>().to_string();
}

}  // namespace ad_utility::httpUtils
