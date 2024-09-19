// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Authors: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>
//          Hannah Bast <bast@cs.uni-freiburg.de>

#include "UrlParser.h"

using namespace ad_utility;

// _____________________________________________________________________________
UrlParser::ParsedUrl UrlParser::parseRequestTarget(std::string_view target) {
  auto urlResult = boost::urls::parse_origin_form(target);
  if (urlResult.has_error()) {
    throw std::runtime_error(
        absl::StrCat("Failed to parse URL: \"", target, "\"."));
  }
  boost::url url = urlResult.value();

  return {url.path(), paramsToMap(url.params())};
}

// _____________________________________________________________________________
ad_utility::HashMap<std::string, std::string> UrlParser::paramsToMap(
    boost::urls::params_view params) {
  ad_utility::HashMap<std::string, std::string> result;
  for (const auto& [key, value, _] : params) {
    auto [blockingElement, wasInserted] = result.insert({key, value});
    if (!wasInserted) {
      throw std::runtime_error(
          absl::StrCat("HTTP parameter \"", key, "\" is set twice. It is \"",
                       value, "\" and \"", blockingElement->second, "\""));
    }
  }
  return result;
}
