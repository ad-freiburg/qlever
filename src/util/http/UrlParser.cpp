// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Authors: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>
//          Hannah Bast <bast@cs.uni-freiburg.de>

#include "UrlParser.h"

using namespace ad_utility;

// _____________________________________________________________________________
UrlParser::ParsedHTTPRequest UrlParser::parseRequestTarget(
    std::string_view target) {
  UrlParser::ParsedHTTPRequest parsed;
  auto result = boost::urls::parse_origin_form(target);
  if (result.has_error()) {
    AD_THROW(absl::StrCat("Failed to parse URL: \"", target, "\"."));
  }
  boost::url url = result.value();

  parsed.path_ = url.path();
  parsed.parameters_ = paramsToMap(url.params());

  return parsed;
}

// _____________________________________________________________________________
ad_utility::HashMap<std::string, std::string> UrlParser::paramsToMap(
    boost::urls::params_view params) {
  ad_utility::HashMap<std::string, std::string> result;
  for (auto param : params) {
    auto [iterator, isNewElement] = result.insert({param.key, param.value});
    if (!isNewElement) {
      AD_THROW("Duplicate HTTP parameter: " + iterator->first);
    }
  }
  return result;
}

// _____________________________________________________________________________
ad_utility::HashMap<std::string, std::string> UrlParser::paramsToMap(
    boost::urls::params_encoded_view params) {
  ad_utility::HashMap<std::string, std::string> result;
  for (auto param : params) {
    auto [iterator, isNewElement] =
        result.insert({param.key.decode(boost::urls::encoding_opts(true)),
                       param.value.decode(boost::urls::encoding_opts(true))});
    if (!isNewElement) {
      AD_THROW("Duplicate HTTP parameter: " + iterator->first);
    }
  }
  return result;
}
