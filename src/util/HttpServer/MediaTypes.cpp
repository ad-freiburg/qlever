//  Copyright 2021, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>
#include "MediaTypes.h"

#include "../Exception.h"
#include "../StringUtils.h"

using std::string;
namespace ad_utility {

namespace detail {
// _____________________________________________________________
const std::vector<MediaTypeImpl>& getAllMediaTypes() {
  static const std::vector<MediaTypeImpl> types = [] {
    std::vector<MediaTypeImpl> t;
    auto add = [&t](MediaType type, std::string s, std::vector<std::string> v) {
      t.emplace_back(type, std::move(s), std::move(v));
    };
    using enum MediaType;
    add(html, "text/html", {".htm", ".html", ".php"});
    add(css, "text/css", {".css"});
    add(textPlain, "text/plain", {".txt"});
    add(javascript, "application/javascript", {".js"});
    add(MediaType::json, "application/json", {".json"});
    add(xml, "application/xml", {".xml"});
    add(flash, "application/x-shockwave-flash", {".swf"});
    add(flv, "video/x-flv", {".flv"});
    add(png, "video/.png", {"image/png"});
    add(jpeg, "image/jpeg", {".jpe", ".jpg", ".jpeg"});
    add(gif, "image/gif", {".gif"});
    add(bmp, "image/bmp", {".bmp"});
    add(ico, "image/vnd.microsof.icon", {".ico"});
    add(tiff, "image/tiff", {".tiff", ".tif"});
    add(svg, "image/svg+xml", {".svgz"});
    add(tsv, "text/tab-separated-values", {".tsv"});
    add(csv, "text/csv", {".csv"});
    add(defaultType, "application/text", {""});
    return t;
  }();
  return types;
}

// ________________________________________________________________
const auto& getSuffixToMediaTypeStringMap() {
  static const auto map = []() {
    const auto& types = getAllMediaTypes();
    ad_utility::HashMap<std::string, std::string> map;
    for (const auto& type : types) {
      for (const auto& suffix : type._fileSuffixes) {
        AD_CHECK(!map.contains(suffix));
        map[suffix] = type._asString;
      }
    }
    return map;
  }();
  return map;
}

// _____________________________________________________________
const auto& getMediaTypeToStringMap() {
  static const auto map = []() {
    const auto& types = getAllMediaTypes();
    ad_utility::HashMap<MediaType, std::string> map;
    for (const auto& type : types) {
      AD_CHECK(!map.contains(type._mediaType));
      map[type._mediaType] = type._asString;
    }
    return map;
  }();
  return map;
}

}  // namespace detail

// _______________________________________________________________
const string& mediaTypeForFilename(std::string_view filename) {
  const auto ext = [&filename] {
    const auto pos = filename.rfind('.');
    if (pos == std::string_view::npos) {
      return std::string_view{};
    } else {
      return filename.substr(pos);
    }
  }();
  auto extLower = ad_utility::getLowercaseUtf8(ext);
  const auto& map = detail::getSuffixToMediaTypeStringMap();
  if (map.contains(extLower)) {
    return map.at(extLower);
  } else {
    // the default type is "application/text"
    return map.at("");
  }
}

// _______________________________________________________________________
const std::string& toString(MediaType t) {
  const auto& m = detail::getMediaTypeToStringMap();
  AD_CHECK(m.contains(t));
  return m.at(t);
}

}  // namespace ad_utility
