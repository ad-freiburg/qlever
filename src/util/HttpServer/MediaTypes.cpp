//  Copyright 2021, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include "MediaTypes.h"

#include <absl/strings/str_join.h>

#include "../Exception.h"
#include "../StringUtils.h"
#include "../TypeTraits.h"
#include "../antlr/ANTLRErrorHandling.h"
#include "./HttpParser/AcceptHeaderQleverVisitor.h"
#include "./HttpParser/generated/AcceptHeaderLexer.h"

using std::string;
namespace ad_utility {

namespace detail {
// _____________________________________________________________
const ad_utility::HashMap<MediaType, MediaTypeImpl>& getAllMediaTypes() {
  static const ad_utility::HashMap<MediaType, MediaTypeImpl> types = [] {
    ad_utility::HashMap<MediaType, MediaTypeImpl> t;
    auto add = [&t](MediaType type, std::string typeString,
                    std::string subtypeString, std::vector<std::string> v) {
      AD_CHECK(!t.contains(type));
      t.insert(std::make_pair(
          type, MediaTypeImpl(type, std::move(typeString),
                              std::move(subtypeString), std::move(v))));
    };
    using enum MediaType;
    add(html, "text", "html", {".htm", ".html", ".php"});
    add(css, "text", "css", {".css"});
    add(textPlain, "text", "plain", {".txt"});
    add(javascript, "application", "javascript", {".js"});
    add(MediaType::json, "application", "json", {".json"});
    add(xml, "application", "xml", {".xml"});
    add(flash, "application", "x-shockwave-flash", {".swf"});
    add(flv, "video", "x-flv", {".flv"});
    add(png, "image", "png", {".png"});
    add(jpeg, "image", "jpeg", {".jpe", ".jpg", ".jpeg"});
    add(gif, "image", "gif", {".gif"});
    add(bmp, "image", "bmp", {".bmp"});
    add(ico, "image", "vnd.microsof.icon", {".ico"});
    add(tiff, "image", "tiff", {".tiff", ".tif"});
    add(svg, "image", "svg+xml", {".svgz"});
    add(tsv, "text", "tab-separated-values", {".tsv"});
    add(csv, "text", "csv", {".csv"});
    add(defaultType, "application", "text", {""});
    add(sparqlJson, "application", "sparql-results+json", {});
    add(qleverJson, "application", "qlever-results+json", {});
    add(turtle, "text", "turtle", {".ttl"});
    add(octetStream, "application", "octet-stream", {});
    return t;
  }();
  return types;
}

// ________________________________________________________________
const auto& getSuffixToMediaTypeStringMap() {
  static const auto map = []() {
    const auto& types = getAllMediaTypes();
    ad_utility::HashMap<std::string, std::string> map;
    for (const auto& [type, impl] : types) {
      for (const auto& suffix : impl._fileSuffixes) {
        AD_CHECK(!map.contains(suffix));
        map[suffix] = impl._asString;
      }
    }
    return map;
  }();
  return map;
}

// ______________________________________________________________
const auto& getStringToMediaTypeMap() {
  static const auto map = []() {
    ad_utility::HashMap<std::string, MediaType> map;
    for (const auto& [mediaType, impl] : getAllMediaTypes()) {
      map[impl._asString] = mediaType;
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
  const auto& m = detail::getAllMediaTypes();
  AD_CHECK(m.contains(t));
  return m.at(t)._asString;
}

// _______________________________________________________________________
const std::string& getType(MediaType t) {
  const auto& m = detail::getAllMediaTypes();
  AD_CHECK(m.contains(t));
  return m.at(t)._type;
}

// ________________________________________________________________________
std::optional<MediaType> toMediaType(std::string_view s) {
  auto lowercase = ad_utility::getLowercaseUtf8(s);
  const auto& m = detail::getStringToMediaTypeMap();
  if (m.contains(lowercase)) {
    return m.at(lowercase);
  } else {
    return std::nullopt;
  }
}

// ___________________________________________________________________________
std::vector<MediaTypeWithQuality> parseAcceptHeader(
    std::string_view acceptHeader, std::vector<MediaType> supportedMediaTypes) {
  struct ParserAndVisitor {
   private:
    string input;
    antlr4::ANTLRInputStream stream{input};
    AcceptHeaderLexer lexer{&stream};
    antlr4::CommonTokenStream tokens{&lexer};

   public:
    AcceptHeaderParser parser{&tokens};
    AcceptHeaderQleverVisitor visitor;
    explicit ParserAndVisitor(string toParse,
                              std::vector<MediaType> supportedMediaTypes)
        : input{std::move(toParse)}, visitor{std::move(supportedMediaTypes)} {
      parser.setErrorHandler(std::make_shared<ThrowingErrorStrategy>());
    }
  };

  auto p = ParserAndVisitor{std::string{acceptHeader},
                            std::move(supportedMediaTypes)};
  try {
    auto context = p.parser.acceptWithEof();
    auto resultAsAny = p.visitor.visitAcceptWithEof(context);
    auto result =
        std::move(resultAsAny.as<std::vector<MediaTypeWithQuality>>());
    std::sort(result.begin(), result.end(), std::greater<>{});
    return result;
  } catch (const antlr4::ParseCancellationException& p) {
    throw antlr4::ParseCancellationException(
        "Error while parsing accept header \"" + std::string{acceptHeader} +
        "\". " + p.what());
  }
}

// ___________________________________________________________________________
std::optional<MediaType> getMediaTypeFromAcceptHeader(
    std::string_view acceptHeader,
    const std::vector<MediaType>& supportedMediaTypes) {
  AD_CHECK(!supportedMediaTypes.empty());
  // empty accept Header means "any type is allowed", so simply choose one.
  if (acceptHeader.empty()) {
    return *supportedMediaTypes.begin();
  }

  auto orderedMediaTypes = parseAcceptHeader(acceptHeader, supportedMediaTypes);

  auto getMediaTypeFromPart = [&supportedMediaTypes]<typename T>(
                                  const T& part) -> std::optional<MediaType> {
    const std::optional<MediaType> noValue = std::nullopt;
    if constexpr (ad_utility::isSimilar<T, MediaTypeWithQuality::Wildcard>) {
      return *supportedMediaTypes.begin();
    } else if constexpr (ad_utility::isSimilar<
                             T, MediaTypeWithQuality::TypeWithWildcard>) {
      auto it = std::find_if(
          supportedMediaTypes.begin(), supportedMediaTypes.end(),
          [&part](const auto& el) { return getType(el) == part._type; });
      return it == supportedMediaTypes.end() ? noValue : *it;
    } else if constexpr (ad_utility::isSimilar<T, MediaType>) {
      auto it = std::find(supportedMediaTypes.begin(),
                          supportedMediaTypes.end(), part);
      return it != supportedMediaTypes.end() ? part : noValue;
    } else {
      static_assert(ad_utility::alwaysFalse<T>);
    }
  };

  for (const auto& mediaType : orderedMediaTypes) {
    auto match = std::visit(getMediaTypeFromPart, mediaType._mediaType);
    if (match.has_value()) {
      return match.value();
    }
  }

  // No supported `MediaType` was found, return std::nullopt.
  return std::nullopt;
}

// ______________________________________________________________________
std::string getErrorMessageForSupportedMediaTypes(
    const std::vector<MediaType>& supportedMediaTypes) {
  // TODO<joka921> Refactor this, as soon as clang supports ranges.
  std::vector<std::string> asString;
  for (const auto& type : supportedMediaTypes) {
    asString.push_back(toString(type));
  }
  return "Currently the following media types are supported: " +
         absl::StrJoin(asString, ", ");
}

}  // namespace ad_utility
