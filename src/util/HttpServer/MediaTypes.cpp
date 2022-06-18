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
    // using enum MediaType;
    add(MediaType::html, "text", "html", {".htm", ".html", ".php"});
    add(MediaType::css, "text", "css", {".css"});
    add(MediaType::textPlain, "text", "plain", {".txt"});
    add(MediaType::javascript, "application", "javascript", {".js"});
    add(MediaType::json, "application", "json", {".json"});
    add(MediaType::xml, "application", "xml", {".xml"});
    add(MediaType::flash, "application", "x-shockwave-flash", {".swf"});
    add(MediaType::flv, "video", "x-flv", {".flv"});
    add(MediaType::png, "image", "png", {".png"});
    add(MediaType::jpeg, "image", "jpeg", {".jpe", ".jpg", ".jpeg"});
    add(MediaType::gif, "image", "gif", {".gif"});
    add(MediaType::bmp, "image", "bmp", {".bmp"});
    add(MediaType::ico, "image", "vnd.microsof.icon", {".ico"});
    add(MediaType::tiff, "image", "tiff", {".tiff", ".tif"});
    add(MediaType::svg, "image", "svg+xml", {".svgz"});
    add(MediaType::tsv, "text", "tab-separated-values", {".tsv"});
    add(MediaType::csv, "text", "csv", {".csv"});
    add(MediaType::defaultType, "application", "text", {""});
    add(MediaType::sparqlJson, "application", "sparql-results+json", {});
    add(MediaType::qleverJson, "application", "qlever-results+json", {});
    add(MediaType::turtle, "text", "turtle", {".ttl"});
    add(MediaType::octetStream, "application", "octet-stream", {});
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
absl::optional<MediaType> toMediaType(std::string_view s) {
  auto lowercase = ad_utility::getLowercaseUtf8(s);
  const auto& m = detail::getStringToMediaTypeMap();
  if (m.contains(lowercase)) {
    return m.at(lowercase);
  } else {
    return absl::nullopt;
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
absl::optional<MediaType> getMediaTypeFromAcceptHeader(
    std::string_view acceptHeader,
    const std::vector<MediaType>& supportedMediaTypes) {
  AD_CHECK(!supportedMediaTypes.empty());
  // empty accept Header means "any type is allowed", so simply choose one.
  if (acceptHeader.empty()) {
    return *supportedMediaTypes.begin();
  }

  auto orderedMediaTypes = parseAcceptHeader(acceptHeader, supportedMediaTypes);

  auto getMediaTypeFromPart = [&supportedMediaTypes]<typename T>(
                                  const T& part) -> absl::optional<MediaType> {
    const absl::optional<MediaType> noValue = absl::nullopt;
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

  // No supported `MediaType` was found, return absl::nullopt.
  return absl::nullopt;
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
