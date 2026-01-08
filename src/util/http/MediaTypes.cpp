//  Copyright 2021, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>
//
// Copyright 2025, Bayerische Motoren Werke Aktiengesellschaft (BMW AG)

#include "util/http/MediaTypes.h"

#include "util/Algorithm.h"
#include "util/StringUtils.h"
#include "util/antlr/ANTLRErrorHandling.h"
#include "util/http/HttpParser/AcceptHeaderQleverVisitor.h"
#include "util/http/HttpParser/generated/AcceptHeaderLexer.h"

using std::string;
namespace ad_utility {

namespace detail {
using enum MediaType;
// The first media type in this list is the default, if no other type is
// specified in the request. It's "application/sparql-results+json", as
// required by the SPARQL standard.
constexpr std::array SUPPORTED_MEDIA_TYPES{
    sparqlJson, sparqlXml,   qleverJson,        tsv, csv, turtle,
    ntriples,   octetStream, binaryQleverExport};

// _____________________________________________________________
const ad_utility::HashMap<MediaType, MediaTypeImpl>& getAllMediaTypes() {
  static const ad_utility::HashMap<MediaType, MediaTypeImpl> types = [] {
    ad_utility::HashMap<MediaType, MediaTypeImpl> t;
    auto add = [&t](MediaType type, std::string typeString,
                    std::string subtypeString, std::vector<std::string> v) {
      AD_CONTRACT_CHECK(!t.contains(type));
      t.insert(std::make_pair(
          type, MediaTypeImpl(type, std::move(typeString),
                              std::move(subtypeString), std::move(v))));
    };
    using enum MediaType;
    add(textPlain, "text", "plain", {".txt"});
    add(json, "application", "json", {".json"});
    add(tsv, "text", "tab-separated-values", {".tsv"});
    add(csv, "text", "csv", {".csv"});
    add(sparqlJson, "application", "sparql-results+json", {});
    add(sparqlXml, "application", "sparql-results+xml", {});
    add(qleverJson, "application", "qlever-results+json", {});
    add(turtle, "text", "turtle", {".ttl"});
    add(ntriples, "application", "n-triples", {".nt"});
    add(octetStream, "application", "octet-stream", {});
    add(binaryQleverExport, "application", "qlever-export+octet-stream", {});
    return t;
  }();
  return types;
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

// _______________________________________________________________________
const std::string& toString(MediaType t) {
  const auto& m = detail::getAllMediaTypes();
  AD_CONTRACT_CHECK(m.contains(t));
  return m.at(t)._asString;
}

// _______________________________________________________________________
const std::string& getType(MediaType t) {
  const auto& m = detail::getAllMediaTypes();
  AD_CONTRACT_CHECK(m.contains(t));
  return m.at(t)._type;
}

// ________________________________________________________________________
std::optional<MediaType> toMediaType(std::string_view s) {
  auto lowercase = ad_utility::utf8ToLower(s);
  const auto& m = detail::getStringToMediaTypeMap();
  if (m.contains(lowercase)) {
    return m.at(lowercase);
  } else {
    return std::nullopt;
  }
}

// For use with `ThrowingErrorListener` in `parseAcceptHeader`.
class InvalidMediaTypeParseException : public ParseException {
 public:
  explicit InvalidMediaTypeParseException(
      std::string_view cause,
      std::optional<ExceptionMetadata> metadata = std::nullopt)
      : ParseException{cause, std::move(metadata),
                       "Parsing of media type failed:"} {}
};

// ___________________________________________________________________________
std::vector<MediaTypeWithQuality> parseAcceptHeader(
    std::string_view acceptHeader) {
  struct ParserAndVisitor {
   private:
    string input;
    antlr4::ANTLRInputStream stream{input};
    AcceptHeaderLexer lexer{&stream};
    antlr4::CommonTokenStream tokens{&lexer};
    antlr_utility::ThrowingErrorListener<InvalidMediaTypeParseException>
        errorListener_{};

   public:
    AcceptHeaderParser parser{&tokens};
    AcceptHeaderQleverVisitor visitor;
    explicit ParserAndVisitor(string toParse) : input{std::move(toParse)} {
      parser.removeErrorListeners();
      parser.addErrorListener(&errorListener_);
      lexer.removeErrorListeners();
      lexer.addErrorListener(&errorListener_);
    }
  };

  auto p = ParserAndVisitor{std::string{acceptHeader}};
  try {
    auto context = p.parser.acceptWithEof();
    auto resultAsAny = p.visitor.visitAcceptWithEof(context);
    auto result = std::any_cast<std::vector<MediaTypeWithQuality>>(
        std::move(resultAsAny));
    std::sort(result.begin(), result.end(), std::greater<>{});
    return result;
  } catch (const antlr4::ParseCancellationException& p) {
    throw antlr4::ParseCancellationException(
        "Error while parsing accept header \"" + std::string{acceptHeader} +
        "\". " + p.what());
  }
}

// ___________________________________________________________________________
std::vector<MediaType> getMediaTypesFromAcceptHeader(
    std::string_view acceptHeader) {
  // TODO: make this function not throwing by changing
  // `AcceptHeaderQleverVisitor`
  static_assert(!detail::SUPPORTED_MEDIA_TYPES.empty());

  // Empty header means the same as no header.
  if (acceptHeader.empty()) {
    return {};
  }

  auto orderedMediaTypes = parseAcceptHeader(acceptHeader);

  std::vector<MediaType> result;

  for (const auto& mediaType : orderedMediaTypes) {
    std::visit(
        [&result](const auto& part) {
          using T = std::decay_t<decltype(part)>;
          if constexpr (ad_utility::isSimilar<T,
                                              MediaTypeWithQuality::Wildcard>) {
            // Nothing to do in this case.
            (void)part;
          } else if constexpr (ad_utility::isSimilar<
                                   T, MediaTypeWithQuality::TypeWithWildcard>) {
            for (MediaType supportedType :
                 detail::SUPPORTED_MEDIA_TYPES |
                     ql::views::filter([&part](const auto& el) {
                       return getType(el) == part._type;
                     })) {
              result.push_back(supportedType);
            }
          } else if constexpr (ad_utility::isSimilar<T, MediaType>) {
            if (ad_utility::contains(detail::SUPPORTED_MEDIA_TYPES, part)) {
              result.push_back(part);
            }
          } else {
            static_assert(ad_utility::alwaysFalse<T>);
          }
        },
        mediaType._mediaType);
  }

  return result;
}

// ______________________________________________________________________
std::string getErrorMessageForSupportedMediaTypes() {
  return "Currently the following media types are supported: " +
         lazyStrJoin(
             detail::SUPPORTED_MEDIA_TYPES | ql::views::transform(toString),
             ", ");
}

}  // namespace ad_utility
