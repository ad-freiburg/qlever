//  Copyright 2026 The QLever Authors, in particular:
//
//  2026 Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>, UFR
//
//  UFR = University of Freiburg, Chair of Algorithms and Data Structures

#ifndef QLEVER_UTIL_PARSEDURI_H
#define QLEVER_UTIL_PARSEDURI_H

#include <boost/url/url.hpp>
#include <boost/url/url_view.hpp>
#include <string_view>
#include <type_traits>

#include "util/Exception.h"
#include "util/UriParserUri.h"

template <bool specCompliant>
class ParsedUriImpl {
 public:
  using Type =
      std::conditional_t<specCompliant, UriParserUri, boost::urls::url>;

  explicit ParsedUriImpl(std::string_view uri) : uri_{uri} {}

  bool operator==(const ParsedUriImpl& other) const {
    if constexpr (specCompliant) {
      return uri_ == other.uri_;
    } else {
      return uri_.buffer() == other.uri_.buffer();
    }
  }

  bool operator!=(const ParsedUriImpl& other) const {
    return !(*this == other);
  }

  ParsedUriImpl resolveUri(std::string_view uriString) const {
    if constexpr (specCompliant) {
      return ParsedUriImpl{uri_.resolveUri(uriString)};
    } else {
      boost::urls::url uri = uri_;
      uri.resolve(boost::urls::url_view{uriString});
      return ParsedUriImpl{std::move(uri)};
    }
  }

  std::string toIriString() const {
    int charsRequired;
    if constexpr (specCompliant) {
      auto printResult = uriToStringCharsRequiredA(&uri_.get(), &charsRequired);
      AD_CORRECTNESS_CHECK(printResult == URI_SUCCESS);
    } else {
      charsRequired = uri_.buffer().size();
    }
    std::string targetIri;
    targetIri.resize(charsRequired + 2);
    targetIri.at(0) = '<';
    if constexpr (specCompliant) {
      auto printResult = uriToStringA(targetIri.data() + 1, &uri_.get(),
                                      charsRequired + 1, nullptr);
      AD_CORRECTNESS_CHECK(printResult == URI_SUCCESS);
    } else {
      std::memcpy(targetIri.data() + 1, uri_.buffer().data(), charsRequired);
    }
    targetIri.back() = '>';
    return targetIri;
  }

 private:
  Type uri_;

  explicit ParsedUriImpl(Type uri) : uri_{std::move(uri)} {}
};

using ParsedUri = ParsedUriImpl<true>;
// Unused.
using BoostParsedUri = ParsedUriImpl<false>;

#endif  // QLEVER_UTIL_PARSEDURI_H
