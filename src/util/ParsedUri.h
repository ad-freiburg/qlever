//  Copyright 2026 The QLever Authors, in particular:
//
//  2026 Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>, UFR
//
//  UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_UTIL_PARSEDURI_H
#define QLEVER_UTIL_PARSEDURI_H

#include <boost/url/url.hpp>
#include <boost/url/url_view.hpp>
#include <string_view>
#include <type_traits>

#include "util/Exception.h"
#include "util/UriParserUri.h"

namespace qlever::util {
// Wrapper class that combines the interfaces of `UriParserUri` and
// `boost::urls::url` into a single class allowing users to toggle between the
// implementations using the `specCompliant` template parameter. The reason this
// class exists in the first place is because the way `Boost.URL` resolves
// relative IRIs does not 100% match the SPARQL standard, but is convenient to
// use, whereas the `uriparser` C library is fully compliant with the SPARQL
// standard, but has a more cumbersome interface. In the future we might decide
// to drop one or the other.
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

  // Resolve this as a base URI against `uriString` and return the result.
  ParsedUriImpl resolveUri(std::string_view uriString) const {
    if constexpr (specCompliant) {
      return ParsedUriImpl{uri_.resolveUri(uriString)};
    } else {
      boost::urls::url uri = uri_;
      uri.resolve(boost::urls::url_view{uriString});
      return ParsedUriImpl{std::move(uri)};
    }
  }

  // Serialize a string representation of the equivalent IRI with `<>`.
  std::string toIriString() const {
    std::string targetIri;
    if constexpr (specCompliant) {
      int charsRequired;
      auto printResult = uriToStringCharsRequiredA(&uri_.get(), &charsRequired);
      AD_CORRECTNESS_CHECK(printResult == URI_SUCCESS);
      targetIri.resize(charsRequired + 2);
      printResult = uriToStringA(targetIri.data() + 1, &uri_.get(),
                                 charsRequired + 1, nullptr);
      AD_CORRECTNESS_CHECK(printResult == URI_SUCCESS);
    } else {
      targetIri.resize(uri_.buffer().size() + 2);
      std::memcpy(targetIri.data() + 1, uri_.buffer().data(),
                  uri_.buffer().size());
    }
    targetIri.front() = '<';
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
}  // namespace qlever::util

#endif  // QLEVER_UTIL_PARSEDURI_H
