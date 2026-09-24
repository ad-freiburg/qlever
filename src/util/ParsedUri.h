//  Copyright 2026 The QLever Authors, in particular:
//
//  2026 Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>, UFR
//
//  UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_UTIL_PARSEDURI_H
#define QLEVER_UTIL_PARSEDURI_H

#include <string>
#include <string_view>

#include "util/Exception.h"
#include "util/UriParserUri.h"

namespace qlever::util {
// Wrapper class around `UriParserUri` that provides a convenient interface for
// parsing and resolving IRIs. The `uriparser` C library is fully compliant with
// the SPARQL standard for resolving relative IRIs, but has a rather cumbersome
// interface, which this class hides.
class ParsedUri {
 public:
  explicit ParsedUri(std::string_view uri) : uri_{uri} {}

  bool operator==(const ParsedUri& other) const { return uri_ == other.uri_; }

  // In C++17 mode we need to explicitly define this.
#ifdef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
  bool operator!=(const ParsedUri& other) const { return !(*this == other); }
#endif

  // Resolve this as a base URI against `uriString` and return the result.
  ParsedUri resolveUri(std::string_view uriString) const {
    return ParsedUri{uri_.resolveUri(uriString)};
  }

  // Serialize a string representation of the equivalent IRI with `<>`.
  std::string toIriString() const {
    std::string targetIri;
    int charsRequired;
    auto printResult = uriToStringCharsRequiredA(&uri_.get(), &charsRequired);
    AD_CORRECTNESS_CHECK(printResult == URI_SUCCESS);
    targetIri.resize(charsRequired + 2);
    printResult = uriToStringA(targetIri.data() + 1, &uri_.get(),
                               charsRequired + 1, nullptr);
    AD_CORRECTNESS_CHECK(printResult == URI_SUCCESS);
    targetIri.front() = '<';
    targetIri.back() = '>';
    return targetIri;
  }

 private:
  UriParserUri uri_;

  explicit ParsedUri(UriParserUri uri) : uri_{std::move(uri)} {}
};
}  // namespace qlever::util

#endif  // QLEVER_UTIL_PARSEDURI_H
