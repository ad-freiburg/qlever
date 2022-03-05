// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Robin Textor-Falconi (textorr@informatik.uni-freiburg.de)

#pragma once

#include <absl/strings/ascii.h>
#include <absl/strings/str_split.h>

#include "./beast.h"

namespace ad_utility::content_encoding {

enum class CompressionMethod { NONE, DEFLATE, GZIP };

namespace detail {

constexpr std::string_view DEFLATE = "deflate";
constexpr std::string_view GZIP = "gzip";

inline CompressionMethod getCompressionMethodFromAcceptEncodingHeader(
    std::vector<std::string_view> acceptedEncodings) {
  const auto& contains = [&acceptedEncodings](std::string_view value) {
    return std::find(acceptedEncodings.begin(), acceptedEncodings.end(),
                     value) != acceptedEncodings.end();
  };
  if (contains(DEFLATE)) {
    return CompressionMethod::DEFLATE;
  } else if (contains(GZIP)) {
    return CompressionMethod::GZIP;
  }
  return CompressionMethod::NONE;
}
}  // namespace detail

using boost::beast::http::field;

template <typename Body>
CompressionMethod getCompressionMethodForRequest(
    const boost::beast::http::request<Body>& request) {
  auto acceptEncodingHeaders =
      request.base().equal_range(field::accept_encoding);
  std::vector<std::string_view> acceptedHeaders;
  for (auto& current = acceptEncodingHeaders.first;
       current != acceptEncodingHeaders.second; current++) {
    std::string_view headerContent = current->value();
    std::vector<std::string_view> tokens =
        absl::StrSplit(headerContent, absl::ByChar(','));
    for (auto token : tokens) {
      acceptedHeaders.push_back(absl::StripAsciiWhitespace(token));
    }
  }
  return detail::getCompressionMethodFromAcceptEncodingHeader(acceptedHeaders);
}

template <bool isRequest, typename Fields>
void setContentEncodingHeaderForCompressionMethod(
    CompressionMethod method,
    boost::beast::http::header<isRequest, Fields>& header) {
  if (method == CompressionMethod::DEFLATE) {
    header.insert(field::content_encoding, detail::DEFLATE);
  } else if (method == CompressionMethod::GZIP) {
    header.insert(field::content_encoding, detail::GZIP);
  }
}

inline std::ostream& operator<<(std::ostream& out,
                                CompressionMethod compressionMethod) {
  switch (compressionMethod) {
    case CompressionMethod::NONE:
      out << "CompressionMethod::NONE";
      break;
    case CompressionMethod::DEFLATE:
      out << "CompressionMethod::DEFLATE";
      break;
    case CompressionMethod::GZIP:
      out << "CompressionMethod::GZIP";
      break;
  }
  return out;
}

}  // namespace ad_utility::content_encoding
