// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Robin Textor-Falconi (textorr@informatik.uni-freiburg.de)

#pragma once

#include "./beast.h"

namespace ad_utility::content_encoding {

enum class CompressionMethod { NONE, DEFLATE, GZIP };

namespace detail {

constexpr std::string_view DEFLATE = "deflate";
constexpr std::string_view GZIP = "gzip";

constexpr CompressionMethod getCompressionMethodFromAcceptEncodingHeader(
    const std::string_view acceptEncodingHeader) {
  if (acceptEncodingHeader.find(DEFLATE) != std::string::npos) {
    return CompressionMethod::DEFLATE;
  } else if (acceptEncodingHeader.find(GZIP) != std::string::npos) {
    return CompressionMethod::GZIP;
  }
  return CompressionMethod::NONE;
}
}  // namespace detail

using boost::beast::http::field;

template <typename Body>
CompressionMethod getCompressionMethodForRequest(
    const boost::beast::http::request<Body>& request) {
  std::string_view acceptEncodingHeader =
      request.base()[field::accept_encoding];
  return detail::getCompressionMethodFromAcceptEncodingHeader(
      acceptEncodingHeader);
}

template <bool isRequest, typename Fields>
void setContentEncodingHeaderForCompressionMethod(
    CompressionMethod method,
    boost::beast::http::header<isRequest, Fields>& header) {
  if (method == CompressionMethod::DEFLATE) {
    header.set(field::content_encoding, detail::DEFLATE);
  } else if (method == CompressionMethod::GZIP) {
    header.set(field::content_encoding, detail::GZIP);
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
