// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Robin Textor-Falconi (textorr@informatik.uni-freiburg.de)

#pragma once

#include "./beast.h"

namespace ad_utility::content_encoding {

enum class CompressionMethod { NONE, DEFLATE };

namespace detail {

constexpr CompressionMethod getCompressionMethodAcceptEncodingHeader(
    const std::string_view acceptEncodingHeader) {
  // TODO evaluate if gzip can be supported using boost
  if (acceptEncodingHeader.find("deflate") != std::string::npos) {
    return CompressionMethod::DEFLATE;
  }
  return CompressionMethod::NONE;
}
}  // namespace detail

template <typename Body>
inline CompressionMethod getCompressionMethodForRequest(
    const boost::beast::http::request<Body>& request) {
  std::string_view acceptEncodingHeader =
      request.base()[boost::beast::http::field::accept_encoding];
  return detail::getCompressionMethodAcceptEncodingHeader(acceptEncodingHeader);
}

}  // namespace ad_utility::content_encoding
