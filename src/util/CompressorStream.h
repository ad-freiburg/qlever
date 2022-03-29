// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Robin Textor-Falconi (textorr@informatik.uni-freiburg.de)

#pragma once

// For some include orders the EOF constant is not defined although `<cstdio>`
// was included, so we define it manually.
// TODO<joka921> Find out where this happens.
#ifndef EOF
#define EOF std::char_traits<char>::eof()
#endif
#include <boost/iostreams/filter/gzip.hpp>
#include <boost/iostreams/filtering_stream.hpp>
#include <string>

#include "./Generator.h"
#include "./HttpServer/ContentEncodingHelper.h"

namespace ad_utility::streams {
namespace io = boost::iostreams;
using ad_utility::content_encoding::CompressionMethod;

/**
 * Takes a range of strings. Behavior: The concatenation of all yielded strings
 * is the compression, specified by the `compressionMethod` applied to the
 * concatenation of all the strings from the range.
 */
template <typename Range>
cppcoro::generator<std::string> compressStream(
    Range range, CompressionMethod compressionMethod) {
  io::filtering_ostream filteringStream;
  std::string stringBuffer;

  // setup compression method
  if (compressionMethod == CompressionMethod::DEFLATE) {
    filteringStream.push(io::zlib_compressor(io::zlib::best_speed));
  } else if (compressionMethod == CompressionMethod::GZIP) {
    filteringStream.push(io::gzip_compressor(io::gzip::best_speed));
  }
  filteringStream.push(io::back_inserter(stringBuffer), 0);

  for (const auto& value : range) {
    filteringStream << value;
    if (!stringBuffer.empty()) {
      co_yield stringBuffer;
      stringBuffer.clear();
    }
  }
  // reset() flushes the stream and puts the remaining bytes into stringBuffer.
  filteringStream.reset();
  if (!stringBuffer.empty()) {
    co_yield stringBuffer;
  }
}
}  // namespace ad_utility::streams
