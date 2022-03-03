// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Robin Textor-Falconi (textorr@informatik.uni-freiburg.de)

#pragma once

#include <boost/iostreams/filter/gzip.hpp>
#include <boost/iostreams/filtering_stream.hpp>
#include <string>

#include "./Generator.h"
#include "./HttpServer/ContentEncodingHelper.h"

namespace ad_utility::streams {
namespace io = boost::iostreams;
using ad_utility::content_encoding::CompressionMethod;

template <typename GeneratorType>
cppcoro::generator<std::string> compressStream(
    std::remove_reference_t<GeneratorType> generator,
    CompressionMethod compressionMethod) {
  io::filtering_ostream filteringStream;
  std::string stringBuffer;
  if (compressionMethod == CompressionMethod::DEFLATE) {
    filteringStream.push(io::zlib_compressor(io::zlib::best_speed));
  } else if (compressionMethod == CompressionMethod::GZIP) {
    filteringStream.push(io::gzip_compressor(io::gzip::best_speed));
  }
  filteringStream.push(io::back_inserter(stringBuffer), 0);

  for (const auto& value : generator) {
    filteringStream << value;
    if (!stringBuffer.empty()) {
      co_yield stringBuffer;
      stringBuffer.clear();
    }
  }
  filteringStream.reset();
  if (!stringBuffer.empty()) {
    co_yield stringBuffer;
  }
}
}  // namespace ad_utility::streams
