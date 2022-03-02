// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Robin Textor-Falconi (textorr@informatik.uni-freiburg.de)

#pragma once

#include <boost/iostreams/filter/gzip.hpp>
#include <boost/iostreams/filtering_stream.hpp>
#include <memory>
#include <string_view>

#include "./HttpServer/ContentEncodingHelper.h"
#include "./StringSupplier.h"

namespace ad_utility::streams {
namespace io = boost::iostreams;
using ad_utility::content_encoding::CompressionMethod;
namespace http = boost::beast::http;

class CompressorStream : public StringSupplier {
  std::unique_ptr<StringSupplier> _supplier;
  io::filtering_ostream _filteringStream;
  std::string _value;
  CompressionMethod _compressionMethod;

 public:
  CompressorStream(std::unique_ptr<StringSupplier> supplier,
                   CompressionMethod compressionMethod)
      : _supplier{std::move(supplier)}, _compressionMethod{compressionMethod} {
    if (compressionMethod == CompressionMethod::DEFLATE) {
      _filteringStream.push(io::zlib_compressor(io::zlib::best_speed));
    } else if (compressionMethod == CompressionMethod::GZIP) {
      _filteringStream.push(io::gzip_compressor(io::gzip::best_speed));
    }
    _filteringStream.push(io::back_inserter(_value), 0);
  }

  [[nodiscard]] bool hasNext() const override { return _supplier->hasNext(); }

  std::string_view next() override {
    _value.clear();
    while (_value.empty() && _supplier->hasNext()) {
      _filteringStream << _supplier->next();
    }
    if (!_supplier->hasNext()) {
      _filteringStream.reset();
    }
    return _value;
  }
};
}  // namespace ad_utility::streams
