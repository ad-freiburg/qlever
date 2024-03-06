// Copyright 2024, University of Freiburg,
//                 Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach <johannes.kalmbach@gmail.com>

#ifndef QLEVER_FSSTCOMPRESSOR_H
#define QLEVER_FSSTCOMPRESSOR_H

#include <fsst.h>

#include <memory>
#include <string>
#include <vector>

#include "util/Exception.h"

// A simple C++ wrapper around the C-API of the `FSST` library. It consists of
// two types, a thredsafe `FsstDecoder` that can be used to perform
// decompression, and a single-threaded `FsstEncoder` for compression.
// TODO<joka921> There are a lot of `const_cast`s that look rather fishy because
// they cast away constness. `FSST` currently has many function parameters that
// are logically const, but are not marked as const. I have opened a PR for FSST
// to make them const, as soon as this is merged, get rid of all the
// `const_cast`s and `mutable`s in this file.
class FsstDecoder {
 private:
  mutable fsst_decoder_t decoder_;

 public:
  // The default constructor does lead to an invalid decoder, but is required
  // for the serialization module. Don't use it.
  FsstDecoder() = default;

  // Construct from the internal `fsst_decoder_t`. Note that the typical way to
  // obtain an `FsstDecoder` is by first creating a `FsstEncoder` and calling
  // `getDecoder()` on that encoder.
  explicit FsstDecoder(fsst_decoder_t decoder) : decoder_{decoder} {}

  // Decompress a  single string.
  std::string decompress(std::string_view str) const {
    std::string output;
    output.resize(8 * str.size());
    size_t size = fsst_decompress(
        &decoder_, str.size(),
        reinterpret_cast<unsigned char*>(const_cast<char*>(str.data())),
        output.size(), reinterpret_cast<unsigned char*>(output.data()));
    // FSST compresses at most by a factor of 8.
    AD_CORRECTNESS_CHECK(size <= output.size());
    output.resize(size);
    return output;
  }
  // Allow this type to be trivially serializable,
  friend std::true_type allowTrivialSerialization(
      std::same_as<FsstDecoder> auto, auto);
};

// The encoder class.
class FsstEncoder {
 private:
  // The encoder state of FSST is rather complex and managed via a pointer
  // indirection. We manage this using a `unique_ptr` with a custom deleter.
  struct Deleter {
    void operator()(fsst_encoder_t* ptr) const { fsst_destroy(ptr); }
  };
  using Ptr = std::unique_ptr<fsst_encoder_t, Deleter>;
  Ptr encoder_;

 public:
  // Create an `FsstEncoder`. The given `strings` are used to create the
  // codebook.
  explicit FsstEncoder(const std::vector<std::string>& strings)
      : encoder_{makeEncoder(strings)} {}

  // Compress a single string.
  std::string compress(std::string_view word) {
    size_t len = word.size();
    std::string output;
    output.resize(7 + 2 * len);
    unsigned char* dummyOutput;
    auto data =
        reinterpret_cast<unsigned char*>(const_cast<char*>(word.data()));
    size_t outputLen = 0;
    size_t numCompressed =
        fsst_compress(encoder_.get(), 1, &len, &data, output.size(),
                      reinterpret_cast<unsigned char*>(output.data()),
                      &outputLen, &dummyOutput);
    AD_CORRECTNESS_CHECK(numCompressed == 1);
    output.resize(outputLen);
    return output;
  }

  // Return a decoder, that can be used to decompress strings that have been
  // compressed by this encoder.
  FsstDecoder makeDecoder() {
    return FsstDecoder{fsst_decoder(encoder_.get())};
  }

  // Interface for the case that all the strings that shall ever be compressed
  // using the same codebook shall also contribute to that codebook. Build a
  // codebook from the `strings`, and then use that codebook to compress each of
  // the `strings`. The result consists of a large `std::string` that contains
  // all the compressed strings concatenated, a `vector<string_view` that points
  // to the compressed strings, and a decoder that can be used to decompress the
  // strings again.
  using BulkResult =
      std::tuple<std::string, std::vector<std::string_view>, FsstDecoder>;
  static BulkResult compressAll(const std::vector<std::string>& strings) {
    return makeEncoder<true>(strings);
  }

 private:
  // The implementation of the constructor and of `compressAll`.
  template <bool alsoCompressAll = false>
  static std::conditional_t<alsoCompressAll, BulkResult, Ptr> makeEncoder(
      const std::vector<std::string>& strings) {
    std::vector<size_t> lengths;
    std::vector<unsigned char*> pointers;
    [[maybe_unused]] size_t totalSize = 0;
    for (const auto& string : strings) {
      lengths.push_back(string.size());
      totalSize += string.size();
      pointers.push_back(
          reinterpret_cast<unsigned char*>(const_cast<char*>(string.data())));
    }
    auto ptr = fsst_create(strings.size(), lengths.data(), pointers.data(), 0);
    if constexpr (!alsoCompressAll) {
      return Ptr{ptr, Deleter{}};
    } else {
      std::string output;
      // Assume that the compression acutally makes the input smaller.
      output.resize(totalSize);
      std::vector<char*> outputPtrs;
      outputPtrs.resize(strings.size());
      std::vector<size_t> outputLengths;
      outputLengths.resize(strings.size());
      while (true) {
        size_t numCompressed = fsst_compress(
            ptr, strings.size(), lengths.data(), pointers.data(), output.size(),
            reinterpret_cast<unsigned char*>(output.data()),
            outputLengths.data(),
            reinterpret_cast<unsigned char**>(outputPtrs.data()));
        // Typically one iteration should suffice, we repeat in a loop with
        // exponential growth of the output buffer.
        if (numCompressed == strings.size()) {
          break;
        }
        output.resize(2 * output.size());
      }
      // Convert the result pointers to `string_views` for easier handling.
      std::vector<std::string_view> stringViews;
      stringViews.reserve(strings.size());
      for (size_t i = 0; i < strings.size(); ++i) {
        stringViews.emplace_back(outputPtrs.at(i), outputLengths.at(i));
      }
      return std::tuple{std::move(output), std::move(stringViews),
                        FsstDecoder(fsst_decoder(ptr))};
    }
  }
};

#endif  // QLEVER_FSSTCOMPRESSOR_H
