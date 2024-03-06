//
// Created by kalmbacj on 3/5/24.
//

#ifndef QLEVER_FSSTCOMPRESSOR_H
#define QLEVER_FSSTCOMPRESSOR_H

#include <fsst.h>

#include <memory>
#include <string>
#include <vector>

#include "util/Exception.h"

class FsstDecoder {
 private:
  // TODO<joka921> We really have to start this PR to FSST.
  mutable fsst_decoder_t decoder_;

 public:
  // TODO<joka921> This constructor is a bit unsafe, but we need it for
  // serialization. Should we do something about this?
  FsstDecoder() = default;
  FsstDecoder(fsst_decoder_t decoder) : decoder_{decoder} {}
  std::string decompress(const std::string& str) const {
    std::string output;
    output.resize(8 * str.size());
    size_t size = fsst_decompress(
        &decoder_, str.size(),
        reinterpret_cast<unsigned char*>(const_cast<char*>(str.data())),
        output.size(), reinterpret_cast<unsigned char*>(output.data()));
    // TODO<joka921> implement the fallback for the correct size.
    AD_CORRECTNESS_CHECK(size <= output.size());
    output.resize(size);
    return output;
  }
  // Allow this type to be trivially serializable,
  friend std::true_type allowTrivialSerialization(
      std::same_as<FsstDecoder> auto, auto);
};

class FsstEncoder {
 private:
  struct Deleter {
    void operator()(fsst_encoder_t* ptr) const { fsst_destroy(ptr); }
  };
  using Ptr = std::unique_ptr<fsst_encoder_t, Deleter>;
  Ptr encoder_;

 public:
  explicit FsstEncoder(const std::vector<std::string>& strings)
      : encoder_{makeEncoder(strings)} {}
  std::string compress(std::string& word) {
    size_t len = word.size();
    std::string output;
    output.resize(7 + 2 * len);
    unsigned char* dummyOutput;
    auto data = reinterpret_cast<unsigned char*>(word.data());
    size_t outputLen = 0;
    size_t numCompressed =
        fsst_compress(encoder_.get(), 1, &len, &data, output.size(),
                      reinterpret_cast<unsigned char*>(output.data()),
                      &outputLen, &dummyOutput);
    AD_CORRECTNESS_CHECK(numCompressed == 1);
    output.resize(outputLen);
    return output;
  }

  FsstDecoder makeDecoder() {
    return FsstDecoder{fsst_decoder(encoder_.get())};
  }

 private:
  static Ptr makeEncoder(const std::vector<std::string>& strings) {
    std::vector<size_t> lengths;
    std::vector<unsigned char*> pointers;
    for (const auto& string : strings) {
      lengths.push_back(string.size());
      // TODO<joka921> is this bad casting safe, and also can we fix fsst to
      // only take const pointers?
      pointers.push_back(
          reinterpret_cast<unsigned char*>(const_cast<char*>(string.data())));
    }
    auto ptr = fsst_create(strings.size(), lengths.data(), pointers.data(), 0);
    // TODO<joka921> Use absl::wrap_unique
    return {ptr, Deleter{}};
  }
};

#endif  // QLEVER_FSSTCOMPRESSOR_H
