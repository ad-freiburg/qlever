//  Copyright 2024, 2026, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>
//  Author: Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR

#ifndef QLEVER_SRC_INDEX_VOCABULARY_COMPRESSIONWRAPPERS_H
#define QLEVER_SRC_INDEX_VOCABULARY_COMPRESSIONWRAPPERS_H

#include "backports/algorithm.h"
#include "backports/concepts.h"
#include "backports/span.h"
#include "index/vocabulary/PrefixCompressor.h"
#include "index/vocabulary/PrefixHeuristic.h"
#include "util/CompilerWarnings.h"
#include "util/FsstCompressor.h"

namespace ad_utility::vocabulary {

// A helper concept for the compression wrappers below.
// A `BulkResultForDecoder` is a tuple of 3 elements, the first of which is an
// implementation detail (e.g. a buffer that stores the data for the second
// argument ), the second of which is a `vector<string_view>` or
// `vector<string>` that stores compressed strings and the third of which is a
// `Decoder`.
template <typename T, typename Decoder>
CPP_requires(
    BulkResultForDecoder_,
    requires(T t)(std::tuple_size_v<T> == 3,
                  ad_utility::SimilarToAny<decltype(std::get<1>(t)),
                                           std::vector<std::string_view>,
                                           std::vector<std::string>>,
                  ad_utility::SimilarTo<decltype(std::get<2>(t)), Decoder>));

template <typename T, typename Decoder>
CPP_concept BulkResultForDecoder =
    CPP_requires_ref(BulkResultForDecoder_, T, Decoder);

template <typename T>
CPP_requires(
    CompressionWrapper_,
    requires(const T& t, std::string& scratch, ql::span<char> out)(
        // Return the number of decoders that are stored.
        concepts::same_as<decltype(t.numDecoders()), size_t>,
        // Decompress the given string, use the Decoder specified by the second
        // argument.
        concepts::same_as<decltype(t.decompress(std::string_view{}, size_t{0})),
                          std::string>,
        concepts::same_as<decltype(t.maxDecompressedSize(std::string_view{},
                                                         size_t{0})),
                          size_t>,
        concepts::same_as<decltype(t.decompressInto(std::string_view{},
                                                    size_t{0}, out, scratch)),
                          size_t>,
        // Compress all the strings and return the strings together with a
        // `Decoder` that can be used to decompress the strings again.
        BulkResultForDecoder<
            decltype(T::compressAll(std::vector<std::string>{})),
            typename T::Decoder>,
        concepts::constructible_from<T, std::vector<typename T::Decoder>>));

// A concept for the `CompressionWrappers` that can be passed as template
// arguments to the compressed vocabulary to specify the compression algorithm.
template <typename T>
CPP_concept CompressionWrapper = CPP_requires_ref(CompressionWrapper_, T);

namespace detail {

template <typename Decoder, typename = void>
struct HasThreeArgumentDecompressInto : std::false_type {};

template <typename Decoder>
struct HasThreeArgumentDecompressInto<
    Decoder, std::void_t<decltype(std::declval<const Decoder&>().decompressInto(
        std::declval<std::string_view>(), std::declval<ql::span<char>>(),
        std::declval<std::string&>()))>> : std::true_type {};
// A class that holds a `vector<DecoderT>` and forwards `decompress`,
// `maxDecompressedSize`, and `decompressInto` to `decoders_[index]`. It is
// used as a building block for types that fulfill the `CompressionWrapper`
// concept above.
template <typename DecoderT>
struct DecoderMultiplexer {
  using Decoder = DecoderT;
  using Decoders = std::vector<Decoder>;
  using Strings = std::vector<std::string>;

 private:
  std::vector<Decoder> decoders_;

 public:
  DecoderMultiplexer() = default;
  explicit DecoderMultiplexer(Decoders decoders)
      : decoders_{std::move(decoders)} {}
  std::string decompress(std::string_view compressed,
                         size_t decoderIndex) const {
    DISABLE_CLANG_UNUSED_RESULT_WARNING
    return decoders_.at(decoderIndex).decompress(compressed);
    ENABLE_CLANG_WARNINGS
  }

  // Return the maximum number of output bytes needed to decompress
  // `compressed` with `decoderIndex`.
  [[nodiscard]] size_t maxDecompressedSize(std::string_view compressed,
                                           size_t decoderIndex) const {
    return decoders_.at(decoderIndex).maxDecompressedSize(compressed);
  }

  // Decode `compressed` with `decoderIndex` into `out`. `scratch` is used
  // only by multi-stage FSST; single-stage decoders ignore it.
  [[nodiscard]] size_t decompressInto(std::string_view compressed,
                                      size_t decoderIndex, ql::span<char> out,
                                      std::string& scratch) const {
    DISABLE_CLANG_UNUSED_RESULT_WARNING
    auto& decoder = decoders_.at(decoderIndex);
    if constexpr (requires {
                    decoder.decompressInto(compressed, out, scratch);
                  }) {
      return decoder.decompressInto(compressed, out, scratch);
    } else {
      (void)scratch;
      return decoder.decompressInto(compressed, out);
    }
    ENABLE_CLANG_WARNINGS
  }

  size_t numDecoders() const { return decoders_.size(); }
  const Decoders& getDecoders() const { return decoders_; }
};
}  // namespace detail

// A compression wrapper that applies the FSST compression algorithm.
struct FsstCompressionWrapper : detail::DecoderMultiplexer<FsstDecoder> {
  using Base = detail::DecoderMultiplexer<FsstDecoder>;
  using Base::Base;
  static FsstEncoder::BulkResult compressAll(const Strings& strings) {
    return FsstEncoder::compressAll(strings);
  }
};
static_assert(CompressionWrapper<FsstCompressionWrapper>);

// A compression wrapper that applies the FSST compression algorithm twice.
struct FsstSquaredCompressionWrapper
    : detail::DecoderMultiplexer<FsstRepeatedDecoder<2>> {
  using Base = detail::DecoderMultiplexer<FsstRepeatedDecoder<2>>;
  using Base::Base;
  using BulkResult =
      std::tuple<std::shared_ptr<std::string>, std::vector<std::string_view>,
                 FsstRepeatedDecoder<2>>;
  static BulkResult compressAll(const Strings& strings) {
    auto [buffer, views, decoder1] = FsstEncoder::compressAll(strings);
    auto [buffer2, views2, decoder2] = FsstEncoder::compressAll(views);
    return {std::move(buffer2), std::move(views2),
            FsstRepeatedDecoder{std::array{decoder1, decoder2}}};
  }
};
static_assert(CompressionWrapper<FsstSquaredCompressionWrapper>);

// A compression wrapper that compresses common prefixes using the greedy
// algorithm from `PrefixHeuristics.h`.
struct PrefixCompressionWrapper : detail::DecoderMultiplexer<PrefixCompressor> {
  using Base = detail::DecoderMultiplexer<PrefixCompressor>;
  using Base::Base;
  using BulkResult = std::tuple<bool, std::vector<std::string>, Decoder>;

  static BulkResult compressAll(const Strings& strings) {
    PrefixCompressor compressor;
    auto stringsCopy = strings;
    ql::ranges::sort(stringsCopy);
    auto prefixes = calculatePrefixes(stringsCopy, NUM_COMPRESSION_PREFIXES);
    compressor.buildCodebook(prefixes);
    Strings compressedStrings;
    for (const auto& string : strings) {
      compressedStrings.push_back(compressor.compress(string));
    }
    return {true, std::move(compressedStrings), std::move(compressor)};
  }
};
static_assert(CompressionWrapper<PrefixCompressionWrapper>);
}  // namespace ad_utility::vocabulary

#endif  // QLEVER_SRC_INDEX_VOCABULARY_COMPRESSIONWRAPPERS_H
