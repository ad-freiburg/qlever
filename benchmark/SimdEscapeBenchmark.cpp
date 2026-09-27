// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <absl/strings/str_replace.h>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <numeric>
#include <random>
#include <string>
#include <string_view>
#include <vector>

#include "../benchmark/infrastructure/Benchmark.h"
#include "engine/SimdEscapeClassifier.h"
#include "util/Exception.h"

namespace ad_benchmark {
namespace {

using namespace ad_utility::simd;

// Realistic synthetic literal generator targeting ~100 MB of dataset.
struct LiteralDataset {
  std::vector<std::string> literals;
  size_t totalBytes = 0;
};

LiteralDataset generateRealisticLiteralDataset(size_t targetBytes = 100 * 1024 *
                                                                    1024) {
  LiteralDataset dataset;
  constexpr std::string_view alphaNum =
      "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 _-./:";
  constexpr std::string_view utf8Words[] = {
      "München", "Düsseldorf", "Zürich", "São Paulo", "København", "Göteborg",
      "Kyïv",    "Łódź",       "東京",   "北京",      "العربية",   "Ελληνικά"};

  std::mt19937_64 rng(42);
  std::uniform_int_distribution<size_t> lenDist(10, 250);
  std::uniform_int_distribution<size_t> charDist(0, alphaNum.size() - 1);
  std::uniform_int_distribution<size_t> utf8Dist(0, 11);
  std::uniform_int_distribution<size_t> escapeProbDist(0, 99);

  size_t currentBytes = 0;
  while (currentBytes < targetBytes) {
    size_t length = lenDist(rng);
    std::string literal;
    literal.reserve(length + 32);

    // Literal starting quote
    literal.push_back('"');

    // 10% chance of UTF-8 content prefix
    if (escapeProbDist(rng) < 10) {
      literal.append(utf8Words[utf8Dist(rng)]);
      literal.push_back(' ');
    }

    // 8% chance this literal contains special escape characters
    bool injectEscapes = (escapeProbDist(rng) < 8);

    for (size_t i = 0; i < length; ++i) {
      if (injectEscapes && i % 25 == 12) {
        // Inject an escape character. Covers Turtle specials as well as
        // the CSV (`,`) and TSV (`\t`) format-specific characters so all
        // scan arms exercise their own hit paths.
        switch (i % 6) {
          case 0:
            literal.push_back('"');
            break;
          case 1:
            literal.push_back('\\');
            break;
          case 2:
            literal.push_back('\n');
            break;
          case 3:
            literal.push_back('\r');
            break;
          case 4:
            literal.push_back(',');
            break;
          default:
            literal.push_back('\t');
            break;
        }
      } else {
        literal.push_back(alphaNum[charDist(rng)]);
      }
    }

    // Literal closing quote
    literal.push_back('"');

    // 25% chance of language tag
    if (escapeProbDist(rng) < 25) {
      literal.append("@en");
    }

    currentBytes += literal.size();
    dataset.literals.push_back(std::move(literal));
  }

  dataset.totalBytes = currentBytes;
  return dataset;
}

// Baseline scalar character-by-character scanner for Turtle.
[[nodiscard]] size_t scalarFindFirstEscapeTurtle(
    std::string_view text) noexcept {
  for (size_t i = 0; i < text.size(); ++i) {
    char c = text[i];
    if (c == '"' || c == '\\' || c == '\n' || c == '\r') {
      return i;
    }
  }
  return std::string_view::npos;
}

// Baseline scalar character-by-character scanner for CSV Special.
[[nodiscard]] size_t scalarFindFirstEscapeCsv(std::string_view text) noexcept {
  for (size_t i = 0; i < text.size(); ++i) {
    char c = text[i];
    if (c == '"' || c == ',' || c == '\r' || c == '\n') {
      return i;
    }
  }
  return std::string_view::npos;
}

// Baseline scalar character-by-character scanner for TSV.
[[nodiscard]] size_t scalarFindFirstEscapeTsv(std::string_view text) noexcept {
  for (size_t i = 0; i < text.size(); ++i) {
    char c = text[i];
    if (c == '\t' || c == '\n' || c == '\r' || c == '\\') {
      return i;
    }
  }
  return std::string_view::npos;
}

// Baseline scalar Turtle literal escaping using absl::StrReplaceAll.
std::string scalarEscapeTurtleLiteral(std::string_view normLiteral) {
  if (normLiteral.size() < 2 || normLiteral.front() != '"') {
    return std::string{normLiteral};
  }
  size_t posSecondQuote = normLiteral.find('"', 1);
  AD_CONTRACT_CHECK(posSecondQuote != std::string_view::npos);
  size_t posLastQuote = normLiteral.rfind('"');
  if (posSecondQuote == posLastQuote &&
      normLiteral.find_first_of("\\\n\r") == std::string_view::npos) {
    return std::string{normLiteral};
  }
  std::string_view content = normLiteral.substr(1, posLastQuote - 1);
  std::string escaped = absl::StrReplaceAll(
      content,
      {{R"(\)", R"(\\)"}, {"\n", "\\n"}, {"\r", "\\r"}, {R"(")", R"(\")"}});
  std::string result;
  result.reserve(escaped.size() + 2 + (normLiteral.size() - posLastQuote));
  result.push_back('"');
  result.append(escaped);
  result.push_back('"');
  result.append(normLiteral.substr(posLastQuote + 1));
  return result;
}

class SimdEscapeBenchmark : public BenchmarkInterface {
 private:
  LiteralDataset dataset_;

 public:
  SimdEscapeBenchmark() {
    std::cout << "Generating ~100 MB of realistic RDF literal strings..."
              << std::endl;
    dataset_ = generateRealisticLiteralDataset(100 * 1024 * 1024);
    std::cout << "Generated " << dataset_.literals.size() << " literals ("
              << (static_cast<double>(dataset_.totalBytes) / (1024.0 * 1024.0))
              << " MB total)." << std::endl;
  }

  std::string name() const final {
    return "SIMD Literal Escape Classification & Formatting (100 MB)";
  }

  BenchmarkResults runAllBenchmarks() final {
    BenchmarkResults results;
    const double totalMB =
        static_cast<double>(dataset_.totalBytes) / (1024.0 * 1024.0);
    const size_t numLiterals = dataset_.literals.size();

    // =========================================================================
    // Group 1: Scanning & Classification Throughput
    // =========================================================================
    {
      auto& group =
          results.addGroup("1. Literal Escape Scanning Throughput (100 MB)");

      // Baseline: Scalar Turtle Scan
      {
        size_t totalEscapesFound = 0;
        auto start = std::chrono::high_resolution_clock::now();
        auto& m = group.addMeasurement(
            "Scalar character-by-character scan (Turtle)", [&]() {
              size_t count = 0;
              for (const auto& lit : dataset_.literals) {
                if (scalarFindFirstEscapeTurtle(lit) !=
                    std::string_view::npos) {
                  ++count;
                }
              }
              totalEscapesFound = count;
              return dataset_.totalBytes;
            });
        auto end = std::chrono::high_resolution_clock::now();
        double seconds = std::chrono::duration<double>(end - start).count();
        double throughputGBs =
            (static_cast<double>(dataset_.totalBytes) / 1e9) / seconds;

        m.metadata().addKeyValuePair("total-bytes-mb", totalMB);
        m.metadata().addKeyValuePair("total-literals", numLiterals);
        m.metadata().addKeyValuePair("throughput-gb-s", throughputGBs);
        m.metadata().addKeyValuePair("escapes-found", totalEscapesFound);
      }

      // SIMD Vector Scan (Turtle)
      {
        size_t totalEscapesFound = 0;
        auto start = std::chrono::high_resolution_clock::now();
        auto& m = group.addMeasurement(
            "SimdEscapeClassifier::findFirstEscape (Turtle AVX2)", [&]() {
              size_t count = 0;
              for (const auto& lit : dataset_.literals) {
                if (SimdEscapeClassifier::hasEscapes<EscapeFormat::Turtle>(
                        lit)) {
                  ++count;
                }
              }
              totalEscapesFound = count;
              return dataset_.totalBytes;
            });
        auto end = std::chrono::high_resolution_clock::now();
        double seconds = std::chrono::duration<double>(end - start).count();
        double throughputGBs =
            (static_cast<double>(dataset_.totalBytes) / 1e9) / seconds;

        m.metadata().addKeyValuePair("total-bytes-mb", totalMB);
        m.metadata().addKeyValuePair("total-literals", numLiterals);
        m.metadata().addKeyValuePair("throughput-gb-s", throughputGBs);
        m.metadata().addKeyValuePair("escapes-found", totalEscapesFound);
      }

      // Baseline: Scalar CSV Scan
      {
        size_t totalEscapesFound = 0;
        auto start = std::chrono::high_resolution_clock::now();
        auto& m = group.addMeasurement(
            "Scalar character-by-character scan (CSV)", [&]() {
              size_t count = 0;
              for (const auto& lit : dataset_.literals) {
                if (scalarFindFirstEscapeCsv(lit) != std::string_view::npos) {
                  ++count;
                }
              }
              totalEscapesFound = count;
              return dataset_.totalBytes;
            });
        auto end = std::chrono::high_resolution_clock::now();
        double seconds = std::chrono::duration<double>(end - start).count();
        double throughputGBs =
            (static_cast<double>(dataset_.totalBytes) / 1e9) / seconds;

        m.metadata().addKeyValuePair("total-bytes-mb", totalMB);
        m.metadata().addKeyValuePair("throughput-gb-s", throughputGBs);
        m.metadata().addKeyValuePair("escapes-found", totalEscapesFound);
      }

      // SIMD Vector Scan (CSV)
      {
        size_t totalEscapesFound = 0;
        auto start = std::chrono::high_resolution_clock::now();
        auto& m = group.addMeasurement(
            "SimdEscapeClassifier::findFirstEscape (CSV AVX2)", [&]() {
              size_t count = 0;
              for (const auto& lit : dataset_.literals) {
                if (SimdEscapeClassifier::hasEscapes<EscapeFormat::CsvSpecial>(
                        lit)) {
                  ++count;
                }
              }
              totalEscapesFound = count;
              return dataset_.totalBytes;
            });
        auto end = std::chrono::high_resolution_clock::now();
        double seconds = std::chrono::duration<double>(end - start).count();
        double throughputGBs =
            (static_cast<double>(dataset_.totalBytes) / 1e9) / seconds;

        m.metadata().addKeyValuePair("total-bytes-mb", totalMB);
        m.metadata().addKeyValuePair("throughput-gb-s", throughputGBs);
        m.metadata().addKeyValuePair("escapes-found", totalEscapesFound);
      }

      // Baseline: Scalar TSV Scan
      {
        size_t totalEscapesFound = 0;
        auto start = std::chrono::high_resolution_clock::now();
        auto& m = group.addMeasurement(
            "Scalar character-by-character scan (TSV)", [&]() {
              size_t count = 0;
              for (const auto& lit : dataset_.literals) {
                if (scalarFindFirstEscapeTsv(lit) != std::string_view::npos) {
                  ++count;
                }
              }
              totalEscapesFound = count;
              return dataset_.totalBytes;
            });
        auto end = std::chrono::high_resolution_clock::now();
        double seconds = std::chrono::duration<double>(end - start).count();
        double throughputGBs =
            (static_cast<double>(dataset_.totalBytes) / 1e9) / seconds;

        m.metadata().addKeyValuePair("total-bytes-mb", totalMB);
        m.metadata().addKeyValuePair("throughput-gb-s", throughputGBs);
        m.metadata().addKeyValuePair("escapes-found", totalEscapesFound);
      }

      // SIMD Vector Scan (TSV)
      {
        size_t totalEscapesFound = 0;
        auto start = std::chrono::high_resolution_clock::now();
        auto& m = group.addMeasurement(
            "SimdEscapeClassifier::findFirstEscape (TSV AVX2)", [&]() {
              size_t count = 0;
              for (const auto& lit : dataset_.literals) {
                if (SimdEscapeClassifier::hasEscapes<EscapeFormat::Tsv>(lit)) {
                  ++count;
                }
              }
              totalEscapesFound = count;
              return dataset_.totalBytes;
            });
        auto end = std::chrono::high_resolution_clock::now();
        double seconds = std::chrono::duration<double>(end - start).count();
        double throughputGBs =
            (static_cast<double>(dataset_.totalBytes) / 1e9) / seconds;

        m.metadata().addKeyValuePair("total-bytes-mb", totalMB);
        m.metadata().addKeyValuePair("throughput-gb-s", throughputGBs);
        m.metadata().addKeyValuePair("escapes-found", totalEscapesFound);
      }
    }

    // =========================================================================
    // Group 2: End-to-End Literal Formatting & Escaping Throughput
    // =========================================================================
    {
      auto& group = results.addGroup(
          "2. End-to-End Escaping & Formatting Throughput (100 MB)");

      // Baseline: Scalar StrReplaceAll Escaping (Turtle)
      {
        size_t totalOutputBytes = 0;
        auto start = std::chrono::high_resolution_clock::now();
        auto& m = group.addMeasurement(
            "Baseline scalar StrReplaceAll (Turtle)", [&]() {
              size_t outBytes = 0;
              for (const auto& lit : dataset_.literals) {
                std::string formatted = scalarEscapeTurtleLiteral(lit);
                outBytes += formatted.size();
              }
              totalOutputBytes = outBytes;
              return totalOutputBytes;
            });
        auto end = std::chrono::high_resolution_clock::now();
        double seconds = std::chrono::duration<double>(end - start).count();
        double throughputGBs =
            (static_cast<double>(dataset_.totalBytes) / 1e9) / seconds;

        m.metadata().addKeyValuePair("total-input-mb", totalMB);
        m.metadata().addKeyValuePair(
            "output-bytes-mb",
            static_cast<double>(totalOutputBytes) / (1024.0 * 1024.0));
        m.metadata().addKeyValuePair("throughput-gb-s", throughputGBs);
      }

      // SIMD Fast-Path Branchless Copy & Escape (Turtle)
      {
        size_t totalOutputBytes = 0;
        auto start = std::chrono::high_resolution_clock::now();
        auto& m = group.addMeasurement(
            "SimdEscapeClassifier::validRDFLiteralFromNormalized (Turtle)",
            [&]() {
              size_t outBytes = 0;
              for (const auto& lit : dataset_.literals) {
                std::string formatted =
                    SimdEscapeClassifier::validRDFLiteralFromNormalized(lit);
                outBytes += formatted.size();
              }
              totalOutputBytes = outBytes;
              return totalOutputBytes;
            });
        auto end = std::chrono::high_resolution_clock::now();
        double seconds = std::chrono::duration<double>(end - start).count();
        double throughputGBs =
            (static_cast<double>(dataset_.totalBytes) / 1e9) / seconds;

        m.metadata().addKeyValuePair("total-input-mb", totalMB);
        m.metadata().addKeyValuePair(
            "output-bytes-mb",
            static_cast<double>(totalOutputBytes) / (1024.0 * 1024.0));
        m.metadata().addKeyValuePair("throughput-gb-s", throughputGBs);
      }

      // SIMD Fast-Path Direct Buffer Copying (Turtle zero temporary string
      // allocations)
      {
        // Turtle escaping expands each byte to at most two; size the
        // buffer from the input with margin instead of a magic constant.
        std::vector<char> outputBuffer(dataset_.totalBytes * 2 + 1024);
        size_t totalOutputBytes = 0;
        auto start = std::chrono::high_resolution_clock::now();
        auto& m = group.addMeasurement(
            "SimdEscapeClassifier::copyAndEscape (Direct Buffer Streaming)",
            [&]() {
              char* outPtr = outputBuffer.data();
              for (const auto& lit : dataset_.literals) {
                outPtr =
                    SimdEscapeClassifier::copyAndEscape<EscapeFormat::Turtle>(
                        lit, outPtr);
              }
              totalOutputBytes =
                  static_cast<size_t>(outPtr - outputBuffer.data());
              AD_CONTRACT_CHECK(totalOutputBytes <= outputBuffer.size());
              return totalOutputBytes;
            });
        auto end = std::chrono::high_resolution_clock::now();
        double seconds = std::chrono::duration<double>(end - start).count();
        double throughputGBs =
            (static_cast<double>(dataset_.totalBytes) / 1e9) / seconds;

        m.metadata().addKeyValuePair("total-input-mb", totalMB);
        m.metadata().addKeyValuePair(
            "output-bytes-mb",
            static_cast<double>(totalOutputBytes) / (1024.0 * 1024.0));
        m.metadata().addKeyValuePair("throughput-gb-s", throughputGBs);
      }
    }

    return results;
  }
};

AD_REGISTER_BENCHMARK(SimdEscapeBenchmark);

}  // namespace
}  // namespace ad_benchmark
