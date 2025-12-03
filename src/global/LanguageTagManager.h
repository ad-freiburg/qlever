// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_SRC_GLOBAL_LANGUAGETAGMANAGER_H
#define QLEVER_SRC_GLOBAL_LANGUAGETAGMANAGER_H

#include <algorithm>
#include <optional>
#include <string>
#include <vector>

#include "util/Exception.h"
#include "util/json.h"

/// Manages the mapping between language tags (as strings) and their compact
/// integer representations for storage in ValueIds.
class LanguageTagManager {
 public:
  // Constants for special language tag values
  static constexpr uint32_t numLangTagBits = 20;
  static constexpr uint32_t maxLangTagIndex = (1u << numLangTagBits) - 1;
  static constexpr uint32_t noLanguageTag = maxLangTagIndex;  // 2^20 - 1
  static constexpr uint32_t unknownLanguageTag =
      maxLangTagIndex - 1;  // 2^20 - 2

  LanguageTagManager() {
    // Initialize with default common languages
    optimizedLanguages_ = {"",   "mul", "en", "fi", "fr", "ja",
                           "cs", "ru",  "es", "sv", "pt", "uk",
                           "da", "el",  "de", "it", "pl", "no"};
  }

  /// Get the index for a language tag string. Returns noLanguageTag if empty,
  /// unknownLanguageTag if not in the optimized list, or the index in the list.
  uint32_t getLanguageTagIndex(std::string_view languageTag) const {
    if (languageTag.empty()) {
      return noLanguageTag;
    }

    auto it = std::find(optimizedLanguages_.begin(), optimizedLanguages_.end(),
                        languageTag);
    if (it != optimizedLanguages_.end()) {
      return static_cast<uint32_t>(
          std::distance(optimizedLanguages_.begin(), it));
    }

    return unknownLanguageTag;
  }

  /// Get the language tag string for a given index. Returns empty string for
  /// noLanguageTag, throws for unknownLanguageTag, or returns the language
  /// string.
  std::string_view getLanguageTag(uint32_t index) const {
    if (index == noLanguageTag) {
      return "";
    }
    if (index == unknownLanguageTag) {
      AD_THROW("Cannot retrieve language tag for unknown language index");
    }
    AD_CONTRACT_CHECK(index < optimizedLanguages_.size());
    return optimizedLanguages_[index];
  }

  /// Get the list of optimized languages
  const std::vector<std::string>& getOptimizedLanguages() const {
    return optimizedLanguages_;
  }

  /// Set the list of optimized languages (for configuration during index
  /// building)
  void setOptimizedLanguages(std::vector<std::string> languages) {
    optimizedLanguages_ = std::move(languages);
  }

  /// Add a language to the optimized list if not already present
  void addOptimizedLanguage(std::string language) {
    if (std::find(optimizedLanguages_.begin(), optimizedLanguages_.end(),
                  language) == optimizedLanguages_.end()) {
      optimizedLanguages_.push_back(std::move(language));
    }
  }

  /// Get the number of optimized languages
  size_t numOptimizedLanguages() const { return optimizedLanguages_.size(); }

  /// Set the vocabulary ID (as raw bits) for a language tag index
  /// This should be called after the index is loaded from disk
  void setLanguageTagIdBits(uint32_t languageTagIndex, uint64_t vocabIdBits) {
    if (languageTagToVocabIdBits_.size() <= languageTagIndex) {
      languageTagToVocabIdBits_.resize(languageTagIndex + 1);
    }
    languageTagToVocabIdBits_[languageTagIndex] = vocabIdBits;
  }

  /// Get the vocabulary ID (as raw bits) for a language tag index
  /// Returns std::nullopt if the mapping hasn't been set yet
  std::optional<uint64_t> getLanguageTagIdBits(
      uint32_t languageTagIndex) const {
    if (languageTagIndex < languageTagToVocabIdBits_.size()) {
      return languageTagToVocabIdBits_[languageTagIndex];
    }
    return std::nullopt;
  }

  /// Clear all ID mappings (useful when reloading an index)
  void clearLanguageTagIds() { languageTagToVocabIdBits_.clear(); }

  // JSON serialization support
  friend void to_json(nlohmann::json& j, const LanguageTagManager& manager) {
    j = manager.optimizedLanguages_;
  }

  friend void from_json(const nlohmann::json& j, LanguageTagManager& manager) {
    manager.optimizedLanguages_ = j.get<std::vector<std::string>>();
  }

 private:
  std::vector<std::string> optimizedLanguages_;
  // Mapping from language tag index to the vocabulary ID (as raw bits) of the
  // language string (e.g., index 0 for "en" maps to the ID of the literal "en"
  // in the vocab)
  std::vector<std::optional<uint64_t>> languageTagToVocabIdBits_;
};

#endif  // QLEVER_SRC_GLOBAL_LANGUAGETAGMANAGER_H
