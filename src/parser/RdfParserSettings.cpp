// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "parser/RdfParserSettings.h"

#include "util/Exception.h"

namespace {
// The JSON keys of the members of `RdfParserSettings`. The first and the last
// one are the same as in the settings file of the index build.
constexpr std::string_view INTEGER_OVERFLOW_BEHAVIOR_KEY =
    "parser-integer-overflow-behavior";
constexpr std::string_view INVALID_LITERALS_ARE_SKIPPED_KEY =
    "invalid-literals-are-skipped";
constexpr std::string_view RELAXED_PARSING_KEY = "ascii-prefixes-only";
}  // namespace

// _____________________________________________________________________________
void to_json(nlohmann::json& j, const TurtleParserIntegerOverflowBehavior& b) {
  switch (b) {
    case TurtleParserIntegerOverflowBehavior::Error:
      j = OVERFLOWING_INTEGERS_THROW;
      return;
    case TurtleParserIntegerOverflowBehavior::OverflowingToDouble:
      j = OVERFLOWING_INTEGERS_BECOME_DOUBLES;
      return;
    case TurtleParserIntegerOverflowBehavior::AllToDouble:
      j = ALL_INTEGERS_BECOME_DOUBLES;
      return;
  }
  AD_FAIL();
}

// _____________________________________________________________________________
void from_json(const nlohmann::json& j,
               TurtleParserIntegerOverflowBehavior& b) {
  auto name = j.get<std::string>();
  if (name == OVERFLOWING_INTEGERS_THROW) {
    b = TurtleParserIntegerOverflowBehavior::Error;
  } else if (name == OVERFLOWING_INTEGERS_BECOME_DOUBLES) {
    b = TurtleParserIntegerOverflowBehavior::OverflowingToDouble;
  } else if (name == ALL_INTEGERS_BECOME_DOUBLES) {
    b = TurtleParserIntegerOverflowBehavior::AllToDouble;
  } else {
    throw std::runtime_error(absl::StrCat(
        "Invalid behavior for integers that overflow: \"", name,
        "\". The supported values are \"", OVERFLOWING_INTEGERS_THROW, "\", \"",
        OVERFLOWING_INTEGERS_BECOME_DOUBLES, "\", and \"",
        ALL_INTEGERS_BECOME_DOUBLES, "\""));
  }
}

// _____________________________________________________________________________
void to_json(nlohmann::json& j, const RdfParserSettings& settings) {
  j = nlohmann::json::object();
  j[INTEGER_OVERFLOW_BEHAVIOR_KEY] = settings.integerOverflowBehavior_;
  j[INVALID_LITERALS_ARE_SKIPPED_KEY] = settings.invalidLiteralsAreSkipped_;
  j[RELAXED_PARSING_KEY] = settings.useRelaxedParsing_;
}

// _____________________________________________________________________________
void from_json(const nlohmann::json& j, RdfParserSettings& settings) {
  settings = RdfParserSettings{};
  auto readIfPresent = [&j](std::string_view key, auto& target) {
    if (auto it = j.find(key); it != j.end()) {
      it->get_to(target);
    }
  };
  readIfPresent(INTEGER_OVERFLOW_BEHAVIOR_KEY,
                settings.integerOverflowBehavior_);
  readIfPresent(INVALID_LITERALS_ARE_SKIPPED_KEY,
                settings.invalidLiteralsAreSkipped_);
  readIfPresent(RELAXED_PARSING_KEY, settings.useRelaxedParsing_);
}
