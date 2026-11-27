// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_PARSER_RDFPARSERSETTINGS_H
#define QLEVER_SRC_PARSER_RDFPARSERSETTINGS_H

#include <absl/strings/str_cat.h>

#include <stdexcept>
#include <string>
#include <string_view>

#include "backports/three_way_comparison.h"
#include "util/json.h"

enum class TurtleParserIntegerOverflowBehavior {
  Error,
  OverflowingToDouble,
  AllToDouble
};

// The names of the `TurtleParserIntegerOverflowBehavior`s, as they are used in
// the settings file of the index build and in the configuration of an index.
inline constexpr std::string_view OVERFLOWING_INTEGERS_THROW =
    "overflowing-integers-throw";
inline constexpr std::string_view OVERFLOWING_INTEGERS_BECOME_DOUBLES =
    "overflowing-integers-become-doubles";
inline constexpr std::string_view ALL_INTEGERS_BECOME_DOUBLES =
    "all-integers-become-doubles";

// Convert a `TurtleParserIntegerOverflowBehavior` to and from JSON (as one of
// the names above). Throw if the name is unknown.
void to_json(nlohmann::json& j, const TurtleParserIntegerOverflowBehavior& b);
void from_json(const nlohmann::json& j, TurtleParserIntegerOverflowBehavior& b);

// The key under which the `RdfParserSettings` of an index build are stored in
// the configuration of the index.
inline constexpr std::string_view RDF_PARSER_SETTINGS_KEY =
    "rdf-parser-settings";

// The settings that control how an RDF parser deals with the input. They are
// the same for all the parsers of an index build, so the parsers that handle
// several files (`RdfMultifileParser` and `RdfAsyncMultifileParser`) take them
// in their constructor and pass them on to every parser that they create for a
// single file or for a block of a file. They are also stored in the
// configuration of the index (see `IndexImpl::rdfParserSettings`), so that
// everything that parses RDF data for an existing index (for example `LOAD` or
// the Graph Store Protocol) parses it in the same way as the index build.
struct RdfParserSettings {
  // How to handle integer literals that overflow QLever's 64-bit integers.
  TurtleParserIntegerOverflowBehavior integerOverflowBehavior_ =
      TurtleParserIntegerOverflowBehavior::Error;
  // If true then triples with invalid literals (for example
  // "noNumber"^^xsd:integer) are ignored. If false an exception is thrown when
  // such literals are encountered.
  bool invalidLiteralsAreSkipped_ = false;
  // If true, the faster `TokenizerCtre` is used instead of the
  // standard-compliant `Tokenizer` (see the comment on `TurtleParser` in
  // `RdfParser.h` for the limitations of the relaxed mode).
  //
  // NOTE: This setting is only evaluated by the parsers that handle several
  // files, when they choose the parser for a single file. The parsers for a
  // single file have the tokenizer as a template parameter.
  bool useRelaxedParsing_ = false;

  QL_DEFINE_DEFAULTED_EQUALITY_OPERATOR_LOCAL(RdfParserSettings,
                                              integerOverflowBehavior_,
                                              invalidLiteralsAreSkipped_,
                                              useRelaxedParsing_)

  // Convert to and from JSON. A key that is missing gets its default value.
  friend void to_json(nlohmann::json& j, const RdfParserSettings& settings);
  friend void from_json(const nlohmann::json& j, RdfParserSettings& settings);
};

#endif  // QLEVER_SRC_PARSER_RDFPARSERSETTINGS_H
