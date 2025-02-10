// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Felix Meisen (fesemeisen@outlook.de)

#pragma once

#include <string>

#include "engine/TextIndexScanForEntity.h"
#include "engine/TextIndexScanForWord.h"
#include "parser/MagicServiceQuery.h"
#include "util/HashMap.h"

struct TextSearchConfig {
  std::optional<std::string> word_;
  std::optional<Variable> varToBindMatch_;
  std::optional<Variable> varToBindWordScore_;
  std::optional<std::variant<Variable, std::string>> entity_;
  std::optional<Variable> varToBindEntityScore_;
};

namespace parsedQuery {

class TextSearchException : public std::runtime_error {
  using std::runtime_error::runtime_error;
};

struct TextSearchQuery : MagicServiceQuery {
  ad_utility::HashMap<Variable, TextSearchConfig> configVarToConfigs_;
  ad_utility::HashMap<Variable, Variable> configVarToTextVar_;

  // See MagicServiceQuery
  void addParameter(const SparqlTriple& triple) override;

  std::vector<std::variant<TextIndexScanForWordConfiguration,
                           TextIndexScanForEntityConfiguration>>
  toConfigs() const;
};

}  // namespace parsedQuery
