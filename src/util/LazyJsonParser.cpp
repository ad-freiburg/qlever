// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Moritz Dom (domm@informatik.uni-freiburg.de)

#include "util/LazyJsonParser.h"

#include <absl/strings/ascii.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_join.h>

#include <variant>

#include "util/Exception.h"

namespace ad_utility {

// ____________________________________________________________________________
LazyJsonParser::LazyJsonParser(std::vector<std::string> arrayPath)
    : arrayPath_(std::move(arrayPath)),
      prefixInArray_(absl::StrCat(
          absl::StrJoin(arrayPath_.begin(), arrayPath_.end(), "",
                        [](std::string* out, const std::string& s) {
                          absl::StrAppend(out, "{\"", s, "\": ");
                        }),
          "[")),
      suffixInArray_(absl::StrCat("]", std::string(arrayPath_.size(), '}'))) {}

// ____________________________________________________________________________
std::string LazyJsonParser::parseChunk(std::string_view inStr) {
  size_t idx = input_.size();
  absl::StrAppend(&input_, inStr);

  // End-index (exclusive) of the current `input_` to construct a result.
  size_t materializeEnd = 0;

  // If the previous chunk ended within a Literal, finish parsing it.
  if (inLiteral_) {
    parseLiteral(idx);
    ++idx;
  }

  // Resume parsing the current section.
  while (idx < input_.size()) {
    switch (state_.index()) {
      case 0:
        parseBeforeArrayPath(idx);
        break;
      case 1:
        parseInArrayPath(idx, materializeEnd);
        if (arrayPath_.empty() &&
            std::holds_alternative<AfterArrayPath>(state_)) {
          materializeEnd = idx;
        }
        break;
      case 2:
        parseAfterArrayPath(idx, materializeEnd);
        break;
      default:
        AD_FAIL();
        break;
    }
  }

  return constructResultFromParsedChunk(materializeEnd);
}

// ____________________________________________________________________________
void LazyJsonParser::parseLiteral(size_t& idx) {
  AD_CORRECTNESS_CHECK(inLiteral_ || input_[idx] == '"');
  if (input_[idx] == '"' && !inLiteral_) {
    ++idx;
    if (std::holds_alternative<BeforeArrayPath>(state_)) {
      std::get<BeforeArrayPath>(state_).strStart_ = idx;
    }
    inLiteral_ = true;
  }

  for (; idx < input_.size(); ++idx) {
    if (idx > 1'000'000) {
      throw std::runtime_error(
          "Ill-formed JSON: Element size exceeds 1 Megabyte.");
    }
    if (isEscaped_) {
      isEscaped_ = false;
      continue;
    }
    switch (input_[idx]) {
      case '"':
        // End of literal.
        if (std::holds_alternative<BeforeArrayPath>(state_)) {
          std::get<BeforeArrayPath>(state_).strEnd_ = idx;
        }
        inLiteral_ = false;
        return;
      case '\\':
        isEscaped_ = true;
        break;
      default:
        break;
    }
  }
}

// ____________________________________________________________________________
void LazyJsonParser::parseBeforeArrayPath(size_t& idx) {
  AD_CORRECTNESS_CHECK(std::holds_alternative<BeforeArrayPath>(state_));
  auto& state = std::get<BeforeArrayPath>(state_);

  for (; idx < input_.size(); ++idx) {
    if (idx > 1'000'000) {
      throw std::runtime_error(
          "Ill-formed JSON: Header size exceeds 1 Megabyte.");
    }
    switch (input_[idx]) {
      case '{':
        state.tryAddKeyToPath(input_);
        break;
      case '[':
        if (state.openBrackets_ == 0) {
          state.tryAddKeyToPath(input_);
        }
        ++state.openBrackets_;
        if (state.curPath_ == arrayPath_) {
          // Reached arrayPath.
          state_ = InArrayPath();
          ++idx;
          return;
        }
        break;
      case ']':
        --state.openBrackets_;
        if (state.openBrackets_ == 0 && !state.curPath_.empty()) {
          state.curPath_.pop_back();
        }
        break;
      case '}':
        if (!state.curPath_.empty()) {
          state.curPath_.pop_back();
        }
        break;
      case '"':
        parseLiteral(idx);
        break;
      default:
        break;
    }
  }
}

// ____________________________________________________________________________
void LazyJsonParser::parseInArrayPath(size_t& idx, size_t& materializeEnd) {
  AD_CORRECTNESS_CHECK(std::holds_alternative<InArrayPath>(state_));
  auto& state = std::get<InArrayPath>(state_);

  for (; idx < input_.size(); ++idx) {
    if (idx - materializeEnd > 1'000'000) {
      throw std::runtime_error(
          "Ill-formed JSON: Element size exceeds 1 Megabyte.");
    }
    switch (input_[idx]) {
      case '{':
      case '[':
        ++state.openBracketsAndBraces_;
        break;
      case '}':
        --state.openBracketsAndBraces_;
        break;
      case ']':
        if (state.openBracketsAndBraces_ == 0) {
          // End of ArrayPath reached.
          state_ = AfterArrayPath{.remainingBraces_ = arrayPath_.size()};
          ++idx;
          return;
        }
        --state.openBracketsAndBraces_;
        break;
      case ',':
        if (state.openBracketsAndBraces_ == 0) {
          materializeEnd = idx;
        }
        break;
      case '"':
        parseLiteral(idx);
        break;
      default:
        break;
    }
  }
}

// ____________________________________________________________________________
void LazyJsonParser::parseAfterArrayPath(size_t& idx, size_t& materializeEnd) {
  AD_CORRECTNESS_CHECK(std::holds_alternative<AfterArrayPath>(state_));
  auto& state = std::get<AfterArrayPath>(state_);

  for (; idx < input_.size(); ++idx) {
    if (idx - materializeEnd > 1'000'000) {
      throw std::runtime_error(
          "Ill-formed JSON: Element size exceeds 1 Megabyte.");
    }
    switch (input_[idx]) {
      case '{':
        state.remainingBraces_ += 1;
        break;
      case '}':
        state.remainingBraces_ -= 1;
        if (state.remainingBraces_ == 0) {
          // End reached.
          materializeEnd = idx + 1;
        }
        break;
      case '"':
        parseLiteral(idx);
        break;
      default:
        break;
    }
  }
}

// ____________________________________________________________________________
std::string LazyJsonParser::constructResultFromParsedChunk(
    size_t materializeEnd) {
  std::string res;
  if (materializeEnd == 0) {
    return res;
  }

  if (yieldCount_ > 0) {
    res = prefixInArray_;
  }
  ++yieldCount_;

  absl::StrAppend(&res, input_.substr(0, materializeEnd));
  input_ = input_.substr(std::min(materializeEnd + 1, input_.size()));

  if (std::holds_alternative<InArrayPath>(state_)) {
    absl::StrAppend(&res, suffixInArray_);
  }

  return res;
}

// ____________________________________________________________________________
void LazyJsonParser::BeforeArrayPath::tryAddKeyToPath(std::string_view input) {
  if (strEnd_ != 0) {
    curPath_.emplace_back(input.substr(strStart_, strEnd_ - strStart_));
  }
}

}  // namespace ad_utility
