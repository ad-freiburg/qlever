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
LazyJsonParser::Generator LazyJsonParser::parse(
    cppcoro::generator<std::string_view> partialJson,
    std::vector<std::string> arrayPath) {
  LazyJsonParser p(std::move(arrayPath));
  Details& details = co_await cppcoro::getDetails;
  for (const auto& chunk : partialJson) {
    if (details.first100_.size() < 100) {
      details.first100_ += chunk.substr(0, 100);
    }
    details.last100_ =
        chunk.substr(std::max(0, static_cast<int>(chunk.size() - 100)), 100);

    if (auto res = p.parseChunk(chunk); res.has_value()) {
      co_yield res;
      if (p.endReached_) {
        co_return;
      }
    }
  }
}

// ____________________________________________________________________________
LazyJsonParser::Generator LazyJsonParser::parse(
    cppcoro::generator<ql::span<std::byte>> partialJson,
    std::vector<std::string> arrayPath) {
  return parse(
      [](cppcoro::generator<ql::span<std::byte>> partialJson)
          -> cppcoro::generator<std::string_view> {
        for (const auto& bytes : partialJson) {
          co_yield std::string_view(reinterpret_cast<const char*>(bytes.data()),
                                    bytes.size());
        }
      }(std::move(partialJson)),
      std::move(arrayPath));
}

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
std::optional<nlohmann::json> LazyJsonParser::parseChunk(
    std::string_view inStr) {
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
  if (std::holds_alternative<BeforeArrayPath>(state_)) {
    parseBeforeArrayPath(idx);
  }
  if (std::holds_alternative<InArrayPath>(state_)) {
    materializeEnd = parseInArrayPath(idx);
  }
  if (std::holds_alternative<AfterArrayPath>(state_)) {
    std::optional<size_t> optEnd = parseAfterArrayPath(idx);
    if (optEnd) {
      materializeEnd = optEnd.value();
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
      std::get<BeforeArrayPath>(state_).optLiteral_ =
          BeforeArrayPath::LiteralView{.start_ = idx, .length_ = 0};
    }
    inLiteral_ = true;
  }

  for (; idx < input_.size(); ++idx) {
    if (isEscaped_) {
      isEscaped_ = false;
      continue;
    }
    switch (input_[idx]) {
      case '"':
        // End of literal.
        if (std::holds_alternative<BeforeArrayPath>(state_)) {
          std::get<BeforeArrayPath>(state_).optLiteral_.value().length_ =
              idx -
              std::get<BeforeArrayPath>(state_).optLiteral_.value().start_;
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
size_t LazyJsonParser::parseInArrayPath(size_t& idx) {
  AD_CORRECTNESS_CHECK(std::holds_alternative<InArrayPath>(state_));
  auto& state = std::get<InArrayPath>(state_);
  size_t materializeEnd = 0;

  auto exitArrayPath = [&]() {
    state_ = AfterArrayPath{.remainingBraces_ = arrayPath_.size()};
    ++idx;
    if (arrayPath_.empty()) {
      materializeEnd = idx;
    }
    return materializeEnd;
  };

  for (; idx < input_.size(); ++idx) {
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
          return exitArrayPath();
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
  return materializeEnd;
}

// ____________________________________________________________________________
std::optional<size_t> LazyJsonParser::parseAfterArrayPath(size_t& idx) {
  AD_CORRECTNESS_CHECK(std::holds_alternative<AfterArrayPath>(state_));
  auto& state = std::get<AfterArrayPath>(state_);

  for (; idx < input_.size(); ++idx) {
    switch (input_[idx]) {
      case '{':
        state.remainingBraces_ += 1;
        break;
      case '}':
        state.remainingBraces_ -= 1;
        if (state.remainingBraces_ == 0) {
          // End reached.
          endReached_ = true;
          return idx + 1;
        }
        break;
      case '"':
        parseLiteral(idx);
        break;
      default:
        break;
    }
  }
  return std::nullopt;
}

// ____________________________________________________________________________
std::optional<nlohmann::json> LazyJsonParser::constructResultFromParsedChunk(
    size_t materializeEnd) {
  size_t nextChunkStart =
      materializeEnd == 0 ? 0 : std::min(materializeEnd + 1, input_.size());
  if (input_.size() - nextChunkStart >= 1'000'000) {
    throw Error(
        "QLever currently doesn't support SERVICE results where a single "
        "result row is larger than 1MB");
  }
  if (nextChunkStart == 0) {
    return std::nullopt;
  } else {
    AD_CORRECTNESS_CHECK(!std::holds_alternative<BeforeArrayPath>(state_));
  }

  std::string resStr = yieldCount_ > 0 ? prefixInArray_ : "";
  ++yieldCount_;

  bool parsingCompletelyDone =
      std::holds_alternative<AfterArrayPath>(state_) &&
      (std::get<AfterArrayPath>(state_).remainingBraces_ == 0);
  bool endsWithComma =
      materializeEnd < input_.size() && input_[materializeEnd] == ',';

  // materializeEnd either holds the index to a `,` between two elements in the
  // arrayPath or the (non-existent) first-character after the input.
  AD_CORRECTNESS_CHECK(endsWithComma || parsingCompletelyDone);

  absl::StrAppend(&resStr, input_.substr(0, materializeEnd));
  input_ = input_.substr(nextChunkStart);

  if (!parsingCompletelyDone) {
    absl::StrAppend(&resStr, suffixInArray_);
  }

  try {
    return nlohmann::json::parse(resStr);
  } catch (const nlohmann::json::parse_error& e) {
    throw Error(e.what());
  }
}

// ____________________________________________________________________________
void LazyJsonParser::BeforeArrayPath::tryAddKeyToPath(std::string_view input) {
  if (optLiteral_) {
    curPath_.emplace_back(
        input.substr(optLiteral_.value().start_, optLiteral_.value().length_));
    optLiteral_ = std::nullopt;
  }
}

}  // namespace ad_utility
