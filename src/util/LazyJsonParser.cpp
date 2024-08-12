#include "LazyJsonParser.h"

#include <absl/strings/ascii.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_join.h>

namespace ad_utility {

// ____________________________________________________________________________
LazyJsonParser::LazyJsonParser(std::vector<std::string> arrayPath)
    : arrayPath_(arrayPath),
      prefixInArray_(absl::StrCat(
          absl::StrJoin(arrayPath.begin(), arrayPath.end(), "",
                        [](std::string* out, const std::string& s) {
                          absl::StrAppend(out, "{\"", s, "\": ");
                        }),
          "[")),
      suffixInArray_(absl::StrCat("]", std::string(arrayPath.size(), '}'))) {}

// ____________________________________________________________________________
std::string LazyJsonParser::parseChunk(const std::string& inStr) {
  size_t idx = input_.size();
  absl::StrAppend(&input_, inStr);
  size_t materializeEnd = 0;

  auto tryAddKeyToPath = [this]() {
    if (strEnd_ != 0) {
      curPath_.emplace_back(input_.substr(strStart_, strEnd_ - strStart_));
    }
  };

  if (inString_) {
    parseString(idx);
    ++idx;
  }

  if (inArrayPath_) {
    materializeEnd = parseArrayPath(idx);
    ++idx;
  }

  for (; idx < input_.size(); ++idx) {
    switch (input_[idx]) {
      case '{':
        tryAddKeyToPath();
        break;
      case '[':
        if (openBrackets_ == 0) {
          tryAddKeyToPath();
        }
        ++openBrackets_;
        if (isInArrayPath()) {
          inArrayPath_ = true;
          ++idx;
          materializeEnd = parseArrayPath(idx);
        }
        break;
      case ']':
        --openBrackets_;
        if (openBrackets_ == 0 && !curPath_.empty()) {
          curPath_.pop_back();
        }
        break;
      case '}':
        if (curPath_.empty()) {
          materializeEnd = idx + 1;
        } else {
          curPath_.pop_back();
        }
        break;
      case '"':
        parseString(idx);
        break;
      default:
        break;
    }
  }

  // Construct result.
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

  if (inArrayPath_) {
    absl::StrAppend(&res, suffixInArray_);
  }

  return res;
}

// ____________________________________________________________________________
size_t LazyJsonParser::parseArrayPath(size_t& idx) {
  size_t lastObjectEnd = 0;
  for (; idx < input_.size(); ++idx) {
    switch (input_[idx]) {
      case '{':
        ++openBracesInArrayPath_;
        break;
      case '[':
        ++openBracesInArrayPath_;
        break;
      case '}':
        --openBracesInArrayPath_;
        break;
      case ']':
        if (openBracesInArrayPath_ == 0 && !curPath_.empty()) {
          inArrayPath_ = false;
          curPath_.pop_back();
          return lastObjectEnd;
        }
        --openBracesInArrayPath_;
        break;
      case ',':
        if (openBracesInArrayPath_ == 0) {
          lastObjectEnd = idx;
        }
        break;
      case '"':
        parseString(idx);
        break;
      default:
        break;
    }
  }
  return lastObjectEnd;
}

// ____________________________________________________________________________
void LazyJsonParser::parseString(size_t& idx) {
  for (; idx < input_.size(); ++idx) {
    if (isEscaped_) {
      isEscaped_ = false;
      continue;
    }
    switch (input_[idx]) {
      case '"':
        inString_ = !inString_;
        if (inString_) {
          strStart_ = idx + 1;
        } else {
          strEnd_ = idx;
          return;
        }
        break;
      case '\\':
        isEscaped_ = true;
        break;
      default:
        break;
    }
  }
}

}  // namespace ad_utility
