#include "LazyJsonParser.h"

#include <absl/strings/ascii.h>
#include <absl/strings/str_cat.h>

#include <algorithm>

namespace ad_utility {

std::string LazyJsonParser::parseChunk(std::string inStr) {
  inStr = absl::StrCat(remainingInput_, inStr);

  const auto startIt = inStr.begin() + remainingInput_.size();

  if (remainingInput_.empty()) {
    prefixPath_ = curPath_;
  }

  bool arrayPathTouched = inArrayPath_;
  bool endReached = false;
  auto materializeEnd = inStr.begin();
  auto strStart = startIt;

  auto openBracket = [&](bool isObject) {
    if (inArrayPath_) {
      ++openBracesInArrayPath_;
    } else if (setKeysValueType_) {
      curPath_.emplace_back(tempKey_, isObject);
      tempKey_.clear();

      inArrayPath_ = isInArrayPath();
      if (inArrayPath_) {
        arrayPathTouched = true;
      }

      setKeysValueType_ = false;
    }
    if (isObject) {
      expectKey_ = !inArrayPath_;
    }
  };

  auto closeBracket = [&](std::string::iterator it) {
    if (openBracesInArrayPath_ > 0) {
      --openBracesInArrayPath_;
      if (openBracesInArrayPath_ == 0) {
        materializeEnd = it + 1;
      }
    } else {
      inArrayPath_ = isInArrayPath();
      if (!curPath_.empty()) {
        curPath_.pop_back();
      }
    }
  };

  for (auto it = startIt; it != inStr.end(); ++it) {
    if (isEscaped_) {
      isEscaped_ = false;
      continue;
    }
    switch (*it) {
      case '{':
        openBracket(true);
        break;
      case '[':
        openBracket(false);
        break;
      case '}':
        if (curPath_.empty()) {
          endReached = true;
          materializeEnd = it + 1;
        }
        closeBracket(it);
        break;
      case ']':
        closeBracket(it);
        break;
      case '"':
        if (inArrayPath_) {
          continue;
        }
        inString_ = !inString_;

        if (expectKey_) {
          if (inString_) {
            strStart = it + 1;
          } else {
            absl::StrAppend(&tempKey_, std::string(strStart, it));
            setKeysValueType_ = true;
            expectKey_ = false;
          }
        }
        break;
      case ',':
        expectKey_ = curPath_.empty() || curPath_.back().isObject;
        tempKey_.clear();
        break;
      case '\\':
        isEscaped_ = true;
        break;
    }
  }

  // temporary save unfinished key
  if (expectKey_ && inString_) {
    absl::StrAppend(&tempKey_, std::string(strStart, inStr.end()));
  }

  std::string res = "";

  if ((!inArrayPath_ && !endReached) ||
      (inArrayPath_ && materializeEnd == inStr.begin())) {
    remainingInput_ = inStr;
    return res;
  }

  // skip bad prefix
  const std::string nonPrefixChars = ", ]}";
  auto fixedBegin =
      std::find_if(inStr.begin(), inStr.end(), [&nonPrefixChars](char c) {
        return nonPrefixChars.find(c) == std::string::npos;
      });

  // add prefix path after removing closed and ignored objects/arrays
  for (auto it = inStr.begin(); it != fixedBegin; ++it) {
    if ((*it == ']' || *it == '}') && !prefixPath_.empty()) {
      prefixPath_.pop_back();
    }
  }
  if (!prefixPath_.empty()) {
    res = "{";
    for (const auto& n : prefixPath_) {
      absl::StrAppend(&res, "\"", n.key, "\": ", n.isObject ? "{" : "[");
    }
  }
  prefixPath_.clear();

  absl::StrAppend(&res, std::string(fixedBegin, materializeEnd));
  remainingInput_ = std::string(materializeEnd, inStr.end());

  // reconstruct closing brackets
  for (auto it = curPath_.rbegin(); it != curPath_.rend(); ++it) {
    absl::StrAppend(&res, it->isObject ? "}" : "]");
  }

  if (curPath_.size() > 0) {
    absl::StrAppend(&res, "}");
  }
  return res;
}
}  // namespace ad_utility
