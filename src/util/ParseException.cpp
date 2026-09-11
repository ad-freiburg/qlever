//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Julian Mundhahs (mundhahj@informatik.uni-freiburg.de)

#include "util/ParseException.h"

#include <Token.h>

#include "util/Exception.h"
#include "util/Log.h"
#include "util/StringUtils.h"

// ___________________________________________________________________________
std::string ExceptionMetadata::coloredError() const {
  // stopIndex_ == startIndex_ - 1 might happen if the offending string is
  // empty.
  AD_CONTRACT_CHECK(stopIndex_ + 1 >= startIndex_);
  std::string_view query = query_;
  // The `startIndex_` and `stopIndex_` are wrt Unicode codepoints, but the
  // `query_` is UTF-8 encoded.
  auto first = ad_utility::getUTF8Substring(query, 0, startIndex_);
  auto middle = ad_utility::getUTF8Substring(query, startIndex_,
                                             stopIndex_ + 1 - startIndex_);
  auto end = ad_utility::getUTF8Substring(query, stopIndex_ + 1);

  return absl::StrCat(first, "\x1b[1m\x1b[4m\x1b[31m", middle, "\x1b[0m", end);
}

// ___________________________________________________________________________
std::string_view ExceptionMetadata::offendingClause() const {
  // stopIndex_ == startIndex_ - 1 might happen if the offending string is
  // empty.
  AD_CONTRACT_CHECK(stopIndex_ + 1 >= startIndex_);
  // The `startIndex_` and `stopIndex_` are wrt Unicode codepoints, but the
  // `query_` is UTF-8 encoded.
  return ad_utility::getUTF8Substring(query_, startIndex_,
                                      stopIndex_ + 1 - startIndex_);
}

// ___________________________________________________________________________
void logErrorAndHighlightedMetadata(
    std::string& errorMsg, const std::optional<ExceptionMetadata>& metadata) {
  AD_LOG_ERROR << errorMsg << std::endl;
  if (!metadata.has_value()) {
    return;
  }
  // The `coloredError()` message might fail because of the different
  // Unicode handling of QLever and ANTLR. Make sure to detect this case so
  // that we can fix it if it happens.
  try {
    AD_LOG_ERROR << metadata->coloredError() << std::endl;
  } catch (const std::exception& e) {
    errorMsg.append(absl::StrCat(
        " Highlighting an error for the command line log failed: ", e.what()));
    AD_LOG_ERROR << "Failed to highlight error in operation. " << e.what()
                 << std::endl;
    AD_LOG_ERROR << metadata->query_ << std::endl;
  }
}

// ___________________________________________________________________________
ParseException::ParseException(std::string_view cause,
                               std::optional<ExceptionMetadata> metadata,
                               std::string_view prefix)
    : causeRaw_{cause},
      cause_{absl::StrCat(prefix, " ", cause)},
      metadata_{std::move(metadata)} {
  if (metadata_.has_value()) {
    causeWithMetadata_ =
        absl::StrCat(cause_, " in \"", metadata_->offendingClause(),
                     "\" at line ", metadata_->line_);
  } else {
    causeWithMetadata_ = cause_;
  }
}
