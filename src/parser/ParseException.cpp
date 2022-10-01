//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Julian Mundhahs (mundhahj@informatik.uni-freiburg.de)

#include <Token.h>
#include <parser/ParseException.h>
#include <util/Exception.h>

std::string ExceptionMetadata::coloredError() const {
  // stopIndex_ == startIndex_ - 1 might happen if the offending string is
  // empty.
  AD_CHECK(stopIndex_ + 1 >= startIndex_);
  std::string_view query = query_;
  // The `startIndex_` and `stopIndex_` are wrt Unicode codepoints, but the
  // `query_` is UTF-8 encoded.
  auto first = ad_utility::getUTF8Substring(query, 0, startIndex_);
  auto middle = ad_utility::getUTF8Substring(query, startIndex_,
                                             stopIndex_ + 1 - startIndex_);
  auto end = ad_utility::getUTF8Substring(query, stopIndex_ + 1);

  return absl::StrCat(first, "\x1b[1m\x1b[4m\x1b[31m", middle, "\x1b[0m", end);
}
