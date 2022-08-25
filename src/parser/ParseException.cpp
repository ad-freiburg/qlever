//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Julian Mundhahs (mundhahj@informatik.uni-freiburg.de)

#include <Token.h>
#include <parser/ParseException.h>
#include <util/Exception.h>

std::string ExceptionMetadata::coloredError() const {
  AD_CHECK(startIndex_ < query_.size());
  AD_CHECK(stopIndex_ < query_.size());
  AD_CHECK(startIndex_ <= stopIndex_);
  std::string_view query = query_;
  auto first = query.substr(0, startIndex_);
  auto middle = query.substr(startIndex_, stopIndex_ - startIndex_ + 1);
  auto end = query.substr(stopIndex_ + 1);

  return absl::StrCat(first, "\x1b[1m\x1b[4m\x1b[31m", middle, "\x1b[0m", end);
}
