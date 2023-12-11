// Copyright 2023, University of Freiburg, Chair of Algorithms and Data
// Structures.
// Author: Andre Schlegel (November of 2023,
// schlegea@informatik.uni-freiburg.de)

#pragma once

#include "util/StringUtils.h"

namespace ad_utility {
// ___________________________________________________________________________
std::string insertThousandSeparator(const std::string_view str,
                                    const char separatorSymbol,
                                    const char floatingPointSignifier) {
  // For identification of a digit.
  auto isDigit = [](const char c) {
    // `char` is ASCII. So the number symbols are the codes from 48 to 57.
    return '0' <= c && c <= '9';
  };

  // Numbers as `delimiterSymbol`, or `floatingPointSignifier` are not allowed.
  AD_CONTRACT_CHECK(!isDigit(separatorSymbol) &&
                    !isDigit(floatingPointSignifier));

  /*
  Not all ranges support the option to insert new values between old values,
  so we create a new string in the wanted format.
  */
  std::ostringstream ostream;

  // Identifying groups of thousands is easier, when reversing the range.
  auto reversedRange = std::views::reverse(str);
  auto rSearchIterator = std::begin(reversedRange);
  const auto rEnd = std::end(reversedRange);

  // 'Parse' the string.
  while (rSearchIterator != rEnd) {
    auto nextNonDigitIterator =
        std::ranges::find_if_not(rSearchIterator, rEnd, isDigit);

    /*
    If `rSearchIterator` is a digit and `nextNonDigitIterator` not  the
    `floatingPointSignifier`, repeatedly add 3 digits, followed by the
    delimiter, until there are less than 4 digits remaining between
    `rSearchIterator` and `nextNonDigitIterator`.
    Because there an no valid thousander delimiters anywhere between 3 digits.
    */
    if (rSearchIterator != nextNonDigitIterator &&
        (nextNonDigitIterator == rEnd ||
         *nextNonDigitIterator != floatingPointSignifier)) {
      while (std::distance(rSearchIterator, nextNonDigitIterator) > 3) {
        ostream << *(rSearchIterator++) << *(rSearchIterator++)
                << *(rSearchIterator++) << separatorSymbol;
      }
    }

    /*
    Find the start of the next digit sequence and add the remaining
    unprocessed symbols before it.
    */
    auto nextDigitSequenceStartIterator =
        std::ranges::find_if(nextNonDigitIterator, rEnd, isDigit);
    std::ranges::for_each(rSearchIterator, nextDigitSequenceStartIterator,
                          [&ostream](const char c) { ostream << c; });
    rSearchIterator = nextDigitSequenceStartIterator;
  }

  // Remember: We copied a reversed `r`.
  // TODO Is it possible, to just reverse the `ostream`?
  std::string toReturn{std::move(ostream).str()};
  std::ranges::reverse(toReturn);
  return toReturn;
}

}  // namespace ad_utility
