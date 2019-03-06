// Copyright 2016, University of Freiburg, Chair of Algorithms and Data
// Structures.
// Author: Björn Buchhold <buchholb>

#pragma once

#include <assert.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>
#include <cmath>
#include <iostream>
#include <string>
#include <vector>

#include "../global/Constants.h"
#include "./Exception.h"
#include "./StringUtils.h"

using std::cerr;
using std::cout;
using std::endl;
using std::string;
using std::vector;

namespace ad_utility {
//! Convert a value literal to an index word.
//! Ontology values have a prefix and a readable format apart form that.
//! IndexWords are not necessarily readable but lexicographical
//! comparison should yield the same ordering that one would expect from
//! a natural ordering of the values invloved.
inline string convertValueLiteralToIndexWord(const string& orig);

//! Convert an index word to an ontology value.
//! Ontology values have a prefix and a readable format apart form that.
//! IndexWords are not necessarily readable but lexicographical
//! comparison should yield the same ordering that one would expect from
//! a natural ordering of the values invloved.
inline string convertIndexWordToValueLiteral(const string& indexWord);

//! Enumeration type for the supported numeric xsd:<type>'s
enum class NumericType : char {
  INTEGER = 'I',
  FLOAT = 'F',
  DOUBLE = 'D',
  DECIMAL = 'T'
};

//! Converts like "12.34" to :v:float:PP0*2E0*1234F and -0.123 to
//! :v:float:M-0*1E9*876F with the last F used to indicate that the value
//! was originally a float. ('I' if it was an int).
inline string convertFloatStringToIndexWord(
    const string& value, const NumericType type = NumericType::FLOAT);

//! Converts like this: :v:float:PP0*2E0*1234F to "12.34" and
//! :v:float:M-0*1E9*876F to "-0.123".
inline string convertIndexWordToFloatString(const string& indexWord);

//! Converts like this: ":v:float:PP0*2E0*1234F to "12.34 and
//! :v:float:M-0*1E9*876F to -0.123".
inline float convertIndexWordToFloat(const string& indexWord);

//! Brings a date to the format:
//! return string(VALUE_DATE_PREFIX) + year + month + day +
//!        VALUE_DATE_TIME_SEPARATOR + hour + minute + second;
inline string normalizeDate(const string& value);

//! Add prefix and take complement for negative dates.
inline string convertDateToIndexWord(const string& value);

//! Only strip the prefix and revert the complement for negative years.
inline string convertIndexWordToDate(const string& indexWord);

//! Takes an integer as string and return the complement.
inline string getBase10ComplementOfIntegerString(const string& orig);

//! Remove leading zeros.
inline string removeLeadingZeros(const string& orig);

//! Check if this looks like an value literal
inline bool isXsdValue(const string& val);

//! Check if the given string is numeric, i.e. a decimal integer or float string
//! i.e. "42", "42.2", "0123", ".3" are all numeric while "a", "1F", "0x32" are
//! not
inline bool isNumeric(const string& val);

//! Converts numeric strings (as determined by isNumeric()) into index words
inline string convertNumericToIndexWord(const string& val);

//! Convert a language tag like "@en" to the corresponding entity uri
//! for the efficient language filter
inline string convertLangtagToEntityUri(const string& tag);
inline std::optional<string> convertEntityUriToLangtag(const string& word);
inline std::string convertToLanguageTaggedPredicate(const string& pred,
                                                    const string& langtag);

// _____________________________________________________________________________
string convertValueLiteralToIndexWord(const string& orig) {
  /*
   * Value literals can have one of two forms
   * 0) "123"^^<http://www.w3.org/2001/XMLSchema#integer>
   * 1) "123"^^xsd:integer
   *
   * TODO: This ignores the URI such that xsd:integer == foo:integer ==
   * <http://baz#integer>
   */
  assert(orig.size() > 0);
  assert(orig[0] == '\"');
  string value;
  string type;
  size_t posOfSecondQuote = orig.rfind('\"');
  assert(posOfSecondQuote != string::npos);
  // -1 for the quote and since substr takes a length
  // not an end position
  value = orig.substr(1, posOfSecondQuote - 1);
  if (orig[orig.size() - 1] == '>') {
    size_t posOfHashTag = orig.rfind('#');
    assert(posOfHashTag != string::npos);

    // +2 for '>' and Hashtag
    type = orig.substr(posOfHashTag + 1, orig.size() - (posOfHashTag + 2));
  } else {
    size_t posOfDoubleDot = orig.rfind(':');
    if (posOfDoubleDot == string::npos) {
      AD_THROW(ad_semsearch::Exception::BAD_INPUT,
               "No ':' in non-URL ValueLiteral " + orig);
    }

    // +1 for double dot
    type = orig.substr(posOfDoubleDot + 1, orig.size() - (posOfDoubleDot + 1));
  }

  if (type == "dateTime" || type == "gYear" || type == "gYearMonth" ||
      type == "date") {
    return convertDateToIndexWord(value);
  } else {
    // No longer convert to int. instead always convert to float and
    // have a special marker at the very end to tell if the original number
    // was int for float. The benefit: can compare float with int that way.
    if (type == "int" || type == "integer") {
      return convertFloatStringToIndexWord(value + ".0", NumericType::INTEGER);
    }
    // We treat double and decimal as synonyms for float
    if (type == "float") {
      return convertFloatStringToIndexWord(value, NumericType::FLOAT);
    } else if (type == "double") {
      return convertFloatStringToIndexWord(value, NumericType::DOUBLE);
    } else if (type == "decimal") {
      return convertFloatStringToIndexWord(value, NumericType::DECIMAL);
    }
  }
  return orig;
}

// _____________________________________________________________________________
string convertIndexWordToValueLiteral(const string& indexWord) {
  if (startsWith(indexWord, VALUE_DATE_PREFIX)) {
    std::ostringstream os;
    string date = removeLeadingZeros(convertIndexWordToDate(indexWord));
    if (date.size() == 0 || startsWith(date, VALUE_DATE_TIME_SEPARATOR)) {
      date = string("0") + date;
    }
    os << "\"" << date << "\"" << XSD_DATETIME_SUFFIX;
    return os.str();
  }
  if (startsWith(indexWord, VALUE_FLOAT_PREFIX)) {
    if (endsWith(indexWord, "F")) {
      std::ostringstream os;
      os << "\"" << convertIndexWordToFloatString(indexWord) << "\""
         << XSD_FLOAT_SUFFIX;
      return os.str();
    }
    if (endsWith(indexWord, "D")) {
      std::ostringstream os;
      os << "\"" << convertIndexWordToFloatString(indexWord) << "\""
         << XSD_DOUBLE_SUFFIX;
      return os.str();
    }
    if (endsWith(indexWord, "T")) {
      std::ostringstream os;
      os << "\"" << convertIndexWordToFloatString(indexWord) << "\""
         << XSD_DECIMAL_SUFFIX;
      return os.str();
    }
    if (endsWith(indexWord, "I")) {
      std::ostringstream os;
      string asFloat = convertIndexWordToFloatString(indexWord);
      os << "\"" << asFloat.substr(0, asFloat.find('.')) << "\""
         << XSD_INT_SUFFIX;
      return os.str();
    }
  }
  return indexWord;
}

// _____________________________________________________________________________
string convertFloatStringToIndexWord(const string& orig,
                                     const NumericType type) {
  // Need a copy to modify.
  string value;
  // ignore a + in the beginning of the number
  if (orig.size() > 0 && orig[0] == '+') {
    value = orig.substr(1);
  } else {
    value = orig;
  }
  size_t posOfDot = value.find('.');
  if (posOfDot == string::npos) {
    return convertFloatStringToIndexWord(value + ".0", type);
  }

  // Treat the special case 0.0
  if (value == "0.0" || value == "-0.0") {
    return string(VALUE_FLOAT_PREFIX) + "N0" + char(type);
  }

  std::ostringstream os;
  os << VALUE_FLOAT_PREFIX;

  bool negaMantissa = (value[0] == '-');
  if (negaMantissa) {
    value = value.substr(1);
    --posOfDot;
  }
  os << (negaMantissa ? 'M' : 'P');

  // Get the exponent.
  int exponent = static_cast<int>(posOfDot) - 1;
  if (posOfDot <= 1) {
    if (value[0] == '0') {
      exponent = -1;
      size_t i = 2;
      while (i < value.size() && value[i] == '0') {
        ++i;
        --exponent;
      }
      if (i == value.size()) {
        exponent = -1;
      }
    }
  }

  // Produce a string for the exponent sign.
  // Note that the index word has to start with one of:
  // PP, PM, M+, M- so that the lexicographical ordering
  // reflects the actual order of floats.
  bool negaExpo = (exponent < 0);
  if (negaMantissa) {
    os << (negaExpo ? '-' : '+');
  } else {
    os << (negaExpo ? 'M' : 'P');
  }

  // Produce a string representation of the exponent
  std::ostringstream expoOs;
  expoOs << exponent;
  string expoString = (negaExpo ? expoOs.str().substr(1) : expoOs.str());

  if (negaMantissa != negaExpo) {
    expoString = getBase10ComplementOfIntegerString(expoString);
  }

  // Add padding to the exponent;
  AD_CHECK_GT(DEFAULT_NOF_VALUE_EXPONENT_DIGITS, expoString.size());
  size_t nofPaddingElems =
      DEFAULT_NOF_VALUE_EXPONENT_DIGITS - expoString.size();
  for (size_t i = 0; i < nofPaddingElems; ++i) {
    os << (negaExpo == negaMantissa ? '0' : '9');
  }
  os << expoString;

  // Get the mantissa with digit complements for negative numbers.
  std::ostringstream mant;
  size_t i = 0;
  while (i < value.size()) {
    if (value[i] != '.') {
      if (negaMantissa) {
        mant << (9 - atoi(value.substr(i, 1).c_str()));
      } else {
        mant << value[i];
      }
    }
    ++i;
  }

  os << 'E';
  os << mant.str().substr(0, DEFAULT_NOF_VALUE_MANTISSA_DIGITS);
  // Padding for mantissa. Necessary because we append something
  // to restore the original type.
  for (size_t i = mant.str().size(); i < DEFAULT_NOF_VALUE_MANTISSA_DIGITS;
       ++i) {
    if (!negaMantissa) {
      os << '0';
    } else {
      os << '9';
    }
  }
  return os.str() + char(type);
}

// _____________________________________________________________________________
string convertIndexWordToFloatString(const string& indexWord) {
  const static size_t prefixLength =
      std::char_traits<char>::length(VALUE_FLOAT_PREFIX);
  AD_CHECK_GT(indexWord.size(), prefixLength + 1);
  // the -1 accounts for the originally float/int identifier suffix
  string number =
      indexWord.substr(prefixLength, indexWord.size() - prefixLength - 1);
  // Handle the special case 0.0
  if (number == "N0") {
    return "0.0";
  }
  assert(number.size() >= 5);
  bool negaMantissa = number[0] == 'M';
  bool negaExponent = number[1] == 'M' || number[1] == '-';

  size_t posOfE = number.find('E');
  assert(posOfE != string::npos && posOfE > 0 && posOfE < number.size() - 1);

  string exponentString =
      ((negaMantissa == negaExponent)
           ? number.substr(2, posOfE - 2)
           : getBase10ComplementOfIntegerString(number.substr(2, posOfE - 2)));
  size_t absExponent = static_cast<size_t>(atoi(exponentString.c_str()));
  string mantissa =
      (!negaMantissa
           ? number.substr(posOfE + 1)
           : getBase10ComplementOfIntegerString(number.substr(posOfE + 1)));

  std::ostringstream os;
  if (negaMantissa) {
    os << '-';
  }

  if (negaExponent) {
    os << "0.";
    --absExponent;
    while (absExponent > 0) {
      os << '0';
      --absExponent;
    }

    // Skip leading zeros of the mantissa
    size_t i = 0;
    while (mantissa[i] == '0') {
      ++i;
    }
    os << ad_utility::rstrip(mantissa.substr(i), '0');
  } else {
    // Skip leading zeros of the mantissa
    size_t i = 0;
    while (mantissa[i] == '0') {
      ++i;
    }

    // Collect later zeros and only write them if something nonzero is found.
    // This eliminates the padding.
    std::ostringstream zeros;
    size_t tenToThe = 0;
    while (i < mantissa.size()) {
      if (mantissa[i] == '0') {
        zeros << '0';
      } else {
        os << zeros.str();
        zeros.str("");
        os << mantissa[i];
      }
      ++i;
      if (tenToThe == absExponent) {
        os << zeros.str();
        zeros.str("");
        os << ".";
      }
      ++tenToThe;
    }

    if (os.str().back() == '.') {
      os << '0';
    }

    if (absExponent > mantissa.size()) {
      for (size_t i = 0; i < absExponent - mantissa.size(); ++i) {
        os << "0";
      }
    }
  }
  return os.str();
}

// _____________________________________________________________________________
float convertIndexWordToFloat(const string& indexWord) {
  size_t prefixLength = std::char_traits<char>::length(VALUE_FLOAT_PREFIX);
  AD_CHECK_GT(indexWord.size(), prefixLength + 1);
  // the -1 accounts for the originally float/int identifier suffix
  string number =
      indexWord.substr(prefixLength, indexWord.size() - prefixLength - 1);
  // Handle the special case 0.0
  if (number == "N0") {
    return 0;
  }
  assert(number.size() >= 5);
  bool negaMantissa = number[0] == 'M';
  bool negaExponent = number[1] == 'M' || number[1] == '-';

  size_t posOfE = number.find('E');
  assert(posOfE != string::npos && posOfE > 0 && posOfE < number.size() - 1);

  string exponentString =
      ((negaMantissa == negaExponent)
           ? number.substr(2, posOfE - 2)
           : getBase10ComplementOfIntegerString(number.substr(2, posOfE - 2)));
  long absExponent = static_cast<size_t>(atoi(exponentString.c_str()));
  string mantissa =
      (!negaMantissa
           ? number.substr(posOfE + 1)
           : getBase10ComplementOfIntegerString(number.substr(posOfE + 1)));
  size_t mStart, mStop;
  for (mStart = 0; mStart < mantissa.size() && mantissa[mStart] == '0';
       mStart++)
    ;
  for (mStop = mantissa.size() - 1; mStop > mStart && mantissa[mStop] == '0';
       mStop--)
    ;
  long absMantissa = stol(mantissa.substr(mStart, mStop - mStart + 1));
  unsigned int mantissaLog = std::log10(absMantissa);
  if (negaMantissa) {
    if (negaExponent) {
      return -absMantissa * std::pow(10, -absExponent - mantissaLog);
    } else {
      return -absMantissa * std::pow(10, absExponent - mantissaLog);
    }
  } else {
    if (negaExponent) {
      return absMantissa * std::pow(10, -absExponent - mantissaLog);
    } else {
      return absMantissa * std::pow(10, absExponent - mantissaLog);
    }
  }
}

// _____________________________________________________________________________
string normalizeDate(const string& orig) {
  string value(orig);
  // Remove timezone information if present.
  size_t posOfZ = value.find('Z');
  if (posOfZ != string::npos) {
    value = value.substr(0, posOfZ);
  }
  // Find the possible start of time information.
  size_t posOfT = value.find('T');

  string second = "00";
  string minute = "00";
  string hour = "00";
  string day = "00";
  string month = "00";
  string year = "0000";

  // Parse hour minute second.
  if (posOfT != string::npos) {
    size_t posOfFirstCol = string::npos;

    // Avoid finding a in timezone info, e.g. "T10-03:00"
    if (posOfT + 3 < value.size() && value[posOfT + 3] != '-' &&
        value[posOfT + 3] != '+') {
      posOfFirstCol = value.find(':');
    }
    size_t posOfSecondCol = string::npos;
    // Only look for a second colon if there is a first colon
    // and avoid finding a second colon in timezone info, e.g. "T10:00-03:00"
    if (posOfFirstCol != string::npos && posOfFirstCol + 3 < value.size() &&
        value[posOfFirstCol + 3] != '-' && value[posOfFirstCol + 3] != '+') {
      posOfSecondCol = value.find(':', posOfFirstCol + 1);
    }
    if (posOfSecondCol != string::npos) {
      assert(posOfSecondCol + 3 <= value.size());
      // Time has hours minutes and seconds.
      hour = value.substr(posOfT + 1, posOfFirstCol - (posOfT + 1));
      minute =
          value.substr(posOfFirstCol + 1, posOfSecondCol - (posOfFirstCol + 1));
      second = value.substr(posOfSecondCol + 1, 2);
      // We ignore possible milliseconds that follow.
    } else if (posOfFirstCol != string::npos) {
      assert(posOfFirstCol + 3 <= value.size());
      // Time has hours and minutes.
      hour = value.substr(posOfT + 1, posOfFirstCol - (posOfT + 1));
      minute = value.substr(posOfFirstCol + 1, 2);
    } else {
      assert(posOfT + 3 <= value.size());
      // Time has only hours.
      hour = value.substr(posOfT + 1, 2);
    }
    // Sanity check.
    assert(hour.size() == 2);
    assert(minute.size() == 2);
    assert(second.size() == 2);
  }
  if (posOfT == string::npos || posOfT != 0) {
    // Parse year month date.
    // Determine up the where we should parse.
    size_t end = value.size();
    if (posOfT != string::npos) {
      end = posOfT;
    }

    size_t posOfFirstHyph = value.find('-', 1);
    size_t posOfSecondHyph = string::npos;
    if (posOfFirstHyph != string::npos) {
      posOfSecondHyph = value.find('-', posOfFirstHyph + 1);
    }

    if (posOfSecondHyph != string::npos) {
      // Case has day and month
      year = value.substr(0, posOfFirstHyph);
      month = value.substr(posOfFirstHyph + 1,
                           posOfSecondHyph - (posOfFirstHyph + 1));
      day = value.substr(posOfSecondHyph + 1, end - (posOfSecondHyph + 1));
    } else if (posOfFirstHyph != string::npos) {
      // Case has month
      year = value.substr(0, posOfFirstHyph);
      month = value.substr(posOfFirstHyph + 1, end - (posOfFirstHyph + 1));
    } else {
      // Case year only
      year = value;
    }
    assert(month.size() == 2);
    assert(day.size() == 2);
  }

  string paddedYear;
  paddedYear.reserve(DEFAULT_NOF_DATE_YEAR_DIGITS);
  // Pad the year.
  if (year[0] == '-') {
    paddedYear.append("-");
    paddedYear.append(DEFAULT_NOF_DATE_YEAR_DIGITS - year.size(), '9');
    paddedYear.append(getBase10ComplementOfIntegerString(year.substr(1)));
  } else {
    paddedYear.append(DEFAULT_NOF_DATE_YEAR_DIGITS - year.size(), '0');
    paddedYear.append(year);
  }
  year = paddedYear;
  assert(year.size() == size_t(DEFAULT_NOF_DATE_YEAR_DIGITS));
  return string(VALUE_DATE_PREFIX) + year + "-" + month + "-" + day +
         VALUE_DATE_TIME_SEPARATOR + hour + ":" + minute + ":" + second;
}

// _____________________________________________________________________________
string convertDateToIndexWord(const string& value) {
  std::ostringstream os;
  string norm = normalizeDate(value);
  if (norm[0] == '-') {
    os << '-'
       << getBase10ComplementOfIntegerString(
              norm.substr(1, DEFAULT_NOF_DATE_YEAR_DIGITS - 1));
    os << norm.substr(DEFAULT_NOF_DATE_YEAR_DIGITS);
  } else {
    os << norm;
  }
  return os.str();
}

// _____________________________________________________________________________
string convertIndexWordToDate(const string& indexWord) {
  size_t prefixLength = std::char_traits<char>::length(VALUE_DATE_PREFIX);
  std::ostringstream os;
  if (indexWord[prefixLength] == '-') {
    os << '-'
       << getBase10ComplementOfIntegerString(indexWord.substr(
              prefixLength + 1, DEFAULT_NOF_DATE_YEAR_DIGITS - 1));
    os << indexWord.substr(prefixLength + DEFAULT_NOF_DATE_YEAR_DIGITS);
  } else {
    os << indexWord.substr(prefixLength);
  }
  return os.str();
}

// _____________________________________________________________________________
string getBase10ComplementOfIntegerString(const string& orig) {
  std::ostringstream os;
  for (size_t i = 0; i < orig.size(); ++i) {
    os << (9 - atoi(orig.substr(i, 1).c_str()));
  }
  return os.str();
}

// _____________________________________________________________________________
inline string removeLeadingZeros(const string& orig) {
  std::ostringstream os;
  size_t i = 0;
  for (; i < orig.size(); ++i) {
    if (orig[i] != '0') {
      break;
    }
  }
  for (; i < orig.size(); ++i) {
    os << orig[i];
  }
  return os.str();
}

// _____________________________________________________________________________
bool isXsdValue(const string& val) {
  // starting the search for "^^ at position 1 makes sure it's not in the
  // quotes as we already checked that the first char is the first quote
  return !val.empty() && val[0] == '\"' && val.find("\"^^", 1) != string::npos;
}

// _____________________________________________________________________________
bool isNumeric(const string& val) {
  if (val.empty()) {
    return false;
  }
  size_t start = (val[0] == '-' || val[0] == '+') ? 1 : 0;
  size_t posNonDigit = val.find_first_not_of("0123456789", start);
  if (posNonDigit == string::npos) {
    return true;
  }
  if (val[posNonDigit] == '.') {
    return posNonDigit + 1 < val.size() &&
           val.find_first_not_of("0123456789", posNonDigit + 1) == string::npos;
  }
  return false;
}

// _____________________________________________________________________________
string convertNumericToIndexWord(const string& val) {
  NumericType type = NumericType::FLOAT;
  string tmp = val;
  if (tmp.find('.') == string::npos) {
    tmp += ".0";
    type = NumericType::INTEGER;
  }
  return convertFloatStringToIndexWord(tmp, type);
}

// _________________________________________________________
string convertLangtagToEntityUri(const string& tag) {
  return URI_PREFIX + "@" + tag + ">";
}

// _________________________________________________________
std::optional<string> convertEntityUriToLangtag(const string& word) {
  static const string prefix = URI_PREFIX + "@";
  if (ad_utility::startsWith(word, prefix)) {
    return word.substr(prefix.size(), word.size() - prefix.size() - 1);
  } else {
    return std::nullopt;
  }
}
// _________________________________________________________
std::string convertToLanguageTaggedPredicate(const string& pred,
                                             const string& langtag) {
  return '@' + langtag + '@' + pred;
}
}  // namespace ad_utility
