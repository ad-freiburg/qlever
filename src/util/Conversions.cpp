//  Copyright 2022, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

#include "util/Conversions.h"

#include <assert.h>
#include <ctre/ctre.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include "../global/Constants.h"
#include "../parser/TokenizerCtre.h"
#include "./Exception.h"
#include "./StringUtils.h"

namespace ad_utility {

// _____________________________________________________________________________
const char* toTypeIri(NumericType type) {
  switch (type) {
    case NumericType::INTEGER:
      return XSD_INT_TYPE;
    case NumericType::FLOAT:
      return XSD_FLOAT_TYPE;
    case NumericType::DECIMAL:
      return XSD_DECIMAL_TYPE;
    case NumericType::DOUBLE:
      return XSD_DOUBLE_TYPE;
    default:
      // This should never happen
      AD_FAIL();
  }
}

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
      AD_THROW("No ':' in non-URL ValueLiteral " + orig);
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

// ____________________________________________________________________________
std::pair<string, const char*> convertIndexWordToLiteralAndType(
    const string& indexWord) {
  if (indexWord.starts_with(VALUE_DATE_PREFIX)) {
    string date = removeLeadingZeros(convertIndexWordToDate(indexWord));
    if (date.empty() || date.starts_with(VALUE_DATE_TIME_SEPARATOR)) {
      date = string("0") + date;
    }
    return std::make_pair(std::move(date), XSD_DATETIME_TYPE);
  }
  if (indexWord.starts_with(VALUE_FLOAT_PREFIX)) {
    auto type = NumericType{indexWord.back()};
    switch (type) {
      case NumericType::FLOAT:
      case NumericType::DOUBLE:
      case NumericType::DECIMAL:
        return std::make_pair(convertIndexWordToFloatString(indexWord),
                              toTypeIri(type));
      case NumericType::INTEGER:
        string asFloat = convertIndexWordToFloatString(indexWord);
        return std::make_pair(asFloat.substr(0, asFloat.find('.')),
                              toTypeIri(type));
    }
  }
  return std::make_pair(indexWord, nullptr);
}

// _____________________________________________________________________________
string convertIndexWordToValueLiteral(const string& indexWord) {
  auto [literal, typeIri] = convertIndexWordToLiteralAndType(indexWord);
  if (!typeIri) {
    return std::move(literal);
  }
  std::ostringstream os;
  os << "\"" << literal << "\"^^<" << typeIri << '>';
  return std::move(os).str();
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
  string expoString =
      (negaExpo ? std::move(expoOs).str().substr(1) : std::move(expoOs).str());

  if (negaMantissa != negaExpo) {
    expoString = getBase10ComplementOfIntegerString(expoString);
  }

  // Add padding to the exponent;
  AD_CONTRACT_CHECK(DEFAULT_NOF_VALUE_EXPONENT_DIGITS > expoString.size());
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
  for (size_t i = std::move(mant).str().size();
       i < DEFAULT_NOF_VALUE_MANTISSA_DIGITS; ++i) {
    if (!negaMantissa) {
      os << '0';
    } else {
      os << '9';
    }
  }
  return std::move(os).str() + char(type);
}

// _____________________________________________________________________________
string convertIndexWordToFloatString(const string& indexWord) {
  const static size_t prefixLength =
      std::char_traits<char>::length(VALUE_FLOAT_PREFIX);
  AD_CONTRACT_CHECK(indexWord.size() > prefixLength + 1);
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

    // Skip leading and trailing zeros of the mantissa
    size_t start = mantissa.find_first_not_of('0');
    if (start != std::string::npos) {
      size_t end = mantissa.find_last_not_of('0') + 1;
      os << string_view{mantissa}.substr(start, end - start);
    }
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
        os << std::move(zeros).str();
        zeros.str("");
        os << mantissa[i];
      }
      ++i;
      if (tenToThe == absExponent) {
        os << std::move(zeros).str();
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
  return std::move(os).str();
}

// _____________________________________________________________________________
float convertIndexWordToFloat(const string& indexWord) {
  size_t prefixLength = std::char_traits<char>::length(VALUE_FLOAT_PREFIX);
  AD_CONTRACT_CHECK(indexWord.size() > prefixLength + 1);
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
  double absMantissa = 0;
  try {
    auto mantissaSubstr = mantissa.substr(mStart, mStop - mStart + 1);
    // empty mantissa means "0.0"
    if (!mantissaSubstr.empty()) {
      absMantissa = stod(mantissa.substr(mStart, mStop - mStart + 1));
    }
  } catch (const std::exception& e) {
    string substr = mantissa.substr(mStart, mStop - mStart + 1);
    throw std::runtime_error(
        ""
        "Error in stol while trying to convert index word " +
        indexWord + ". The mantissa " + substr +
        " could not be parsed via stod");
  }
  if (absMantissa == 0) {
    return 0.0f;
  }
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
  return std::move(os).str();
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
  return std::move(os).str();
}

// _____________________________________________________________________________
string getBase10ComplementOfIntegerString(const string& orig) {
  std::ostringstream os;
  for (size_t i = 0; i < orig.size(); ++i) {
    os << (9 - atoi(orig.substr(i, 1).c_str()));
  }
  return std::move(os).str();
}

// _____________________________________________________________________________
string removeLeadingZeros(const string& orig) {
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
  return std::move(os).str();
}

// _____________________________________________________________________________
bool isXsdValue(string_view value) {
  static constexpr auto xsdValueRegex = ctll::fixed_string(
      "\"\\^\\^<http://www\\.w3\\.org/2001/XMLSchema#[a-zA-Z]+>$");
  return ctre::search<xsdValueRegex>(value);
}

// _____________________________________________________________________________
bool isNumeric(const string& value) {
  if (ctre::match<TurtleTokenCtre::Double>(value)) {
    throw std::out_of_range{
        "Decimal numbers with an explicit exponent are currently not supported "
        "by QLever, but the following number was encountered: " +
        value};
  }

  if (ctre::match<TurtleTokenCtre::Integer>(value)) {
    return true;
  }
  if (ctre::match<TurtleTokenCtre::Decimal>(value)) {
    return true;
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
  return INTERNAL_ENTITIES_URI_PREFIX + "@" + tag + ">";
}

// _________________________________________________________
std::optional<string> convertEntityUriToLangtag(const string& word) {
  static const string prefix = INTERNAL_ENTITIES_URI_PREFIX + "@";
  if (word.starts_with(prefix)) {
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