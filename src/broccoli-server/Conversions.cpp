// Copyright 2011, University of Freiburg, Chair of Algorithms and Data
// Structures.
// Author: Björn Buchhold <buchholb>

#include <stdlib.h>
#include <stdint.h>
#include <assert.h>
#include <string>
#include <sstream>
#include <vector>

#include "../util/StringUtils.h"
#include "../util/Log.h"
#include "./Globals.h"
#include "../util/Exception.h"
#include "./Conversions.h"

using std::vector;
using std::string;
using std::endl;

using ad_utility::startsWith;
using ad_utility::getLowercaseUtf8;

namespace ad_semsearch {
// _____________________________________________________________________________
bool splitAndCheckTurtle(const string& strTriple, vector<string>* result) {
  result->clear();
  *result = ad_utility::split(strTriple, '\t');
  if (result->size() == 4 && (*result)[3] == ".") {
    (*result)[0] = ad_utility::strip((*result)[0], ' ');
    (*result)[1] = ad_utility::strip((*result)[1], ' ');
    (*result)[2] = ad_utility::strip((*result)[2], ' ');
    return (*result)[0].size() > 0 && (*result)[1].size() > 0
        && (*result)[2].size() > 0;
  }
  return false;
}
// _____________________________________________________________________________
void splitTurtleUncheckedButFast(const string& strTriple,
    vector<string>* result) {
  result->resize(3);
  size_t indexOfTab1 = strTriple.find('\t');
  size_t indexOfTab2 = strTriple.find('\t', indexOfTab1 + 1);
  size_t indexOfTab3 = strTriple.find('\t', indexOfTab2 + 1);
  (*result)[0] = strTriple.substr(0, indexOfTab1);
  (*result)[1] = strTriple.substr(indexOfTab1 + 1,
      indexOfTab2 - (indexOfTab1 + 1));
  (*result)[2] = strTriple.substr(indexOfTab2 + 1,
      indexOfTab3 - (indexOfTab2 + 1));
}
// _____________________________________________________________________________
string subjectToBroccoliStyle(const string& orig) {
  return string(ENTITY_PREFIX) + entify(orig);
}
// _____________________________________________________________________________
string entifyAndFirstCharToUpper(const string& orig) {
  return ad_utility::firstCharToUpperUtf8(entify(orig));
}
// _____________________________________________________________________________
string predicateToBroccoliStyle(const string& orig) {
  return string(RELATION_PREFIX) + entify(orig);
}
// _____________________________________________________________________________
string objectToBroccoliStyle(const string& orig) {
  if (orig.find(XSD_VALUE_NS) != string::npos) {
    return convertFreebaseXsdValueToIndexWord(orig);
  }
  return string(ENTITY_PREFIX) + entify(orig);
}
// _____________________________________________________________________________
string convertFreebaseDateTimeValueToIndexWord(const string& orig) {
  string value = orig;
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
    if (posOfT + 3 < value.size()
        && value[posOfT + 3] != '-'
        && value[posOfT + 3] != '+') {
        posOfFirstCol = value.find(':');
    }
    size_t posOfSecondCol = string::npos;
    // Only look for a second colon if there is a first colon
    // and avoid finding a second colon in timezone info, e.g. "T10:00-03:00"
    if (posOfFirstCol != string::npos
        && posOfFirstCol + 3 < value.size()
        && value[posOfFirstCol + 3] != '-'
        && value[posOfFirstCol + 3] != '+') {
      posOfSecondCol = value.find(':', posOfFirstCol + 1);
    }
    if (posOfSecondCol != string::npos) {
      assert(posOfSecondCol + 3 <= value.size());
      // Time has hours minutes and seconds.
      hour = value.substr(posOfT + 1, posOfFirstCol - (posOfT + 1));
      minute = value.substr(posOfFirstCol + 1,
          posOfSecondCol - (posOfFirstCol + 1));
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
  return string(VALUE_DATE_PREFIX) + year + month + day +
        VALUE_DATE_TIME_SEPARATOR + hour + minute + second;
}
// _____________________________________________________________________________
string convertFreebaseXsdValueToIndexWord(const string& orig) {
  // Take the easy route: convert to ontology word (readable).
  assert(orig.size() > 0);
  assert(orig[0] == '\"');
  assert(orig[orig.size() - 1] == '>');
  size_t posOfSecondQuote = orig.find('\"', 1);
  size_t posOfHashTag = orig.find('#');
  assert(posOfSecondQuote != string::npos);
  assert(posOfHashTag != string::npos);

  string value = orig.substr(1, posOfSecondQuote - 1);
  string type = orig.substr(posOfHashTag + 1, orig.size() - (posOfHashTag + 2));

  if (type == "dateTime" || type == "gYear" || type == "gYearMonth"
      || type == "date") {
    return convertFreebaseDateTimeValueToIndexWord(value);
  } else {
    return convertOntologyValueToIndexWord(
        string(VALUE_PREFIX) + type + ":" + value);
  }
}
// _____________________________________________________________________________
string convertOntologyValueToIndexWord(const string& orig) {
  if (startsWith(orig, ad_semsearch::VALUE_DATE_PREFIX)) {
    return convertOntologyDateToIndexWord(orig);
  }
  if (startsWith(orig, ":v:int:")) {
    return convertOntologyIntegerToIndexWord(
        string(VALUE_INTEGER_PREFIX) + orig.substr(7),
        ad_semsearch::DEFAULT_NOF_VALUE_INTEGER_DIGITS);
  }
  if (startsWith(orig, ad_semsearch::VALUE_INTEGER_PREFIX)) {
    return convertOntologyIntegerToIndexWord(orig,
        ad_semsearch::DEFAULT_NOF_VALUE_INTEGER_DIGITS);
  }
  if (startsWith(orig, ad_semsearch::VALUE_FLOAT_PREFIX)) {
    return convertOntologyFloatToIndexWord(orig,
        ad_semsearch::DEFAULT_NOF_VALUE_EXPONENT_DIGITS,
        ad_semsearch::DEFAULT_NOF_VALUE_MANTISSA_DIGITS);
  }
  return orig;
}
// _____________________________________________________________________________
string convertIndexWordToOntologyValue(const string& indexWord) {
  if (startsWith(indexWord, ad_semsearch::VALUE_DATE_PREFIX)) {
    return convertIndexWordToOntologyDate(indexWord);
  }
  if (startsWith(indexWord, ad_semsearch::VALUE_INTEGER_PREFIX)) {
    return convertIndexWordToOntologyInteger(indexWord);
  }
  if (startsWith(indexWord, ad_semsearch::VALUE_FLOAT_PREFIX)) {
    return convertIndexWordToOntologyFloat(indexWord);
  }
  return indexWord;
}
// _____________________________________________________________________________
string convertOntologyIntegerToIndexWord(const string& ontologyInteger,
    size_t nofDigits) {
  size_t prefixLength = std::char_traits<char>::length(
      ad_semsearch::VALUE_INTEGER_PREFIX);
  AD_CHECK_GT(ontologyInteger.size(), prefixLength);
  string number = ontologyInteger.substr(prefixLength);
  std::ostringstream os;
  os << ad_semsearch::VALUE_INTEGER_PREFIX;
  bool isNegative = (number[0] == '-');
  os << (isNegative ? 'M' : 'P');

  size_t length = number.size();
  if (isNegative) --length;
  AD_CHECK_GE(nofDigits, length);
  size_t nofLeadingChars = nofDigits - length;

  for (size_t i = 0; i < nofLeadingChars; ++i) {
    os << (isNegative ? '9' : '0');
  }

  os << (isNegative ? getBase10ComplementOfIntegerString(number.substr(1))
      : number);

  return os.str();
}
// _____________________________________________________________________________
string convertIndexWordToOntologyInteger(const string& indexWord) {
  size_t prefixLength = std::char_traits<char>::length(
      ad_semsearch::VALUE_INTEGER_PREFIX);
  AD_CHECK_GT(indexWord.size(), prefixLength + 1);
  string number = indexWord.substr(prefixLength);
  bool isNegative = (number[0] == 'M');
  std::ostringstream os;
  os << ad_semsearch::VALUE_INTEGER_PREFIX;
  if (isNegative) {
    os << "-";
    size_t i = 1;
    while (number[i] == '9') {
      ++i;
    }
    os << getBase10ComplementOfIntegerString(number.substr(i));
  } else {
    size_t i = 1;
    while (number[i] == '0') {
      ++i;
    }

    if (number.substr(i).size() > 0) {
      os << number.substr(i);
    } else {
      os << '0';
    }
  }
  return os.str();
}
// _____________________________________________________________________________
string convertOntologyFloatToIndexWord(const string& ontologyFloat,
    size_t nofExponentDigits, size_t nofMantissaDigits) {
  size_t prefixLength = std::char_traits<char>::length(
      ad_semsearch::VALUE_FLOAT_PREFIX);
  AD_CHECK_GT(ontologyFloat.size(), prefixLength);
  string number = ontologyFloat.substr(prefixLength);

  size_t posOfDot = number.find('.');
  if (posOfDot == string::npos) {
    return convertOntologyFloatToIndexWord(
        string(ad_semsearch::VALUE_FLOAT_PREFIX) + number + ".0",
        nofExponentDigits, nofMantissaDigits);
  }

  // Treat the special case 0.0
  if (number == "0.0") {
    return string(ad_semsearch::VALUE_FLOAT_PREFIX) + "N0";
  }


  std::ostringstream os;
  os << ad_semsearch::VALUE_FLOAT_PREFIX;

  bool negaMantissa = (number[0] == '-');
  if (negaMantissa) {
    number = number.substr(1);
    --posOfDot;
  }
  os << (negaMantissa ? 'M' : 'P');

  // Get the exponent.
  assert(posOfDot >= 1);
  int exponent = static_cast<int> (posOfDot) - 1;

  if (posOfDot <= 1) {
    if (number[0] == '0') {
      exponent = -1;
      size_t i = 2;
      while (i < number.size() && number[i] == '0') {
        ++i;
        --exponent;
      }
      if (i == number.size()) {
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
  AD_CHECK_GT(nofExponentDigits, expoString.size());
  size_t nofPaddingElems = nofExponentDigits - expoString.size();
  for (size_t i = 0; i < nofPaddingElems; ++i) {
    os << (negaExpo == negaMantissa ? '0' : '9');
  }
  os << expoString;

  // Get the mantissa with digit complements for negative numbers.
  std::ostringstream mant;
  size_t i = 0;
  while (i < number.size()) {
    if (number[i] != '.') {
      if (negaMantissa) {
        mant << (9 - atoi(number.substr(i, 1).c_str()));
      } else {
        mant << number[i];
      }
    }
    ++i;
  }

  // No need to add padding to the mantissa.
  os << 'E';
  os << mant.str();
//  string mantissa = mant.str();
//
//  AD_CHECK_GT(nofMantissaDigits, mantissa.size());
//  nofPaddingElems = nofMantissaDigits - mantissa.size();
//
//  os << mantissa;
//  for (size_t i = 0; i < nofPaddingElems; ++i)
//  {
//    os << (negaMantissa ? '9' : '0');
//  }
//
  return os.str();
}
// _____________________________________________________________________________
string convertIndexWordToOntologyFloat(const string& indexWord) {
  size_t prefixLength = std::char_traits<char>::length(
      ad_semsearch::VALUE_FLOAT_PREFIX);
  AD_CHECK_GT(indexWord.size(), prefixLength);
  string number = indexWord.substr(prefixLength);
  // Handle the special case 0.0
  if (number == "N0") {
    return string(ad_semsearch::VALUE_FLOAT_PREFIX) + "0.0";
  }
  assert(number.size() >= 5);
  bool negaMantissa = number[0] == 'M';
  bool negaExponent = number[1] == 'M' || number[1] == '-';

  size_t posOfE = number.find('E');
  assert(posOfE != string::npos && posOfE > 0 && posOfE < number.size() - 1);

  string exponentString = ((negaMantissa == negaExponent) ? number.substr(2,
      posOfE - 2) : getBase10ComplementOfIntegerString(
      number.substr(2, posOfE - 2)));
  size_t absExponent = static_cast<size_t> (atoi(exponentString.c_str()));
  string mantissa = (!negaMantissa ? number.substr(posOfE + 1)
      : getBase10ComplementOfIntegerString(number.substr(posOfE + 1)));

  std::ostringstream os;
  os << ad_semsearch::VALUE_FLOAT_PREFIX;
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
    os << mantissa.substr(i);
  } else {
    // Skip leading zeros of the mantissa
    size_t i = 0;
    while (mantissa[i] == '0') {
      ++i;
    }

    size_t tenToThe = 0;
    while (i < mantissa.size()) {
      os << mantissa[i];
      ++i;
      if (tenToThe == absExponent) {
        os << ".";
      }
      ++tenToThe;
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
void decodeUrl(const string& url, string* decoded) {
  decoded->clear();
  for (size_t i = 0; i < url.size(); ++i) {
    if (url[i] == '+') {
      *decoded += ' ';
    } else if (url[i] == '%' && i + 2 < url.size()) {
      char h1 = tolower(url[i + 1]);
      if (h1 >= '0' && h1 <= '9') h1 = h1 - '0';
      else if (h1 >= 'a' && h1 <= 'f') {
        h1 = h1 - 'a' + 10;
      } else {
        h1 = -1;
      }
      char h2 = tolower(url[i + 2]);
      if (h2 >= '0' && h2 <= '9') {
        h2 = h2 - '0';
      } else if (h2 >= 'a' && h2 <= 'f') {
        h2 = h2 - 'a' + 10;
      } else {
        h2 = -1;
      }
      if (h1 != -1 && h2 != -1) {
        *decoded += static_cast<char>(h1 * 16 + h2);
        i += 2;
      } else {
        *decoded += '%';
      }
    } else {
      *decoded += url[i];
    }
  }
}
}
