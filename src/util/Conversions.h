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
#include <variant>

#include "../global/Constants.h"
#include "./Exception.h"
#include "./StringUtils.h"
#include "../global/Id.h"
#include "../parser/XsdParser.h"

using std::cerr;
using std::cout;
using std::endl;
using std::string;
using std::vector;

using ParsedVocabularyEntry = std::variant<FancyId, std::string>;
inline decltype(auto) operator<<(std::ostream& str, const ParsedVocabularyEntry entry) {
  str << entry.index() << ":";
  std::visit([&str](const auto& el) {str << el;}, entry);
  return str;
}

  namespace ad_utility {
//! Convert a value literal to an index word.
//! Ontology values have a prefix and a readable format apart form that.
//! IndexWords are not necessarily readable but lexicographical
//! comparison should yield the same ordering that one would expect from
//! a natural ordering of the values invloved.
inline ParsedVocabularyEntry convertValueLiteralToIndexWord(const string& orig);

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
inline ParsedVocabularyEntry convertNumericToIndexWord(const string& val);

//! Convert a language tag like "@en" to the corresponding entity uri
//! for the efficient language filter
inline string convertLangtagToEntityUri(const string& tag);
inline std::optional<string> convertEntityUriToLangtag(const string& word);
inline std::string convertToLanguageTaggedPredicate(const string& pred,
                                                    const string& langtag);

// _____________________________________________________________________________
ParsedVocabularyEntry convertValueLiteralToIndexWord(const string& orig) {
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
      return FancyId(FancyId::INTEGER, std::stoll(value));
    }
    // We treat double and decimal as synonyms for float
    if (type == "float") {
      return FancyId(XsdParser::parseFloat(value).value());
    } else if (type == "double") {
      return FancyId(XsdParser::parseFloat(value).value());
    } else if (type == "decimal") {
      return FancyId(XsdParser::parseFloat(value).value());
    }
  }
  return orig;
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
  ParsedVocabularyEntry convertNumericToIndexWord(const string& val) {
    // TODO<joka921> Also differentiate between floats and ints here
    return FancyId{XsdParser::parseFloat(val).value()};
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
  throw std::runtime_error("Values other than Dates are no longer represented as strings. Please report this");
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
}  // namespace ad_utility
