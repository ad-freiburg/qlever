// Copyright 2016, University of Freiburg, Chair of Algorithms and Data
// Structures.
// Author: Björn Buchhold <buchholb>

#pragma once

#include <optional>
#include <string>
#include <string_view>

namespace ad_utility {
//! Convert a value literal to an index word.
//! Ontology values have a prefix and a readable format apart form that.
//! IndexWords are not necessarily readable but lexicographical
//! comparison should yield the same ordering that one would expect from
//! a natural ordering of the values involved.
std::string convertValueLiteralToIndexWord(const std::string& orig);

//! Convert an index word to an ontology value.
//! Ontology values have a prefix and a readable format apart form that.
//! IndexWords are not necessarily readable but lexicographical
//! comparison should yield the same ordering that one would expect from
//! a natural ordering of the values invloved.
std::string convertIndexWordToValueLiteral(const std::string& indexWord);

//! Enumeration type for the supported numeric xsd:<type>'s
enum class NumericType : char {
  INTEGER = 'I',
  FLOAT = 'F',
  DOUBLE = 'D',
  DECIMAL = 'T'
};

const char* toTypeIri(NumericType type);

//! Converts strings like "12.34" to :v:float:PP0*2E0*1234F and -0.123 to
//! :v:float:M-0*1E9*876F with the last F used to indicate that the value was
//! originally a float. ('I' if it was an int).
std::string convertFloatStringToIndexWord(
    const std::string& value, const NumericType type = NumericType::FLOAT);

//! Converts strings like this: :v:float:PP0*2E0*1234F to "12.34" and
//! :v:float:M-0*1E9*876F to "-0.123".
std::string convertIndexWordToFloatString(const std::string& indexWord);

//! Converts strings like this: ":v:float:PP0*2E0*1234F to "12.34 and
//! :v:float:M-0*1E9*876F to -0.123".
float convertIndexWordToFloat(const std::string& indexWord);

//! Brings a date to the format:
//! return std::string(VALUE_DATE_PREFIX) + year + month + day +
//!        VALUE_DATE_TIME_SEPARATOR + hour + minute + second;
std::string normalizeDate(const std::string& value);

//! Add prefix and take complement for negative dates.
std::string convertDateToIndexWord(const std::string& value);

//! Only strip the prefix and revert the complement for negative years.
std::string convertIndexWordToDate(const std::string& indexWord);

//! Takes an integer as std::string and return the complement.
std::string getBase10ComplementOfIntegerString(const std::string& orig);

//! Remove leading zeros.
std::string removeLeadingZeros(const std::string& orig);

//! Check if this looks like an value literal
bool isXsdValue(std::string_view value);

//! Check if the given std::string is numeric, i.e. a decimal integer or float
//! string i.e. "42", "42.2", "0123", ".3" are all numeric while "a", "1F",
//! "0x32" are not
bool isNumeric(const std::string& val);

//! Converts numeric strings (as determined by isNumeric()) into index words
std::string convertNumericToIndexWord(const std::string& val);

//! Convert a language tag like "@en" to the corresponding entity uri
//! for the efficient language filter
std::string convertLangtagToEntityUri(const std::string& tag);
std::optional<std::string> convertEntityUriToLangtag(const std::string& word);
std::string convertToLanguageTaggedPredicate(const std::string& pred,
                                             const std::string& langtag);

// _____________________________________________________________________________
std::string convertValueLiteralToIndexWord(const std::string& orig);

// ____________________________________________________________________________
std::pair<std::string, const char*> convertIndexWordToLiteralAndType(
    const std::string& indexWord);

// _____________________________________________________________________________
std::string convertIndexWordToValueLiteral(const std::string& indexWord);

// _____________________________________________________________________________
std::string convertFloatStringToIndexWord(const std::string& orig,
                                          const NumericType type);

// _____________________________________________________________________________
std::string convertIndexWordToFloatString(const std::string& indexWord);

// _____________________________________________________________________________
float convertIndexWordToFloat(const std::string& indexWord);

// _____________________________________________________________________________
std::string normalizeDate(const std::string& orig);

// _____________________________________________________________________________
std::string convertDateToIndexWord(const std::string& value);

// _____________________________________________________________________________
std::string convertIndexWordToDate(const std::string& indexWord);

// _____________________________________________________________________________
std::string getBase10ComplementOfIntegerString(const std::string& orig);

// _____________________________________________________________________________
std::string removeLeadingZeros(const std::string& orig);

// _____________________________________________________________________________
bool isXsdValue(std::string_view value);

// _____________________________________________________________________________
bool isNumeric(const std::string& value);

// _____________________________________________________________________________
std::string convertNumericToIndexWord(const std::string& val);

// _________________________________________________________
std::string convertLangtagToEntityUri(const std::string& tag);

// _________________________________________________________
std::optional<std::string> convertEntityUriToLangtag(const std::string& word);
// _________________________________________________________
std::string convertToLanguageTaggedPredicate(const std::string& pred,
                                             const std::string& langtag);

}  // namespace ad_utility
