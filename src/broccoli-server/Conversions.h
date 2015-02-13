// Copyright 2011, University of Freiburg, Chair of Algorithms and Data
// Structures.
// Author: Björn Buchhold <buchholb>

#ifndef  GLOBALS_CONVERSIONS_H_
#define  GLOBALS_CONVERSIONS_H_

#include <stdlib.h>
#include <assert.h>
#include <sstream>
#include <string>
#include <vector>
#include <unordered_map>
#include <unordered_set>

#include "../util/StringUtils.h"
#include "./Globals.h"

using std::vector;
using std::string;
using std::unordered_map;
using std::unordered_set;

namespace ad_semsearch {
//! Splits a tab separated triple (terminated with tab-dot), strips the dot
//! and checks that there are exactly three non-empty columns.
//! Returns true iff the triple is valid.
bool splitAndCheckTurtle(const string& strTriple, vector<string>* result);

//! Splits a tab separated triple (terminated with tab-dot).
//! Relies on well-formed input.
//! If there are problems, better use splitAndCheckTurtle.
void splitTurtleUncheckedButFast(const string& strTriple,
    vector<string>* result);

//! Conversion methods that add required prefixes and perform all necessary
//! conversions. The exact rules may be subject to changes.
string subjectToBroccoliStyle(const string& orig);
string predicateToBroccoliStyle(const string& orig);
string objectToBroccoliStyle(const string& orig);

//! Entify the string (replac spaces etc. with underscore)
//! and make first character uppercase.
string entifyAndFirstCharToUpper(const string& orig);

//! Convert an ontology value to an index word.
//! Ontology values have a prefix and a readable format apart form that.
//! IndexWords are not necessarily readable but lexicographical
//! comparison should yield the same ordering that one would expect from
//! a natural ordering of the values invloved.
string convertOntologyValueToIndexWord(const string& orig);

//! Converts a datetime value like "1990-10-10T13:10:14.000" of freebbase
//! to the index word representation. Note that the xsd definition
//! suffix ("^^<http...") must be removed from the input.
//! Note that freebase datetimes are somewhat non-standard.
//! This method currently works on what freebase gives us rather than standards.
string convertFreebaseDateTimeValueToIndexWord(const string& value);

//! Converts a value like "94.0"^^<http //www.w3.org/2001/XMLSchema#float>
//! to the unreadable index word representation of that value.
string convertFreebaseXsdValueToIndexWord(const string& orig);

//! Convert an index word to an ontology value.
//! Ontology values have a prefix and a readable format apart form that.
//! IndexWords are not necessarily readable but lexicographical
//! comparison should yield the same ordering that one would expect from
//! a natural ordering of the values invloved.
string convertIndexWordToOntologyValue(const string& indexWord);

//! Decode an URL, i.e. %20 -> whitespace and so on.
void decodeUrl(const string& url, string* decoded);

//! Converts like this: "1234 to P0*1234 and -1234 to M9*8765".
string convertOntologyIntegerToIndexWord(const string& ontologyInteger,
    size_t nofDigits);
//! Converts like this: "P0*1234 to 1234 and M9*8765 to -1234".
string convertIndexWordToOntologyInteger(const string& indexWord);
//! Converts like this: "12.34 to PP0*2E0*1234 and -0.123 to M-0*1E9*876".
string convertOntologyFloatToIndexWord(const string& ontologyFloat,
    size_t nofExponentDigits, size_t nofMantissaDigits);
//! Converts like this: "PP0*2E0*1234 to 12.34 and M-0*1E9*876 to -0.123".
string convertIndexWordToOntologyFloat(const string& indexWord);
//! Currently no conversion performed at all, may be changed in the future.
inline string convertOntologyDateToIndexWord(const string& ontologyDate);
//! Currently no conversion performed at all, may be cahnged in the future.
inline string convertIndexWordToOntologyDate(const string& indexWord);
//! Takes an integer as string and return the complement.
inline string getBase10ComplementOfIntegerString(const string& orig);
//! Converts a WIkipedia URL to Broccoli entity name. Note, that the
//! entity name doesn't have to be a real entity, because it may be that
//! it is not in the ontology.
inline string wikiUrlToEntityName(const string& wikiUrl);
//! Replaces colons and whitespaces by underscores,
//! makes the first char uppercase.
inline string entify(const string& orig);


// *****************************************************************************
// Definitions:
// *****************************************************************************

// ____________________________________________________________________________
string convertOntologyDateToIndexWord(const string& ontologyDate) {
  size_t prefixLength = std::char_traits<char>::length(
      ad_semsearch::VALUE_DATE_PREFIX);
  if (ontologyDate[prefixLength] == '-') {
    std::ostringstream os;
    os << ad_semsearch::VALUE_DATE_PREFIX;
    os << '-' << getBase10ComplementOfIntegerString(
            ontologyDate.substr(prefixLength + 1,
                DEFAULT_NOF_DATE_YEAR_DIGITS - 1));
    os << ontologyDate.substr(prefixLength + DEFAULT_NOF_DATE_YEAR_DIGITS);
    return os.str();
  } else {
    return ontologyDate;
  }
}
// ____________________________________________________________________________
string convertIndexWordToOntologyDate(const string& indexWord) {
  // Call convertOntologyDateToIndexWord, which currently performs
  // the same transformation (compute the base-10 complement of the year part).
  return convertOntologyDateToIndexWord(indexWord);
}
// ____________________________________________________________________________
string getBase10ComplementOfIntegerString(const string& orig) {
  std::ostringstream os;
  for (size_t i = 0; i < orig.size(); ++i) {
    os << (9 - atoi(orig.substr(i, 1).c_str()));
  }
  return os.str();
}
// ____________________________________________________________________________
string wikiUrlToEntityName(const string& wikiUrl) {
  string title = wikiUrl.substr(wikiUrl.find_last_of('/') + 1);
  return string(ENTITY_PREFIX) + ad_utility::getLowercaseUtf8(title)
      + ':' + title;
}
// ____________________________________________________________________________
string entify(const string& orig) {
  std::ostringstream os;
  for (size_t i = 0; i < orig.size(); ++i) {
    if (orig[i] == ':' || orig[i] == ' ') {
      os << '_';
    } else {
      os << orig[i];
    }
  }
  return os.str();
}
}
#endif  // GLOBALS_CONVERSIONS_H_
