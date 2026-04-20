// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Marvin Stoetzel <stoetzem@students.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gtest/gtest.h>

#include "engine/ConstructTripleInstantiator.h"
#include "engine/ConstructTypes.h"

namespace {
using namespace qlever::constructExport;

TEST(FormatTerm, dataTypeIsNull) {
  // By the definition of `EvaluatedTermData`, if `rdfTermDataType_`==`nullptr`,
  // then `rdfTermString_` is expected to already hold the value of the rdf term
  // string include the complete serialized form (including the appended
  // datatype string via ^^).
  std::string rdfTermString = "1^^xsd::integer";
  const char* rdfTermDataType = nullptr;
  EvaluatedTermData term = EvaluatedTermData{rdfTermString, rdfTermDataType};
  // Setting `includeDataType` = true instructs `formatTerm` to append the
  // datataype string (which is contained in `rdfTermDataType`) to the string
  // representing the rdf term (`rdfTermString`). But if
  // `rdfTermDataType` == nullptr, then we expect `rdfTermString` to already
  // be appended with its dataType. Thus, the value of `includeDataType` is
  // ignored.
  bool includeDataType = true;
  EXPECT_EQ("1^^xsd::integer", formatTerm(term, includeDataType));
  includeDataType = false;
  EXPECT_EQ("1^^xsd::integer", formatTerm(term, includeDataType));
}

TEST{FormatTerm, dataTypeNotNull} {
  // if `rdfTermDataType_` != null, then the `rdfTermString_` member of
  // `EvalutedTermData` object is expected to NOT already include the datatype
  // string. Then, it depends on `includeDataType` whether the datatype string
  // is appended to the rdf term string.
  std::string rdfTermString = "1^^xsd::integer";
}

}  // namespace
