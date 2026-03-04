// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Christoph Ullinger <ullingec@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#ifndef QLEVER_TEST_PRINTERS_LOCALVOCABENTRYPRINTERS_H
#define QLEVER_TEST_PRINTERS_LOCALVOCABENTRYPRINTERS_H

#include <ostream>

#include "index/LocalVocabEntry.h"

// _____________________________________________________________________________
inline void PrintTo(const LocalVocabEntry& word, std::ostream* os) {
  auto& s = *os;
  s << word.toStringRepresentation();
}

#endif  // QLEVER_TEST_PRINTERS_LOCALVOCABENTRYPRINTERS_H
