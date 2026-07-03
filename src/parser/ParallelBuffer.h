// Copyright 2021 - 2026 The QLever Authors, in particular:
//
// 2021 - 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR

// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

// Compatibility header. `ParallelBuffer`, `ParallelFileBuffer`, and
// `ParallelBufferWithEndRegex` are now type aliases for the corresponding
// async block-source types in `parser/AsyncBlockSource.h`. All implementation
// is in `AsyncBlockSource.cpp`; `ParallelBuffer.cpp` has been removed.

#ifndef QLEVER_SRC_PARSER_PARALLELBUFFER_H
#define QLEVER_SRC_PARSER_PARALLELBUFFER_H

#include "parser/AsyncBlockSource.h"

using ParallelBuffer = qlever::parser::AsyncBlockSource;
using ParallelFileBuffer = qlever::parser::AsyncFileBlockSource;
using ParallelBufferWithEndRegex = qlever::parser::AsyncEndRegexBlockSource;

#endif  // QLEVER_SRC_PARSER_PARALLELBUFFER_H
