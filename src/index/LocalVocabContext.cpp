// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "index/LocalVocabContext.h"

// The destructor is defined out of line, such that the vtable is emitted in
// exactly one translation unit.
LocalVocabContext::~LocalVocabContext() = default;
