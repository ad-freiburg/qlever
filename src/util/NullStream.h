// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_NULLSTREAM_H
#define QLEVER_SRC_UTIL_NULLSTREAM_H

#include <ostream>
#include <streambuf>

namespace ad_utility {

// An output stream that silently discards everything written to it. Useful for
// example to suppress optional logging output without changing call sites.
class NullStream : public std::ostream {
 private:
  class NullBuffer : public std::streambuf {
   public:
    int overflow(int c) override { return c; }
  };
  NullBuffer buffer_;

 public:
  NullStream() : std::ostream(&buffer_) {}
};

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_NULLSTREAM_H
