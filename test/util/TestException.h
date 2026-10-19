// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Robin Textor-Falconi <textorr@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_TEST_UTIL_TESTEXCEPTION_H
#define QLEVER_TEST_UTIL_TESTEXCEPTION_H

#include <exception>

// An exception for tests that pass exceptions between threads. Its message is
// a string literal, so that it owns no memory besides the exception object
// itself. The freeing of a message that is owned by the exception (e.g. by a
// `std::runtime_error`) would be reported as a false positive by TSAN, see
// `misc/tsan-suppressions.txt`.
class TestException : public std::exception {
 private:
  const char* message_;

 public:
  explicit TestException(const char* message) : message_{message} {}
  const char* what() const noexcept override { return message_; }
};

#endif  // QLEVER_TEST_UTIL_TESTEXCEPTION_H
