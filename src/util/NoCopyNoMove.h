// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_NOCOPYNOMOVE_H
#define QLEVER_SRC_UTIL_NOCOPYNOMOVE_H

namespace ad_utility {

// A base class for types that must neither be copied nor moved, typically
// because they hold pointers into themselves, or because other threads refer to
// them by reference. Inherit from this instead of deleting all four of the
// copy/move special member functions by hand, and document at the derived class
// *why* it may not be copied or moved.
//
// NOTE: The destructor is deliberately `protected` and non-virtual, because
// this class is an implementation detail of the derived class and never a
// handle through which an object is deleted.
class NoCopyNoMove {
 public:
  NoCopyNoMove() = default;
  NoCopyNoMove(const NoCopyNoMove&) = delete;
  NoCopyNoMove& operator=(const NoCopyNoMove&) = delete;
  NoCopyNoMove(NoCopyNoMove&&) = delete;
  NoCopyNoMove& operator=(NoCopyNoMove&&) = delete;

 protected:
  ~NoCopyNoMove() = default;
};

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_NOCOPYNOMOVE_H
