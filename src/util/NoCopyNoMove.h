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

// Two small empty base classes that express the two most common restrictions on
// the special member functions of a type: `NoCopyNoMove` forbids copying *and*
// moving, `NoCopy` forbids copying but keeps moving. Inherit from one of them
// instead of writing out the corresponding `= delete`d declarations by hand,
// and document at the derived class *why* it may not be copied (or moved),
// whenever that reason is not obvious.
//
// Both are empty classes, so deriving from them does not increase the size of
// the derived class (empty base optimization).
//
// NOTE: The destructors are deliberately `protected` and non-virtual, because
// these classes are an implementation detail of the derived class and never a
// handle through which an object is deleted.
//
// NOTE: Deriving from one of these classes adds `ad_utility` to the associated
// namespaces of the derived class, so free functions from `ad_utility` become
// visible to argument-dependent lookup for that class. This only matters for
// classes outside of `ad_utility` and shows up as an ambiguity at compile
// time.

namespace ad_utility {

// A base class for types that must neither be copied nor moved, typically
// because they hold pointers into themselves, or because other threads refer to
// them by reference.
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

// A base class for types that must not be copied, but that may be moved,
// typically because they exclusively own a resource (a file, a thread, a memory
// budget, etc.).
//
// NOTE: The move operations of this base class are defaulted and `noexcept`, so
// a derived class still gets its implicit move constructor and move assignment
// operator (unless one of its own members or one of its other base classes
// suppresses them). In other words, this class does not make a type movable,
// it only refrains from making it immovable. To forbid moving as well, use
// `NoCopyNoMove` above.
class NoCopy {
 public:
  NoCopy() = default;
  NoCopy(const NoCopy&) = delete;
  NoCopy& operator=(const NoCopy&) = delete;
  NoCopy(NoCopy&&) noexcept = default;
  NoCopy& operator=(NoCopy&&) noexcept = default;

 protected:
  ~NoCopy() = default;
};

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_NOCOPYNOMOVE_H
