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
// moving, `NoCopy` forbids copying but leaves moving supported. Inherit from
// one of them instead of writing out the corresponding `= delete`d and
// `= default`ed declarations by hand, and possibly document at the derived
// class *why* it may not be copied (or moved), whenever that reason is not
// obvious.
//
// Both are empty classes, so deriving from them does not increase the size of
// the derived class (empty base optimization).
//
// NOTE: The destructors are deliberately `protected` and non-virtual, because
// these classes are an implementation detail of the derived class and never a
// handle through which an object is deleted.
//
// NOTE: Deriving from a class adds the innermost enclosing namespace of that
// base class to the associated namespaces of the derived class, so all free
// functions from that namespace become visible to argument-dependent lookup for
// the derived class. To keep the (large) `ad_utility` namespace out of the
// argument-dependent lookup of every derived class, the actual classes live in
// the dedicated namespace `ad_utility::detail::noCopyNoMove` and are only
// exposed to `ad_utility` via the aliases below. An alias is not an associated
// entity, so only the small detail namespace (which deliberately contains
// nothing but these two classes) is added to the associated namespaces. For a
// test of this behavior see `NoCopyNoMoveTest.cpp`.

namespace ad_utility::detail::noCopyNoMove {

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

}  // namespace ad_utility::detail::noCopyNoMove

namespace ad_utility {

// The aliases through which the two base classes above are used. See the second
// NOTE at the top of this file for why these are aliases and not the classes
// themselves.
using NoCopy = detail::noCopyNoMove::NoCopy;
using NoCopyNoMove = detail::noCopyNoMove::NoCopyNoMove;

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_NOCOPYNOMOVE_H
