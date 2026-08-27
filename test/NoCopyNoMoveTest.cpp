// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gtest/gtest.h>

#include <memory>
#include <type_traits>
#include <utility>

#include "util/NoCopyNoMove.h"

using ad_utility::NoCopy;
using ad_utility::NoCopyNoMove;

namespace {

// A simple type that is copyable and movable, used as a member of the types
// below to make sure that the special member functions are only restricted by
// the respective base class.
struct Payload {
  int value_ = 0;
};

// A type that is neither copyable nor movable by itself, used to check that the
// bases don't accidentally *enable* copying or moving.
struct ImmovableMember {
  ImmovableMember() = default;
  ImmovableMember(const ImmovableMember&) = delete;
  ImmovableMember& operator=(const ImmovableMember&) = delete;
};

// The derived types under test.
struct DerivedNoCopyNoMove : NoCopyNoMove {
  Payload payload_;
};
struct DerivedNoCopy : NoCopy {
  Payload payload_;
};
struct DerivedNoCopyWithImmovableMember : NoCopy {
  ImmovableMember member_;
};

// The same types without the base class, for the `sizeof` comparison below.
struct PlainPayload {
  Payload payload_;
};

// A move-only type that empties itself when moved from, to check that a
// `NoCopy`-derived type really moves its members.
struct MoveOnlyPointer : NoCopy {
  std::unique_ptr<int> pointer_ = std::make_unique<int>(42);
};

// A `NoCopy`-derived type with a user-declared destructor. Such a destructor
// suppresses the *implicit* move operations of the derived class, see the test
// `AUserDeclaredDestructorSuppressesTheImplicitMoveOperations` below.
struct DerivedNoCopyWithDestructor : NoCopy {
  Payload payload_;
  ~DerivedNoCopyWithDestructor() = default;
};

// The same type, but with the move operations declared explicitly, which is
// what a `NoCopy`-derived type with a destructor has to do to stay movable.
struct DerivedNoCopyWithDestructorAndExplicitMoves : NoCopy {
  Payload payload_;
  ~DerivedNoCopyWithDestructorAndExplicitMoves() = default;
  DerivedNoCopyWithDestructorAndExplicitMoves(
      DerivedNoCopyWithDestructorAndExplicitMoves&&) = default;
  DerivedNoCopyWithDestructorAndExplicitMoves& operator=(
      DerivedNoCopyWithDestructorAndExplicitMoves&&) = default;
};

}  // namespace

// _____________________________________________________________________________
TEST(NoCopyNoMove, DerivedTypeIsNeitherCopyableNorMovable) {
  static_assert(!std::is_copy_constructible_v<DerivedNoCopyNoMove>);
  static_assert(!std::is_copy_assignable_v<DerivedNoCopyNoMove>);
  static_assert(!std::is_move_constructible_v<DerivedNoCopyNoMove>);
  static_assert(!std::is_move_assignable_v<DerivedNoCopyNoMove>);
  static_assert(std::is_default_constructible_v<DerivedNoCopyNoMove>);

  // The default constructor still works and the members behave normally.
  DerivedNoCopyNoMove derived;
  derived.payload_.value_ = 17;
  EXPECT_EQ(derived.payload_.value_, 17);
}

// _____________________________________________________________________________
TEST(NoCopy, DerivedTypeIsMovableButNotCopyable) {
  static_assert(!std::is_copy_constructible_v<DerivedNoCopy>);
  static_assert(!std::is_copy_assignable_v<DerivedNoCopy>);
  static_assert(std::is_move_constructible_v<DerivedNoCopy>);
  static_assert(std::is_move_assignable_v<DerivedNoCopy>);
  static_assert(std::is_nothrow_move_constructible_v<DerivedNoCopy>);
  static_assert(std::is_nothrow_move_assignable_v<DerivedNoCopy>);
  static_assert(std::is_default_constructible_v<DerivedNoCopy>);
}

// _____________________________________________________________________________
TEST(NoCopy, MovingActuallyMovesTheMembers) {
  MoveOnlyPointer pointer;
  ASSERT_NE(pointer.pointer_, nullptr);
  EXPECT_EQ(*pointer.pointer_, 42);

  // Move construction transfers the ownership of the pointer.
  MoveOnlyPointer moveConstructed{std::move(pointer)};
  EXPECT_EQ(pointer.pointer_, nullptr);
  ASSERT_NE(moveConstructed.pointer_, nullptr);
  EXPECT_EQ(*moveConstructed.pointer_, 42);

  // The same holds for move assignment.
  MoveOnlyPointer moveAssigned;
  moveAssigned = std::move(moveConstructed);
  EXPECT_EQ(moveConstructed.pointer_, nullptr);
  ASSERT_NE(moveAssigned.pointer_, nullptr);
  EXPECT_EQ(*moveAssigned.pointer_, 42);
}

// _____________________________________________________________________________
TEST(NoCopy, ImmovableMembersStillMakeTheDerivedTypeImmovable) {
  // `NoCopy` does not suppress the implicit move operations, but it also
  // doesn't enable them if a member (or another base class) suppresses them.
  static_assert(
      !std::is_copy_constructible_v<DerivedNoCopyWithImmovableMember>);
  static_assert(!std::is_copy_assignable_v<DerivedNoCopyWithImmovableMember>);
  static_assert(
      !std::is_move_constructible_v<DerivedNoCopyWithImmovableMember>);
  static_assert(!std::is_move_assignable_v<DerivedNoCopyWithImmovableMember>);
}

// _____________________________________________________________________________
TEST(NoCopyNoMove, TheEmptyBaseClassesCostNothing) {
  // Both base classes are empty, so the empty base optimization applies and a
  // derived class has exactly the size it would have without the base.
  static_assert(std::is_empty_v<NoCopy>);
  static_assert(std::is_empty_v<NoCopyNoMove>);
  static_assert(sizeof(DerivedNoCopy) == sizeof(PlainPayload));
  static_assert(sizeof(DerivedNoCopyNoMove) == sizeof(PlainPayload));
  EXPECT_EQ(sizeof(DerivedNoCopy), sizeof(PlainPayload));
  EXPECT_EQ(sizeof(DerivedNoCopyNoMove), sizeof(PlainPayload));
}

// _____________________________________________________________________________
TEST(NoCopy, AUserDeclaredDestructorSuppressesTheImplicitMoveOperations) {
  // A user-declared destructor (even a defaulted one) suppresses the implicit
  // move operations of a class. As the copy operations are deleted by `NoCopy`,
  // the derived class then becomes immovable, which is typically not intended.
  static_assert(!std::is_move_constructible_v<DerivedNoCopyWithDestructor>);
  static_assert(!std::is_move_assignable_v<DerivedNoCopyWithDestructor>);

  // A `NoCopy`-derived class with a destructor therefore has to declare its
  // move operations explicitly to stay movable.
  static_assert(std::is_move_constructible_v<
                DerivedNoCopyWithDestructorAndExplicitMoves>);
  static_assert(
      std::is_move_assignable_v<DerivedNoCopyWithDestructorAndExplicitMoves>);
  static_assert(!std::is_copy_constructible_v<
                DerivedNoCopyWithDestructorAndExplicitMoves>);
  static_assert(
      !std::is_copy_assignable_v<DerivedNoCopyWithDestructorAndExplicitMoves>);
}

// _____________________________________________________________________________
TEST(NoCopyNoMove, TheBaseClassesCannotBeDestroyedFromTheOutside) {
  // The destructors of the base classes are `protected`, so an object can never
  // be deleted through a pointer or reference to one of them. The derived
  // classes are of course still destructible.
  static_assert(!std::is_destructible_v<NoCopy>);
  static_assert(!std::is_destructible_v<NoCopyNoMove>);
  static_assert(std::is_destructible_v<DerivedNoCopy>);
  static_assert(std::is_destructible_v<DerivedNoCopyNoMove>);
}
