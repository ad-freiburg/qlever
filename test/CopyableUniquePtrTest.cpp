// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (April of 2023,
// schlegea@informatik.uni-freiburg.de)

#include <gtest/gtest.h>

#include <memory>

#include "util/CopyableUniquePtr.h"
#include "util/json.h"

/*
@brief Check, if `CopyableUniquePtr` owns an object, that is equal to the
given object.
*/
template <typename T>
static void compareOwnedObject(const ad_utility::CopyableUniquePtr<T>& ptr,
                               const T& objectToCompareTo) {
  // Does `ptr` actually hold an object?
  ASSERT_TRUE(ptr);

  // Is the equal to the given one?
  ASSERT_EQ(*ptr, objectToCompareTo);
}

// Testing `make_copyable_unique`.
TEST(CopyableUniquePtr, make_copyable_unique) {
  {
    ad_utility::CopyableUniquePtr<int> emptyPointer{
        ad_utility::make_copyable_unique<int>()};
    ASSERT_TRUE(emptyPointer);
  }
  {
    ad_utility::CopyableUniquePtr<int> pointer{
        ad_utility::make_copyable_unique<int>(42)};
    compareOwnedObject(pointer, 42);
  }
}

// Copy and move constructor.
TEST(CopyableUniquePtr, CopyAndMoveConstructor) {
  // Copy constructor for empty object.
  {
    ad_utility::CopyableUniquePtr<int> emptyPointer;
    ad_utility::CopyableUniquePtr<int> pointerToCopyTo(emptyPointer);

    // Is `pointerToCopyTo` empty?
    ASSERT_FALSE(pointerToCopyTo);
  }

  // Copy constructor for non-empty object.
  {
    ad_utility::CopyableUniquePtr<int> nonEmptyPointer(
        ad_utility::make_copyable_unique<int>(42));
    ad_utility::CopyableUniquePtr<int> pointerToCopyTo(nonEmptyPointer);

    compareOwnedObject(pointerToCopyTo, 42);

    // Is it really a different object, that the pointer owns? In other words,
    // it it **really** a copy?
    ASSERT_NE(nonEmptyPointer.get(), pointerToCopyTo.get());
  }

  // Move constructor for empty object.
  {
    ad_utility::CopyableUniquePtr<int> emptyPointer;
    ad_utility::CopyableUniquePtr<int> pointerToMoveTo(std::move(emptyPointer));

    // Is `pointerToMoveTo` empty?
    ASSERT_FALSE(pointerToMoveTo);
  }

  // Move constructor for non-empty object.
  {
    ad_utility::CopyableUniquePtr<int> nonEmptyPointer(
        ad_utility::make_copyable_unique<int>(42));
    // Saving the adress of the int object, so that we can later check, that
    // it was actually moved.
    int* const intAdress = nonEmptyPointer.get();

    ad_utility::CopyableUniquePtr<int> pointerToMoveTo(
        std::move(nonEmptyPointer));

    // Does `pointerToMoveTo` own the correct object?
    compareOwnedObject(pointerToMoveTo, 42);
    // Did `nonEmptyPointer` lose its object?
    ASSERT_FALSE(nonEmptyPointer);
    // Is it really the same object as before, that `pointerToMoveTo` now owns?
    ASSERT_EQ(intAdress, pointerToMoveTo.get());
  }
}

// Copy assignment operator.
TEST(CopyableUniquePtr, CopyAssignmentOperator) {
  ad_utility::CopyableUniquePtr<int> intPointer;
  const ad_utility::CopyableUniquePtr<int> fortyTwoPointer{
      ad_utility::make_copyable_unique<int>(42)};
  const ad_utility::CopyableUniquePtr<int> sixPointer{
      ad_utility::make_copyable_unique<int>(6)};

  // Quick check, if both pointer have equal dereferenced objects, that are
  // however not the same object.
  auto check = [](const auto& pointer1, const auto& pointer2) {
    // Both own an object.
    ASSERT_TRUE(pointer1 && pointer2);
    // The owned objects count as equal.
    ASSERT_EQ(*pointer1, *pointer2);
    // The pointers don't own the same object.
    ASSERT_NE(pointer1.get(), pointer2.get());
  };

  // Replacing default `nullptr`.
  intPointer = fortyTwoPointer;
  check(intPointer, fortyTwoPointer);

  // Replacing a non default object.
  intPointer = sixPointer;
  check(intPointer, sixPointer);
}

// Json serialization.
TEST(CopyableUniquePtr, jsonSerialization) {
  // Does an empty `CopyableUniquePtr` serialize as `null`?
  nlohmann::json j = ad_utility::CopyableUniquePtr<int>{};
  ASSERT_TRUE(j.is_null());

  // Does a not empty `CopyableUniquePtr` serialize correctly?
  j = ad_utility::make_copyable_unique<int>(42);
  ASSERT_EQ(j.get<int>(), 42);
}
