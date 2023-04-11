// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (April of 2023, schlegea@informatik.uni-freiburg.de)

#pragma once

#include <memory>
#include <type_traits>
#include <utility>

#include "util/json.h"

namespace ad_utility {

/*
A version of `std::unique_ptr` with a copy assigment operator and a copy
constructor, which both copy the dereferenced pointer to create a new instace
of the object for the `unique_prt`.
Currently not written with support for dynamically-allocated array of objects
in mind, so that may not work.
*/
template<typename T, typename Deleter = std::default_delete<T>>
requires std::is_copy_constructible_v<T>
class CopyableUniquePtr: public std::unique_ptr<T, Deleter>{
  // This makes calling functions, etc. from the base class so much easier.
  using Base = std::unique_ptr<T, Deleter>;

  // This function uses a private constructor, so it needs private access.
  template<typename T2, typename... Args>
  friend constexpr CopyableUniquePtr<T2> make_copyable_unique(Args&&... args);

  /*
  @brief Returns an unique pointer, that holds a copy of the dereferenced
  (copyable) unique pointer, that was given.
  */
  Base CopyDereferencedPointer(const auto& ptr){
    // Different behaviour based on whenever the ptr actually owns an object.
    return ptr ? std::make_unique<T>(*ptr) : nullptr;
  }

  /*
  @brief Creates a `CopyableUniquePtr`, that holds the object formerly
  owned by the unique pointer. Needed for `make_copyable_unique`.
  */
  explicit CopyableUniquePtr(Base&& ptr): Base(std::move(ptr)) {}

  public:
  // Default constructor.
  CopyableUniquePtr(): Base() {}

  // Default destructor.
  ~CopyableUniquePtr() = default;

  // Copy constructor.
  CopyableUniquePtr(const CopyableUniquePtr& ptr):
  Base(CopyDereferencedPointer(ptr)) {}

  // Move constructor.
  CopyableUniquePtr(CopyableUniquePtr&&) noexcept = default;

  // Copy assignment operator.
  CopyableUniquePtr<T, Deleter>& operator=(
    const CopyableUniquePtr<T, Deleter>& ptr){
    // Special behaviour in case, that `ptr` doesn't own an object.
    Base::operator=(CopyDereferencedPointer(ptr));
    return *this;
  }

  // Json serialization.
  friend void to_json(nlohmann::json& j,
    const CopyableUniquePtr<T, Deleter>& p){
    /*
    The serialization of `CopyableUniquePtr` would have identical code to the
    serialization of a normal unique pointer, so we just re-cast it, to save on
    code duplication.
    However, unique pointers can only be created with rvalues, so we also have
    to create a temporary copy.
    */
    j = static_cast<std::unique_ptr<T, Deleter>>(
      CopyableUniquePtr<T, Deleter>{p});
  }
};

/*
@brief Same as `std::make_unique`, but for `CopyableUniquePtr`.
*/
template<typename T, typename... Args>
constexpr CopyableUniquePtr<T> make_copyable_unique(Args&&... args){
  return CopyableUniquePtr<T>(std::make_unique<T>(std::forward<Args>(args)...));
}

}  // namespace ad_utility
