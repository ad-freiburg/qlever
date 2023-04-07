// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (April of 2023, schlegea@informatik.uni-freiburg.de)

#pragma once

#include <memory>
#include <type_traits>

#include "util/json.h"

/*
A version of `std::unique_ptr` with a copy assigment operator and a copy
constructor, which both copy the dereferenced pointer to create a new instace
of the object for the `unique_prt`.
*/
template<typename T, typename Deleter = std::default_delete<T>>
requires std::is_copy_constructible_v<T>
class CopybaleUniquePtr: public std::unique_ptr<T, Deleter>{
  public:
  // This makes calling functions, etc. from the base class so much easier.
  using Base = std::unique_ptr<T, Deleter>;

  // Default constructor.
  CopybaleUniquePtr(): Base() {}

  /*
  @brief Creates a `CopybaleUniquePtr`, that holds a copy of the object owned
  by the given unique pointer.
  */
  explicit CopybaleUniquePtr(const Base& ptr): Base(std::make_unique<T>(*ptr))
  {}

  /*
  @brief Creates a `CopybaleUniquePtr`, that holds the object formerly
  owned by the unique pointer.
  */
  explicit CopybaleUniquePtr(Base&& ptr): Base(std::move(ptr)) {}

  // Copy constructor.
  // Special behaviour only needed, if `ptr` doesn't own an object.
  CopybaleUniquePtr(const CopybaleUniquePtr& ptr):
  Base(ptr ? std::make_unique<T>(*ptr) : nullptr) {}

  // Move constructor.
  CopybaleUniquePtr(CopybaleUniquePtr&&) noexcept = default;

  // Copy assignment operator.
  CopybaleUniquePtr<T, Deleter>& operator=(
    const CopybaleUniquePtr<T, Deleter>& ptr){
    // Special behaviour in case, that `ptr` doesn't own an object.
    if (ptr){
      return Base::operator=(std::make_unique<T>(*ptr));
    } else {
      return Base::operator=(nullptr);
    }
  }

  // Json serialization.
  friend void to_json(nlohmann::json& j,
    const CopybaleUniquePtr<T, Deleter>& p){
    /*
    The serialization of `CopybaleUniquePtr` would have identical code to the
    serialization of a normal unique pointer, so we just re-cast it, to save on
    code duplication.
    However, unique pointers can only be created with rvalues, so we also have
    to create a temporary copy.
    */
    j = static_cast<std::unique_ptr<T, Deleter>>(
      CopybaleUniquePtr<T, Deleter>{p});
  }
};

