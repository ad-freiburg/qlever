// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_OWNEDORVIEWEDVECTOR_H
#define QLEVER_SRC_UTIL_OWNEDORVIEWEDVECTOR_H

#include <absl/cleanup/cleanup.h>

#include <cstddef>
#include <utility>
#include <variant>
#include <vector>

#include "backports/concepts.h"
#include "backports/span.h"
#include "util/Exception.h"
#include "util/Forward.h"
#include "util/Serializer/SerializeVector.h"
#include "util/Serializer/Serializer.h"

namespace ad_utility {

// A contiguous, read-only array of `T`s, which either owns its elements (as a
// `std::vector<T>`), or is a non-owning view into memory that is owned by
// someone else (typically the buffer of a serializer from which it was read
// via `fromZeroCopyDeserializer`, in which case that buffer has to outlive this
// object).
//
// In addition to the owned-or-viewed storage, a `ql::span` of the current
// elements is stored unconditionally, so that the (typically hot) read access
// via `view()`, `operator[]`, etc. never has to dispatch on whether the
// elements are owned. The owned elements can only be changed via `modify`,
// which keeps that span in sync.
//
// This type is move-only: A defaulted copy would make the span of the copy
// point into the elements of the original. To copy, use `clone()`.
//
// NOTE: There deliberately is no variant of this class for
// `std::string`/`std::string_view`: Because of the small string optimization,
// moving a `std::string` can change its `data()`, which would require a
// different handling of the span on moves.
template <typename T>
class OwnedOrViewedVector {
 public:
  using value_type = T;
  using Vector = std::vector<T>;
  using View = ql::span<const T>;

 private:
  std::variant<Vector, View> storage_;
  // Always a view of the elements of `storage_`, no matter which alternative
  // it currently holds.
  View view_;

 public:
  // Create an empty array that owns its (zero) elements.
  OwnedOrViewedVector() = default;

  // Create an array that owns the given `elements`.
  explicit OwnedOrViewedVector(Vector elements)
      : storage_{std::move(elements)}, view_{computeView()} {}

  // Create an array that is a non-owning view of the given `elements`, which
  // have to outlive this object.
  explicit OwnedOrViewedVector(View elements)
      : storage_{elements}, view_{elements} {}

  // Move-only, see the class comment. A moved-from object is empty and owns
  // its (zero) elements. The `view_` is recomputed explicitly (instead of
  // relying on a moved `std::vector` keeping its buffer).
  OwnedOrViewedVector(OwnedOrViewedVector&& other) noexcept
      : storage_{std::move(other.storage_)}, view_{computeView()} {
    other.reset();
  }
  OwnedOrViewedVector& operator=(OwnedOrViewedVector&& other) noexcept {
    if (this != &other) {
      storage_ = std::move(other.storage_);
      view_ = computeView();
      other.reset();
    }
    return *this;
  }
  OwnedOrViewedVector(const OwnedOrViewedVector&) = delete;
  OwnedOrViewedVector& operator=(const OwnedOrViewedVector&) = delete;

  // The move operations above are only user-defined to keep `view_` in sync,
  // there is no resource to clean up: The owned `std::vector` frees its
  // elements itself, and a view owns nothing.
  ~OwnedOrViewedVector() = default;

  // Create an array that is a non-owning, zero-copy view directly into the
  // buffer of `serializer`, which must support zero-copy deserialization (see
  // `ZeroCopyReadSerializer` in `util/Serializer/Serializer.h`). The returned
  // object is only valid as long as the memory backing `serializer`'s buffer
  // is valid and unchanged. The layout that is read here is the one that is
  // written by the serialization below (which is the same as the one of a
  // `std::vector<T>` or `ql::span<const T>`).
  CPP_template(typename S)(requires serialization::ZeroCopyReadSerializer<
                           S>) static OwnedOrViewedVector
      fromZeroCopyDeserializer(S& serializer) {
    return OwnedOrViewedVector{
        serialization::zeroCopyDeserializeToSpan<T>(serializer)};
  }

  // Read-only access to the elements, regardless of whether they are owned.
  View view() const { return view_; }
  size_t size() const { return view_.size(); }
  bool empty() const { return view_.empty(); }
  const T& operator[](size_t i) const { return view_[i]; }
  const T& back() const { return view_.back(); }
  auto begin() const { return view_.begin(); }
  auto end() const { return view_.end(); }

  // Return a copy of this array that always owns its elements (even if this
  // array is a non-owning view).
  OwnedOrViewedVector clone() const {
    return OwnedOrViewedVector{Vector(view_.begin(), view_.end())};
  }

  // Return true iff the elements are owned (and can thus be changed via
  // `modify`).
  bool isOwned() const { return std::holds_alternative<Vector>(storage_); }

  // Call `function` with a reference to the owned `std::vector` of elements,
  // which it may change arbitrarily, and return its result. Throw if the
  // elements are not owned (a view is read-only). The internal span is
  // updated afterwards, also if `function` throws.
  template <typename F>
  decltype(auto) modify(F&& function) {
    AD_CONTRACT_CHECK(isOwned(),
                      "An `OwnedOrViewedVector` that is a non-owning view "
                      "cannot be modified");
    absl::Cleanup updateView{[this] { view_ = computeView(); }};
    return AD_FWD(function)(std::get<Vector>(storage_));
  }

  // Serialization in the same format as a `std::vector<T>`. Reading always
  // produces an object that owns its elements; use `fromZeroCopyDeserializer`
  // to obtain a non-owning, zero-copy view.
  AD_SERIALIZE_FRIEND_FUNCTION(OwnedOrViewedVector) {
    if constexpr (serialization::WriteSerializer<S>) {
      serializer << arg.view();
    } else {
      Vector elements;
      serializer >> elements;
      arg = OwnedOrViewedVector{std::move(elements)};
    }
  }

 private:
  // Return a view of the elements of `storage_`.
  View computeView() const {
    return std::visit(
        [](const auto& x) -> View { return {x.data(), x.size()}; }, storage_);
  }

  // Make this object empty and owning.
  void reset() {
    storage_.template emplace<Vector>();
    view_ = View{};
  }
};

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_OWNEDORVIEWEDVECTOR_H
