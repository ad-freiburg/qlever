// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR

// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_COMPACTSTRINGVECTOR_H
#define QLEVER_SRC_UTIL_COMPACTSTRINGVECTOR_H

#include <cstdint>
#include <string>
#include <variant>
#include <vector>

#include "backports/concepts.h"
#include "backports/span.h"
#include "util/Exception.h"
#include "util/ExceptionHandling.h"
#include "util/File.h"
#include "util/Iterators.h"
#include "util/ResetWhenMoved.h"
#include "util/Serializer/FileSerializer.h"
#include "util/Serializer/SerializeVector.h"
#include "util/TypeTraits.h"

namespace detail {
template <typename DataT>
struct CompactStringVectorWriter;

}

/**
 * @brief Stores a list of variable length data of a single type (e.g.
 *        c-style strings). The data is stored in a single contiguous block
 *        of memory.
 */
template <typename data_type>
class CompactVectorOfStrings {
 public:
  using offset_type = uint64_t;
  using value_type =
      std::conditional_t<std::is_same_v<data_type, char>, std::string_view,
                         ql::span<const data_type>>;
  using vector_type = std::conditional_t<std::is_same_v<data_type, char>,
                                         std::string, std::vector<data_type>>;

  using Writer = detail::CompactStringVectorWriter<data_type>;

 private:
  // The two kinds of storage for the data and the offsets, respectively: an
  // owned vector (after `build()`, or after reading from a regular,
  // non-zero-copy serializer), or a non-owning view into externally-owned
  // memory (after `fromZeroCopyDeserializer`).
  using DataStorage = std::vector<data_type>;
  using OffsetStorage = std::vector<offset_type>;
  using DataView = ql::span<const data_type>;
  using OffsetView = ql::span<const offset_type>;

  std::variant<DataStorage, DataView> data_;
  std::variant<OffsetStorage, OffsetView> offsets_;

 public:
  CompactVectorOfStrings() = default;

  // Build a `CompactVectorOfStrings` as a non-owning, zero-copy view directly
  // into the buffer of `serializer`, which must support zero-copy
  // deserialization (see `ZeroCopyReadSerializer` in
  // `util/Serializer/Serializer.h`). The returned object is only valid as
  // long as the memory backing `serializer`'s buffer is valid and unchanged.
  CPP_template(typename S)(
      requires ad_utility::serialization::ZeroCopyReadSerializer<
          S>) static CompactVectorOfStrings
      fromZeroCopyDeserializer(S& serializer) {
    using namespace ad_utility::serialization;
    CompactVectorOfStrings result;
    result.data_ = zeroCopyDeserializeToSpan<data_type>(serializer);
    result.offsets_ = zeroCopyDeserializeToSpan<offset_type>(serializer);
    return result;
  }

  explicit CompactVectorOfStrings(
      const std::vector<std::vector<data_type>>& input) {
    build(input);
  }

  void clear() { *this = CompactVectorOfStrings{}; }

  virtual ~CompactVectorOfStrings() = default;

  // Append the elements from `input` to this `CompactVectorOfStrings`. Only
  // allowed if this object currently owns its storage (i.e. was not created
  // via `fromZeroCopyDeserializer`).
  CPP_template(typename T)(
      requires ql::ranges::forward_range<T>&& ql::ranges::sized_range<T>&&
          ql::ranges::sized_range<ql::ranges::range_value_t<T>>&& ad_utility::
              SimilarTo<ql::ranges::range_value_t<ql::ranges::range_value_t<T>>,
                        data_type>) void build(const T& input) {
    auto& offsets = ownedOffsets();
    auto& data = ownedData();
    // Also make room for the end offset of the last element.
    offsets.reserve(input.size() + 1);
    size_t dataSize = 0;
    for (const auto& element : input) {
      offsets.push_back(dataSize);
      dataSize += element.size();
    }
    // The last offset is the offset right after the last element.
    offsets.push_back(dataSize);

    data.reserve(dataSize);

    for (const auto& el : input) {
      data.insert(data.end(), el.begin(), el.end());
    }
  }

  // This is a move-only type.
  CompactVectorOfStrings& operator=(const CompactVectorOfStrings&) = delete;
  CompactVectorOfStrings& operator=(CompactVectorOfStrings&&) noexcept =
      default;
  CompactVectorOfStrings(const CompactVectorOfStrings&) = delete;
  CompactVectorOfStrings(CompactVectorOfStrings&&) noexcept = default;

  // There is one more offset than the number of elements.
  size_t size() const { return ready() ? offsetsSpan().size() - 1 : 0; }

  bool ready() const { return !offsetsSpan().empty(); }

  /**
   * @brief operator []
   * @param i
   * @return A `value_type` containing a pointer to the data, and the number of
   *         elements stored at the pointers target.
   */
  const value_type operator[](size_t i) const {
    auto offsets = offsetsSpan();
    offset_type offset = offsets[i];
    const data_type* ptr = dataSpan().data() + offset;
    size_t size = offsets[i + 1] - offset;
    return {ptr, size};
  }

  // Copy this class and apply the transformation `mappingFunction` to its
  // elements. The result always owns its storage.
  CPP_template(typename Func)(
      requires ad_utility::InvocableWithSimilarReturnType<Func, data_type,
                                                          data_type>)
      CompactVectorOfStrings cloneAndRemap(Func mappingFunction) const {
    CompactVectorOfStrings clone;
    clone.offsets_ = ::ranges::to_vector(offsetsSpan());
    clone.data_ = ::ranges::to_vector(
        dataSpan() | ql::views::transform(std::move(mappingFunction)));
    return clone;
  }

  using Iterator = ad_utility::IteratorForAccessOperator<
      CompactVectorOfStrings, ad_utility::AccessViaBracketOperator,
      ad_utility::IsConst::True>;

  Iterator begin() const { return {this, 0}; }
  Iterator end() const { return {this, size()}; }

  using const_iterator = Iterator;

  // Allow serialization via the ad_utility::serialization interface. Note:
  // Reading always produces an object that owns its storage; use
  // `fromZeroCopyDeserializer` to obtain a non-owning, zero-copy view.
  AD_SERIALIZE_FRIEND_FUNCTION(CompactVectorOfStrings) {
    if constexpr (ad_utility::serialization::WriteSerializer<S>) {
      serializer << arg.dataSpan();
      serializer << arg.offsetsSpan();
    } else {
      auto& data = arg.data_.template emplace<DataStorage>();
      auto& offsets = arg.offsets_.template emplace<OffsetStorage>();
      serializer | data;
      serializer | offsets;
    }
  }

 private:
  // Return a read-only view of the data, regardless of whether the storage
  // currently owns its elements or is a non-owning view.
  DataView dataSpan() const {
    return std::visit(
        [](const auto& x) -> DataView { return {x.data(), x.size()}; }, data_);
  }

  // Return a read-only view of the offsets, regardless of whether the
  // storage currently owns its elements or is a non-owning view.
  OffsetView offsetsSpan() const {
    return std::visit(
        [](const auto& x) -> OffsetView { return {x.data(), x.size()}; },
        offsets_);
  }

  // Access the owned vector alternatives. Throws (via `std::get`) if this
  // object is currently a non-owning view, which is a programming error (a
  // zero-copy view is read-only, so `build()` must not be called on it).
  DataStorage& ownedData() { return std::get<DataStorage>(data_); }
  OffsetStorage& ownedOffsets() { return std::get<OffsetStorage>(offsets_); }
};

namespace detail {
// Allows the incremental writing of a `CompactVectorOfStrings` directly to a
// file.
template <typename data_type>
struct CompactStringVectorWriter {
 private:
  using offset_type = typename CompactVectorOfStrings<data_type>::offset_type;

  // The data members are encapsulated in a separate struct to make the
  // definition of the move-assignment operator easier. NOTE: If you add
  // additional data members to this class, add them inside the `Data` struct.
  struct Data {
    ad_utility::File file_;
    off_t startOfFile_{};
    std::vector<offset_type> offsets_{};
    // A `CompactStringVectorWriter` that has been moved from may not call
    // `finish()` any more in its destructor.
    ad_utility::ResetWhenMoved<bool, true> finished_ = false;
    offset_type nextOffset_ = 0;
  };
  Data d_;
  static_assert(std::is_nothrow_move_assignable_v<Data>);
  static_assert(std::is_nothrow_move_constructible_v<Data>);

 public:
  explicit CompactStringVectorWriter(const std::string& filename)
      : d_{{filename, "w"}} {
    commonInitialization();
  }

  explicit CompactStringVectorWriter(ad_utility::File&& file)
      : d_{std::move(file)} {
    commonInitialization();
  }

  void push(const data_type* data, size_t elementSize) {
    AD_CONTRACT_CHECK(!d_.finished_);
    d_.offsets_.push_back(d_.nextOffset_);
    d_.nextOffset_ += elementSize;
    d_.file_.write(data, elementSize * sizeof(data_type));
  }

  // Finish writing, and return the moved file. If the return value is
  // discarded, then the file will be closed immediately by the destructor of
  // the `File` class.
  ad_utility::File finish() {
    if (d_.finished_) {
      return {};
    }
    d_.finished_ = true;
    d_.offsets_.push_back(d_.nextOffset_);
    d_.file_.seek(d_.startOfFile_, SEEK_SET);
    d_.file_.write(&d_.nextOffset_, sizeof(size_t));
    d_.file_.seek(0, SEEK_END);
    ad_utility::serialization::FileWriteSerializer f{std::move(d_.file_)};
    f << d_.offsets_;
    return std::move(f).file();
  }

  ~CompactStringVectorWriter() {
    ad_utility::terminateIfThrows(
        [this]() { finish(); },
        "Finishing the underlying File of a `CompactStringVectorWriter` "
        "during destruction failed");
  }

  // The copy operations would be deleted implicitly (because `File` is not
  // copyable.
  CompactStringVectorWriter(const CompactStringVectorWriter&) = delete;
  CompactStringVectorWriter& operator=(const CompactStringVectorWriter&) =
      delete;

  // The defaulted move constructor behave correctly because of the usage
  // of `ResetWhenMoved` with the `finished` member.
  CompactStringVectorWriter(CompactStringVectorWriter&&) = default;

  // The move assignment first has to `finish` the current object, which already
  // might have been written to.
  CompactStringVectorWriter& operator=(
      CompactStringVectorWriter&& other) noexcept {
    finish();
    d_ = std::move(other.d_);
    return *this;
  }

 private:
  // Has to be run by all the constructors
  void commonInitialization() {
    AD_CORRECTNESS_CHECK(d_.file_.isOpen());
    // We don't know the data size yet.
    d_.startOfFile_ = d_.file_.tell();
    size_t dataSizeDummy = 0;
    d_.file_.write(&dataSizeDummy, sizeof(dataSizeDummy));
  }
};
static_assert(
    std::is_nothrow_move_assignable_v<CompactStringVectorWriter<char>>);
static_assert(
    std::is_nothrow_move_constructible_v<CompactStringVectorWriter<char>>);
}  // namespace detail

#endif  // QLEVER_SRC_UTIL_COMPACTSTRINGVECTOR_H
