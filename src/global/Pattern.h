// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Florian Kramer (florian.kramer@mail.uni-freiburg.de)

#pragma once
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <span>
#include <stdexcept>
#include <string>
#include <vector>

#include "backports/concepts.h"
#include "global/Id.h"
#include "util/ExceptionHandling.h"
#include "util/File.h"
#include "util/Generator.h"
#include "util/Iterators.h"
#include "util/ResetWhenMoved.h"
#include "util/Serializer/FileSerializer.h"
#include "util/Serializer/SerializeVector.h"
#include "util/TypeTraits.h"
#include "util/UninitializedAllocator.h"

typedef uint32_t PatternID;

static const PatternID NO_PATTERN = std::numeric_limits<PatternID>::max();

/**
 * @brief This represents a set of relations of a single entity.
 *        (e.g. a set of books that all have an author and a title).
 *        This information can then be used to efficiently count the relations
 *        that a set of entities has (e.g. for autocompletion of relations
 *        while writing a query).
 */
struct Pattern {
  using value_type = Id;
  using ref = value_type&;
  using const_ref = const value_type&;

  ref operator[](const size_t pos) { return data_[pos]; }
  const_ref operator[](const size_t pos) const { return data_[pos]; }

  using const_iterator = ad_utility::IteratorForAccessOperator<
      Pattern, ad_utility::AccessViaBracketOperator, ad_utility::IsConst::True>;

  const_iterator begin() const { return {this, 0}; }

  const_iterator end() const { return {this, size()}; }

  bool operator==(const Pattern& other) const = default;

  size_t size() const { return data_.size(); }

  void push_back(value_type i) { data_.push_back(i); }

  void clear() { data_.clear(); }

  const_ref back() const { return data_.back(); }
  ref back() { return data_.back(); }
  bool empty() const { return data_.empty(); }

  const value_type* data() const { return data_.data(); }

  std::vector<value_type> data_;
};

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
                         std::span<const data_type>>;
  using vector_type = std::conditional_t<std::is_same_v<data_type, char>,
                                         std::string, std::vector<data_type>>;

  using Writer = detail::CompactStringVectorWriter<data_type>;
  CompactVectorOfStrings() = default;

  explicit CompactVectorOfStrings(
      const std::vector<std::vector<data_type>>& input) {
    build(input);
  }

  void clear() { *this = CompactVectorOfStrings{}; }

  virtual ~CompactVectorOfStrings() = default;

  /**
   * @brief Fills this CompactVectorOfStrings with input.
   * @param The input from which to build the vector.
   * Note: In C++20 mode we use a `requires clause`, in C++17 mode we use a
   * static assert. Both work, as there is only one overload of `build`.
   */
  template <typename T>
  QL_CONCEPT_OR_NOTHING(requires requires(T t) {
    { *(t.begin()->begin()) } -> ad_utility::SimilarTo<data_type>;
  })
  void build(const T& input) {
    static_assert(
        ad_utility::SimilarTo<decltype(*(input.begin()->begin())), data_type>);
    // Also make room for the end offset of the last element.
    offsets_.reserve(input.size() + 1);
    size_t dataSize = 0;
    for (const auto& element : input) {
      offsets_.push_back(dataSize);
      dataSize += element.size();
    }
    // The last offset is the offset right after the last element.
    offsets_.push_back(dataSize);

    data_.reserve(dataSize);

    for (const auto& el : input) {
      data_.insert(data_.end(), el.begin(), el.end());
    }
  }

  // This is a move-only type.
  CompactVectorOfStrings& operator=(const CompactVectorOfStrings&) = delete;
  CompactVectorOfStrings& operator=(CompactVectorOfStrings&&) noexcept =
      default;
  CompactVectorOfStrings(const CompactVectorOfStrings&) = delete;
  CompactVectorOfStrings(CompactVectorOfStrings&&) noexcept = default;

  // There is one more offset than the number of elements.
  size_t size() const { return ready() ? offsets_.size() - 1 : 0; }

  bool ready() const { return !offsets_.empty(); }

  /**
   * @brief operator []
   * @param i
   * @return A std::pair containing a pointer to the data, and the number of
   *         elements stored at the pointers target.
   */
  const value_type operator[](size_t i) const {
    offset_type offset = offsets_[i];
    const data_type* ptr = data_.data() + offset;
    size_t size = offsets_[i + 1] - offset;
    return {ptr, size};
  }

  // Forward iterator for a `CompactVectorOfStrings` that reads directly from
  // disk without buffering the whole `Vector`.
  static cppcoro::generator<vector_type> diskIterator(string filename);

  using Iterator = ad_utility::IteratorForAccessOperator<
      CompactVectorOfStrings, ad_utility::AccessViaBracketOperator,
      ad_utility::IsConst::True>;

  Iterator begin() const { return {this, 0}; }
  Iterator end() const { return {this, size()}; }

  using const_iterator = Iterator;

  // Allow serialization via the ad_utility::serialization interface.
  AD_SERIALIZE_FRIEND_FUNCTION(CompactVectorOfStrings) {
    serializer | arg.data_;
    serializer | arg.offsets_;
  }

 private:
  std::vector<data_type> data_;
  std::vector<offset_type> offsets_;
};

namespace detail {
// Allows the incremental writing of a `CompactVectorOfStrings` directly to a
// file.
template <typename data_type>
struct CompactStringVectorWriter {
  ad_utility::File file_;
  off_t startOfFile_;
  using offset_type = typename CompactVectorOfStrings<data_type>::offset_type;
  std::vector<offset_type> offsets_;

  // A `CompactStringVectorWriter` that has been moved from may not call
  // `finish()` any more in its destructor.
  ad_utility::ResetWhenMoved<bool, true> finished_ = false;
  offset_type nextOffset_ = 0;

  explicit CompactStringVectorWriter(const std::string& filename)
      : file_{filename, "w"} {
    commonInitialization();
  }

  explicit CompactStringVectorWriter(ad_utility::File&& file)
      : file_{std::move(file)} {
    commonInitialization();
  }

  void push(const data_type* data, size_t elementSize) {
    AD_CONTRACT_CHECK(!finished_);
    offsets_.push_back(nextOffset_);
    nextOffset_ += elementSize;
    file_.write(data, elementSize * sizeof(data_type));
  }

  // Finish writing, and return the moved file. If the return value is
  // discarded, then the file will be closed immediately by the destructor of
  // the `File` class.
  ad_utility::File finish() {
    if (finished_) {
      return {};
    }
    finished_ = true;
    offsets_.push_back(nextOffset_);
    file_.seek(startOfFile_, SEEK_SET);
    file_.write(&nextOffset_, sizeof(size_t));
    file_.seek(0, SEEK_END);
    ad_utility::serialization::FileWriteSerializer f{std::move(file_)};
    f << offsets_;
    return std::move(f).file();
  }

  ~CompactStringVectorWriter() {
    if (!finished_) {
      ad_utility::terminateIfThrows(
          [this]() { finish(); },
          "Finishing the underlying File of a `CompactStringVectorWriter` "
          "during destruction failed");
    }
  }

  // The copy operations would be deleted implicitly (because `File` is not
  // copyable.
  CompactStringVectorWriter(const CompactStringVectorWriter&) = delete;
  CompactStringVectorWriter& operator=(const CompactStringVectorWriter&) =
      delete;

  // The move operations have to be explicitly defaulted, because we have a
  // manually defined destructor.
  // Note: The defaulted move operations behave correctly because of the usage
  // of `ResetWhenMoved` with the `finished` member.
  CompactStringVectorWriter(CompactStringVectorWriter&&) = default;
  CompactStringVectorWriter& operator=(CompactStringVectorWriter&&) = default;

 private:
  // Has to be run by all the constructors
  void commonInitialization() {
    AD_CONTRACT_CHECK(file_.isOpen());
    // We don't know the data size yet.
    startOfFile_ = file_.tell();
    size_t dataSizeDummy = 0;
    file_.write(&dataSizeDummy, sizeof(dataSizeDummy));
  }
};
static_assert(
    std::is_nothrow_move_assignable_v<CompactStringVectorWriter<char>>);
static_assert(
    std::is_nothrow_move_constructible_v<CompactStringVectorWriter<char>>);
}  // namespace detail

// Forward iterator for a `CompactVectorOfStrings` that reads directly from
// disk without buffering the whole `Vector`.
template <typename DataT>
cppcoro::generator<typename CompactVectorOfStrings<DataT>::vector_type>
CompactVectorOfStrings<DataT>::diskIterator(string filename) {
  ad_utility::File dataFile{filename, "r"};
  ad_utility::File indexFile{filename, "r"};
  AD_CORRECTNESS_CHECK(dataFile.isOpen());
  AD_CORRECTNESS_CHECK(indexFile.isOpen());

  const size_t dataSizeInBytes = [&]() {
    size_t dataSize;
    dataFile.read(&dataSize, sizeof(dataSize));
    return dataSize * sizeof(DataT);
  }();

  indexFile.seek(sizeof(dataSizeInBytes) + dataSizeInBytes, SEEK_SET);
  size_t size;
  indexFile.read(&size, sizeof(size));

  size--;  // There is one more offset than the number of elements.

  size_t offset;
  indexFile.read(&offset, sizeof(offset));

  for (size_t i = 0; i < size; ++i) {
    size_t nextOffset;
    indexFile.read(&nextOffset, sizeof(nextOffset));
    auto currentSize = nextOffset - offset;
    vector_type result;
    result.resize(currentSize);
    dataFile.read(result.data(), currentSize * sizeof(DataT));
    co_yield result;
    offset = nextOffset;
  }
}

template <>
struct std::hash<Pattern> {
  std::size_t operator()(const Pattern& p) const noexcept {
    std::string_view s = std::string_view(
        reinterpret_cast<const char*>(p.data_.data()), sizeof(Id) * p.size());
    return hash<std::string_view>()(s);
  }
};
