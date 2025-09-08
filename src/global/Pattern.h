// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Authors: Florian Kramer (florian.kramer@mail.uni-freiburg.de)
//          Johannes Kalmbach (kalmbach@cs.uni-freiburg.de)

#ifndef QLEVER_SRC_GLOBAL_PATTERN_H
#define QLEVER_SRC_GLOBAL_PATTERN_H

#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <stdexcept>
#include <string>
#include <vector>

#include "backports/concepts.h"
#include "backports/span.h"
#include "global/Id.h"
#include "util/ExceptionHandling.h"
#include "util/File.h"
#include "util/Generator.h"
#include "util/Iterators.h"
#include "util/ResetWhenMoved.h"
#include "util/Serializer/FileSerializer.h"
#include "util/Serializer/SerializeVector.h"
#include "util/TypeTraits.h"

/**
 * @brief This represents a set of relations of a single entity.
 *        (e.g. a set of books that all have an author and a title).
 *        This information can then be used to efficiently count the relations
 *        that a set of entities has (e.g. for autocompletion of relations
 *        while writing a query).
 */
struct Pattern : std::vector<Id> {
  using PatternId = int32_t;
  static constexpr PatternId NoPattern = std::numeric_limits<PatternId>::max();
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
                         ql::span<const data_type>>;
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
  static cppcoro::generator<vector_type> diskIterator(std::string filename);

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

// Hashing support for the `Pattern` class.
template <>
struct std::hash<Pattern> {
  std::size_t operator()(const Pattern& p) const noexcept {
    std::string_view s = std::string_view(
        reinterpret_cast<const char*>(p.data()), sizeof(Id) * p.size());
    return hash<std::string_view>()(s);
  }
};

#endif  // QLEVER_SRC_GLOBAL_PATTERN_H
