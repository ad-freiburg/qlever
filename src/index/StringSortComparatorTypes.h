// Copyright 2019, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>
//
// Copyright 2025, Bayerische Motoren Werke Aktiengesellschaft (BMW AG)

#ifndef QLEVER_SRC_INDEX_STRINGSORTCOMPARATORTYPES_H
#define QLEVER_SRC_INDEX_STRINGSORTCOMPARATORTYPES_H

#include <cstdint>
#include <string>
#include <string_view>

#include "backports/StartsWithAndEndsWith.h"
#include "backports/concepts.h"
#include "backports/three_way_comparison.h"
#include "util/GenericCharTraits.h"
#include "util/TypeTraits.h"

/**
 * @brief Base class holding the types shared by both the ICU and the NoICU
 * variants of the `LocaleManager` (see `StringSortComparator.h` and
 * `StringSortComparatorsNoICU.h`). None of these types depend on ICU.
 */
class LocaleManagerBase {
 public:
  /// The five collation levels supported by icu, forwarded in a typesafe manner
  enum class Level : uint8_t {
    PRIMARY = 0,
    SECONDARY = 1,
    TERTIARY = 2,
    QUARTERNARY = 3,
    IDENTICAL = 4,
    TOTAL =
        5  // if the identical level returns equal, we take the language  tag
           // into account and then the result by strcmp. that way two strings
           // that have a different byte representation never compare equal
  };

  /**
   * A strong typedef for a string that contains unicode collation weights for
   * another string. The actual storage can be a `std::string` or a
   * `std::string_view`.
   */
  // TODO<GCC12> As soon as we have constexpr std::string, this class can
  //  become constexpr.
  // A `uint8_t` behaves like a `char`, so we use `GenericCharTraits` (see there
  // for why we cannot rely on `std::char_traits` directly).
  using U8CharTraits = ad_utility::GenericCharTraits<uint8_t>;
  using U8String = std::basic_string<uint8_t, U8CharTraits>;
  using U8StringView = std::basic_string_view<uint8_t, U8CharTraits>;

  CPP_template(typename T)(requires ad_utility::SimilarToAny<
                           T, U8String, U8StringView>) class SortKeyImpl {
   public:
    SortKeyImpl() = default;
    explicit SortKeyImpl(U8StringView sortKey) : sortKey_(sortKey) {}
    [[nodiscard]] constexpr const T& get() const noexcept { return sortKey_; }
    constexpr T& get() noexcept { return sortKey_; }

    // Comparison of sort key is done lexicographically on the byte values
    // of member `sortKey_`
    template <typename U>
    [[nodiscard]] int compare(const SortKeyImpl<U>& rhs) const noexcept {
      return U8StringView{sortKey_}.compare(U8StringView{rhs.sortKey_});
    }

    QL_DEFINE_DEFAULTED_THREEWAY_OPERATOR_LOCAL(SortKeyImpl, sortKey_)

    /// Is this sort key a prefix of another sort key. Note: This does not imply
    /// any guarantees on the relation of the underlying strings.
    bool starts_with(const SortKeyImpl& rhs) const noexcept {
      return ql::starts_with(get(), rhs.get());
    }

    /// Return the number of bytes in the `SortKey`
    std::string::size_type size() const noexcept { return get().size(); }

   private:
    T sortKey_;
  };
  using SortKey = SortKeyImpl<U8String>;
  using SortKeyView = SortKeyImpl<U8StringView>;
};

#endif  // QLEVER_SRC_INDEX_STRINGSORTCOMPARATORTYPES_H
