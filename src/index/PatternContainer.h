#pragma once

#include <variant>

#include "../global/Pattern.h"

/**
 * @brief This is a wrapper class arround the data required for the pattern
 * trick. It supports different internal sizes for the predicate
 * ids (1, 2, 4 or 8 bytes), depending on the template paramter.
 * possible values for this parameter are uint8_t, uin16_t, uint32_t and
 * uint64_t from <cstdint>.
 */
template <typename PredicateId>
class PatternContainerImpl {
 public:
  using value_type = PredicateId;
  static constexpr auto predicateIdSize() { return sizeof(PredicateId); }

  CompactStringVector<size_t, PredicateId>& patterns() { return _patterns; }

  const CompactStringVector<size_t, PredicateId>& patterns() const {
    return _patterns;
  }

  std::vector<PatternID>& hasPattern() { return _hasPattern; }

  const std::vector<PatternID>& hasPattern() const { return _hasPattern; }

  CompactStringVector<size_t, PredicateId>& hasPredicate() {
    return _hasPredicate;
  }

  const CompactStringVector<size_t, PredicateId>& hasPredicate() const {
    return _hasPredicate;
  }

  size_t numPatterns() const { return _patterns.size(); }

 private:
  /**
   * @brief Maps pattern ids to sets of predicate ids.
   */
  CompactStringVector<size_t, PredicateId> _patterns;

  /**
   * @brief Maps entity ids to pattern ids.
   */
  std::vector<PatternID> _hasPattern;

  /**
   * @brief Maps entity ids to sets of predicate ids
   */
  CompactStringVector<Id, PredicateId> _hasPredicate;
};

/**
 * The actual PatternContainer. Using a std::variant allows us to reduce the
 * code complexity when this type is only passed around and increases type
 * safety (no manual mismatching of the template parameter of
 * PatternContainerImpl is possible)
 */
using PatternContainer =
    std::variant<PatternContainerImpl<uint8_t>, PatternContainerImpl<uint16_t>,
                 PatternContainerImpl<uint32_t>,
                 PatternContainerImpl<uint64_t>>;
