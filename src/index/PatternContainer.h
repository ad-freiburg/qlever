#pragma once

#include "../global/Pattern.h"

/**
 * @brief This is a wrapper class arround the data required for the pattern
 * trick. It supports different internal sizes for the predicate
 * ids (1, 2, 4 or 8 bytes). Pointers to this class may always be cast to
 * pointers of type PatternsContainerImpl with a supported unsigned integral
 * type of size predicateIdSize. These types are uint8_t, uin16_t, uint32_t and
 * uint64_t from <cstdint>.
 */
class PatternContainer {
 public:
  /**
   * @brief Returns the number of bytes used by a predicate id. Is one of
   * 1, 2, 4 or 8.
   */
  virtual unsigned int predicateIdSize() const = 0;
};

/**
 * @brief This is the actual implementation of the PatternContainer with
 * the template argument. Using inheritance here allows for passing
 * around a pointer to the untemplated PatternContainer in code, reducing
 * the number of templated methods required to work with patterns.
 */
template <typename PredicateId>
class PatternContainerImpl : public PatternContainer {
 public:
  unsigned int predicateIdSize() const override { return sizeof(PredicateId); }

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
