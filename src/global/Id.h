// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)
#pragma once

#include <absl/strings/str_cat.h>

#include <cstdint>
#include <limits>

#include "../util/Exception.h"

// A strong Id type that internally stores a `uint64_t` but can only be
// explicitly converted to and from the underlying `uint64_t`
struct Id {
  using Type = uint64_t;

 private:
  Type _data;

 public:
  static Id make(Type id) noexcept { return {id}; }
  [[nodiscard]] const Type& get() const noexcept { return _data; }
  Type& get() noexcept { return _data; }

  Id() = default;

  bool operator==(const Id&) const = default;
  auto operator<=>(const Id&) const = default;

  static constexpr Id max() { return {std::numeric_limits<Type>::max()}; }

  static constexpr Id min() { return {std::numeric_limits<Type>::min()}; }

  template <typename H>
  friend H AbslHashValue(H h, const Id& id) {
    return H::combine(std::move(h), id.get());
  }

  template <typename Serializer>
  friend void serialize(Serializer& serializer, Id& id) {
    serializer | id._data;
  }

  // This is not only used in debug code, but also for the direct output
  // of `VERBATIM` (integer) columns.
  // TODO<joka921> as soon as we have the folded IDs, this should only be used
  // for debugging with a clearer output like "ID:0".
  friend std::ostream& operator<<(std::ostream& ostr, const Id& id) {
    ostr << id.get();
    return ostr;
  }

 private:
  constexpr Id(Type data) noexcept : _data{data} {}
};

namespace std {
template <>
struct hash<Id> {
  uint64_t operator()(const Id& id) const {
    return std::hash<uint64_t>{}(id.get());
  }
};
}  // namespace std
// typedef uint64_t Id;
// using Id = ad_utility::datatypes::FancyId;
typedef uint16_t Score;

// TODO<joka921> Make the following ID and index types strong.
using ColumnIndex = uint64_t;
using VocabIndex = uint64_t;
using LocalVocabIndex = uint64_t;
using TextRecordIndex = uint64_t;

// TODO<joka921> The following IDs only appear within the text index in the
// `Index` class, so they should not be public.
using WordIndex = uint64_t;
using WordOrEntityIndex = uint64_t;
using TextBlockIndex = uint64_t;
using CompressionCode = uint64_t;

// Integers, that are probably not integers but strong IDs or indices, but their
// true nature is still to be discovered.
using UnknownIndex = uint64_t;

// A value to use when the result should be empty (e.g. due to an optional join)
// The highest two values are used as sentinels.
static const Id ID_NO_VALUE =
    Id::make(std::numeric_limits<uint64_t>::max() - 2);

namespace ad_utility {

// An exception that is thrown when an integer overflow occurs in the
// `MilestoneIdManager`
class MilestoneIdOverflowException : public std::exception {
 private:
  std::string _message;

 public:
  explicit MilestoneIdOverflowException(std::string message)
      : _message{std::move(message)} {}
  [[nodiscard]] const char* what() const noexcept override {
    return _message.c_str();
  }
};

// Manages two kinds of IDs: plain IDs (unsigned 64-bit integers, just called
// "IDs" in the following), and milestone IDs (unsigned 64-bit integers that are
// multiples of `distanceBetweenMilestones`. This class has the functionality to
// find the next milestone of plain ID, to check whether an ID is amilestone ID
// and to convert milestone IDs from and to a local ID space.
template <size_t distanceBetweenMilestones>
class MilestoneIdManager {
 private:
  // The next free ID;
  uint64_t _nextId{0};
  // The last ID that was assigned. Used for overflow detection.
  uint64_t _previousId{0};

 public:
  MilestoneIdManager() = default;

  // The maximum number of milestone Ids.
  constexpr static uint64_t numMilestoneIds =
      std::numeric_limits<uint64_t>::max() / distanceBetweenMilestones;

  // Get the smallest milestone ID that is larger than all (milestone and
  // non-milestone) previously obtained IDs.
  uint64_t getNextMilestoneId() {
    if (!isMilestoneId(_nextId)) {
      _nextId = (milestoneIdFromLocal(milestoneIdToLocal(_nextId) + 1));
    }
    return getNextId();
  }

  // Get the smallest ID that is larger than all previously obtained IDs.
  uint64_t getNextId() {
    if (_nextId < _previousId) {
      throw MilestoneIdOverflowException{absl::StrCat(
          "Overflow while assigning Ids from a MilestoneIdManager. The "
          "previous "
          "milestone Id was ",
          _previousId, " the next id would be ", _nextId,
          ". The maximum number of milestones is ", numMilestoneIds, ".")};
    }
    _previousId = _nextId;
    _nextId++;
    return _previousId;
  }

  // Is this ID a milestone id, equivalently: Is this ID a multiple of
  // `distanceBetweenMilestones`?
  constexpr static bool isMilestoneId(uint64_t id) {
    return id % distanceBetweenMilestones == 0;
  }

  // Convert a milestone ID to its "local" ID by dividing it by
  // `distanceBetweenMilestones` (the i-th milestone ID will become `i`).
  constexpr static uint64_t milestoneIdToLocal(uint64_t id) {
    return id / distanceBetweenMilestones;
  }

  // Convert "local" ID to milestone ID by multiplying it with
  // `distanceBetweenMilestones`.
  constexpr static uint64_t milestoneIdFromLocal(uint64_t id) {
    return id * distanceBetweenMilestones;
  }
};

}  // namespace ad_utility
