// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (January of 2023, schlegea@informatik.uni-freiburg.de)
// Author of the file this file is based on: Björn Buchhold
// (buchhold@informatik.uni-freiburg.de)
//
// Copyright 2025, Bayerische Motoren Werke Aktiengesellschaft (BMW AG)

#include <absl/strings/str_cat.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <cmath>
#include <concepts>
#include <cstddef>
#include <cstdio>
#include <ctime>
#include <functional>
#include <iterator>
#include <limits>
#include <numeric>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <utility>
#include <valarray>

#include "../benchmark/infrastructure/Benchmark.h"
#include "../benchmark/infrastructure/BenchmarkMeasurementContainer.h"
#include "../benchmark/infrastructure/BenchmarkMetadata.h"
#include "../benchmark/util/ResultTableColumnOperations.h"
#include "../test/util/IdTableHelpers.h"
#include "../test/util/JoinHelpers.h"
#include "../test/util/RandomTestHelpers.h"
#include "engine/Engine.h"
#include "engine/Join.h"
#include "engine/QueryExecutionTree.h"
#include "engine/Result.h"
#include "engine/idTable/IdTable.h"
#include "global/ValueId.h"
#include "util/Algorithm.h"
#include "util/ConfigManager/ConfigManager.h"
#include "util/ConfigManager/ConfigOptionProxy.h"
#include "util/ConstexprUtils.h"
#include "util/Exception.h"
#include "util/Forward.h"
#include "util/HashMap.h"
#include "util/HashSet.h"
#include "util/Log.h"
#include "util/MemorySize/MemorySize.h"
#include "util/Random.h"
#include "util/StringUtils.h"
#include "util/TypeTraits.h"

using namespace std::string_literals;

namespace ad_benchmark {

/*
@brief Return true, iff, the given value is unchanged, when casting to type
`Target`
*/
template <typename Target, typename Source>
requires std::convertible_to<Source, Target>
static constexpr bool isValuePreservingCast(const Source& source) {
  return static_cast<Source>(static_cast<Target>(source)) == source;
}

/*
@brief Return biggest possible value for the given arithmetic type.
*/
CPP_template(typename Type)(
    requires ad_utility::Arithmetic<Type>) consteval Type getMaxValue() {
  return std::numeric_limits<Type>::max();
}

/*
@brief Helper function for throwing overflow errors.

@param reason What is the reason for the overflow?
*/
template <typename Type>
void throwOverflowError(const std::string_view reason) {
  std::string typeName;
  if constexpr (std::same_as<Type, double>) {
    typeName = "double";
  } else {
    AD_CORRECTNESS_CHECK((std::same_as<Type, size_t>));
    typeName = "size_t";
  }
  throw std::runtime_error(absl::StrCat(
      typeName, " overflow error: The ", reason, " is bigger than the ",
      typeName, " type maximum ", getMaxValue<Type>(),
      ". Try reducing the values for the configuration options."));
}

/*
A struct holding a list of all unique elements inside an `IdTable` column and
how often they occur inside this column.
Note: Can not be updated after creation.
*/
struct SetOfIdTableColumnElements {
  /*
  The list of all unique elements is also included inside `numOccurrences_`.
  However, accessing a hash map entry based on index position takes linear time,
  instead of constant.
  */
  std::vector<std::reference_wrapper<const ValueId>> uniqueElements_{};
  ad_utility::HashMap<ValueId, size_t> numOccurrences_{};

  /*
  Set the member variables for the given column.
  */
  explicit SetOfIdTableColumnElements(
      const std::span<const ValueId>& idTableColumnRef) {
    ql::ranges::for_each(idTableColumnRef, [this](const ValueId& id) {
      if (auto numOccurrencesIterator = numOccurrences_.find(id);
          numOccurrencesIterator != numOccurrences_.end()) {
        (numOccurrencesIterator->second)++;
      } else {
        numOccurrences_.emplace(id, 1UL);
        uniqueElements_.emplace_back(id);
      }
    });
  }
};

/*
@brief Create an overlap between the join columns of the IdTables, by randomly
choosing distinct elements from the join column of the smaller table and
overriding all their occurrences in the join column with randomly chosen
distinct elements from the join column of the bigger table.

@param smallerTable The table, where distinct join column elements will be
overwritten.
@param biggerTable The table, where distinct join column elements will be copied
from.
@param probabilityToCreateOverlap The height of the probability for any distinct
join column element of `smallerTable` to be overwritten by a random distinct
join column element of `biggerTable`.
@param randomSeed Seed for the random generators.

@returns The number of new overlap matches created. That is, the amount
of rows added to the result of a join operation with the two given tables.
*/
static size_t createOverlapRandomly(IdTableAndJoinColumn* const smallerTable,
                                    const IdTableAndJoinColumn& biggerTable,
                                    const double probabilityToCreateOverlap,
                                    ad_utility::RandomSeed randomSeed) {
  // For easier access.
  auto smallerTableJoinColumnRef{
      smallerTable->idTable.getColumn(smallerTable->joinColumn)};
  const auto& biggerTableJoinColumnRef{
      biggerTable.idTable.getColumn(biggerTable.joinColumn)};

  // The probability for creating an overlap must be in (0,100], any other
  // values make no sense.
  AD_CONTRACT_CHECK(0 < probabilityToCreateOverlap &&
                    probabilityToCreateOverlap <= 100);

  // Is the bigger table actually bigger?
  AD_CONTRACT_CHECK(smallerTableJoinColumnRef.size() <=
                    biggerTableJoinColumnRef.size());

  // Collect and count the table elements.
  SetOfIdTableColumnElements biggerTableJoinColumnSet(biggerTableJoinColumnRef);

  // Seeds for the random generators, so that things are less similar.
  const std::array<ad_utility::RandomSeed, 2> seeds =
      createArrayOfRandomSeeds<2>(std::move(randomSeed));

  /*
  Creating the generator for choosing a random distinct join column element
  in the bigger table.
  */
  ad_utility::SlowRandomIntGenerator<size_t> randomBiggerTableElement(
      0, biggerTableJoinColumnSet.uniqueElements_.size() - 1, seeds.at(0));

  // Generator for checking, if an overlap should be created.
  ad_utility::RandomDoubleGenerator randomDouble(0, 100, seeds.at(1));

  /*
  The amount of new overlap matches, that are generated by this
  function.
  */
  size_t newOverlapMatches{0UL};

  // Create the overlap.
  ad_utility::HashMap<ValueId, std::reference_wrapper<const ValueId>>
      smallerTableElementToNewElement{};
  ql::ranges::for_each(
      smallerTableJoinColumnRef,
      [&randomDouble, &probabilityToCreateOverlap,
       &smallerTableElementToNewElement, &randomBiggerTableElement,
       &newOverlapMatches, &biggerTableJoinColumnSet](auto& id) {
        /*
        If a value has no hash map value, with which it will be overwritten, we
        either assign it its own value, or an element from the bigger table.
        */
        if (auto newValueIterator = smallerTableElementToNewElement.find(id);
            newValueIterator != smallerTableElementToNewElement.end()) {
          const ValueId& newValue = newValueIterator->second;
          // Values, that are only found in the smaller table, are added to the
          // hash map with value 0.
          const size_t numOccurrences{
              biggerTableJoinColumnSet.numOccurrences_[newValue]};

          // Skip, if the addition would cause an overflow.
          if (newOverlapMatches >
              getMaxValue<decltype(newOverlapMatches)>() - numOccurrences) {
            return;
          }
          id = newValue;
          newOverlapMatches += numOccurrences;
        } else if (randomDouble() <= probabilityToCreateOverlap) {
          /*
          Randomly assign one of the distinct elements in the bigger table to be
          the new value. This can only happen the first time, an element in the
          smaller table is encountered, because afterwards it will always have
          an entry in the hash map for the new values.
          */
          smallerTableElementToNewElement.emplace(
              id, biggerTableJoinColumnSet.uniqueElements_.at(
                      randomBiggerTableElement()));
        } else {
          /*
          Assign the value to itself.
          This is needed, because so we can indirectly track the first
          'encounter' with an distinct element in the smaller table and save
          resources.
          */
          smallerTableElementToNewElement.emplace(id, id);
        }
      });

  return newOverlapMatches;
}

/*
@brief Create overlaps between the join columns of the `IdTable`s until it is no
longer possible to grow closer to the wanted number of overlap matches.
Note: Because of runtime complexity concerns, not all overlap possibilities can
be tried and the end result can be unoptimal.

@param smallerTable The table, where distinct join column elements will be
overwritten.
@param biggerTable The table, where distinct join column elements will be copied
from.
@param wantedNumNewOverlapMatches How many new overlap matches should be
created. Note, that it is not always possible to reach the wanted number.
@param randomSeed Seed for the random generators.

@returns The number of new overlap matches created. That is, the amount
of rows added to the result of a join operation with the two given tables.
*/
static size_t createOverlapRandomly(IdTableAndJoinColumn* const smallerTable,
                                    const IdTableAndJoinColumn& biggerTable,
                                    const size_t wantedNumNewOverlapMatches,
                                    ad_utility::RandomSeed randomSeed) {
  // For easier access.
  auto smallerTableJoinColumnRef{
      smallerTable->idTable.getColumn(smallerTable->joinColumn)};
  const auto& biggerTableJoinColumnRef{
      biggerTable.idTable.getColumn(biggerTable.joinColumn)};

  // Is the bigger table actually bigger?
  AD_CONTRACT_CHECK(smallerTableJoinColumnRef.size() <=
                    biggerTableJoinColumnRef.size());

  // Collect and count the table elements.
  const SetOfIdTableColumnElements biggerTableJoinColumnSet(
      biggerTableJoinColumnRef);
  SetOfIdTableColumnElements smallerTableJoinColumnSet(
      smallerTableJoinColumnRef);

  // Seeds for the random generators, so that things are less similar.
  const std::array<ad_utility::RandomSeed, 2> seeds =
      createArrayOfRandomSeeds<2>(std::move(randomSeed));

  /*
  Creating the generator for choosing a random distinct join column element
  in the bigger table.
  */
  ad_utility::SlowRandomIntGenerator<size_t> randomBiggerTableElement(
      0, biggerTableJoinColumnSet.uniqueElements_.size() - 1, seeds.at(0));

  /*
  Assign distinct join column elements of the smaller table to be overwritten by
  the bigger table, until we created enough new overlap matches.
  */
  randomShuffle(smallerTableJoinColumnSet.uniqueElements_.begin(),
                smallerTableJoinColumnSet.uniqueElements_.end(), seeds.at(1));
  size_t newOverlapMatches{0};
  ad_utility::HashMap<ValueId, std::reference_wrapper<const ValueId>>
      smallerTableElementToNewElement{};
  ql::ranges::for_each(
      smallerTableJoinColumnSet.uniqueElements_,
      [&randomBiggerTableElement, &wantedNumNewOverlapMatches,
       &newOverlapMatches, &smallerTableElementToNewElement,
       &biggerTableJoinColumnSet,
       &smallerTableJoinColumnSet](const ValueId& smallerTableId) {
        const auto& biggerTableId{biggerTableJoinColumnSet.uniqueElements_.at(
            randomBiggerTableElement())};

        // Skip this possibility, if we have an overflow.
        size_t newMatches{
            smallerTableJoinColumnSet.numOccurrences_.at(smallerTableId)};
        if (const size_t numOccurencesBiggerTable{
                biggerTableJoinColumnSet.numOccurrences_.at(biggerTableId)};
            newMatches > getMaxValue<size_t>() / numOccurencesBiggerTable) {
          return;
        } else {
          newMatches *= numOccurencesBiggerTable;
        }

        // Just add as long as the result is smaller/equal to the wanted number
        // of overlaps.
        if (newMatches <= wantedNumNewOverlapMatches &&
            newOverlapMatches <= wantedNumNewOverlapMatches - newMatches) {
          smallerTableElementToNewElement.emplace(smallerTableId,
                                                  biggerTableId);
          newOverlapMatches += newMatches;
        }
      });

  // Overwrite the designated values in the smaller table.
  ql::ranges::for_each(
      smallerTableJoinColumnRef, [&smallerTableElementToNewElement](auto& id) {
        if (auto newValueIterator = smallerTableElementToNewElement.find(id);
            newValueIterator != smallerTableElementToNewElement.end()) {
          id = newValueIterator->second;
        }
      });

  return newOverlapMatches;
}

/*
The columns of the automatically generated benchmark tables contain the
following information:
- The parameter, that changes with every row.
- Time needed for sorting `IdTable`s.
- Time needed for merge/galloping join.
- Time needed for sorting and merge/galloping added together.
- Time needed for the hash join.
- How many rows the result of joining the tables has.
- How much faster the hash join is. For example: Two times faster.

The following enum exists, in order to make the information about the order of
columns explicit.
*/
enum struct GeneratedTableColumn : unsigned long {
  ChangingParamter = 0UL,
  TimeForSorting,
  TimeForMergeGallopingJoin,
  TimeForSortingAndMergeGallopingJoin,
  TimeForHashJoin,
  NumRowsOfJoinResult,
  JoinAlgorithmSpeedup
};

// TODO<c++23> Replace usage with `std::to_underlying`.
/*
Convert the given enum value into the underlying type.
*/
template <typename Enum>
requires std::is_enum_v<Enum> auto toUnderlying(const Enum& e) {
  return static_cast<std::underlying_type_t<Enum>>(e);
}

// `T` must be an invocable object, which can be invoked with `const size_t&`
// and returns an instance of `ReturnType`.
template <typename T, typename ReturnType>
concept growthFunction =
    ad_utility::RegularInvocableWithExactReturnType<T, ReturnType,
                                                    const size_t&>;

// Is `T` of the given type, or a function, that takes `size_t` and return
// the given type?
template <typename T, typename Type>
concept isTypeOrGrowthFunction =
    std::same_as<T, Type> || growthFunction<T, Type>;

// There must be exactly one growth function, that either returns a `size_t`, or
// a `float`.
template <typename... Ts>
concept exactlyOneGrowthFunction =
    ((growthFunction<Ts, size_t> || growthFunction<Ts, float>)+...) == 1;

// Is something a growth function?
struct IsGrowthFunction {
  template <typename T>
  constexpr bool operator()() const {
    /*
    We have to cheat a bit, because being a function is not something that
    can easily be checked for to my knowledge. Instead, we simply check if
    it's one of the limited variations of growth function that we allow.
    */
    if constexpr (growthFunction<T, size_t> || growthFunction<T, float>) {
      return true;
    } else {
      return false;
    }
  }
};

/*
@brief Calculates the smallest whole exponent $n$, so that $base^n$ is equal, or
bigger, than the `startingPoint`.
*/
template <std::convertible_to<double> T>
static double calculateNextWholeExponent(const T& base,
                                         const T& startingPoint) {
  // This is a rather simple calculation: We calculate
  // $log_(base)(startingPoint)$ and round up.
  AD_CONTRACT_CHECK(isValuePreservingCast<double>(startingPoint));
  AD_CONTRACT_CHECK(isValuePreservingCast<double>(base));
  return std::ceil(std::log(static_cast<double>(startingPoint)) /
                   std::log(static_cast<double>(base)));
}

/*
@brief Generate a sorted, inclusive interval of exponents $base^x$, with $x$
always a natural number.
*/
CPP_template(typename T)(requires ad_utility::Arithmetic<T> CPP_and
                             std::convertible_to<T, double>) static std::
    vector<T> generateExponentInterval(T base, T inclusiveLowerBound,
                                       T inclusiveUpperBound) {
  std::vector<T> elements{};

  /*
  A short explanation: We calculate $log_(base_)(inclusiveLowerBound_)$ and
  round up. This should give us the $x$ of the first $base_^x$ equal, or
  bigger, than `inclusiveLowerBound_`.
  */
  AD_CONTRACT_CHECK(isValuePreservingCast<double>(base));
  AD_CONTRACT_CHECK(isValuePreservingCast<T>(
      std::pow(static_cast<double>(base),
               calculateNextWholeExponent(base, inclusiveLowerBound))));
  auto currentExponent = static_cast<T>(
      std::pow(static_cast<double>(base),
               calculateNextWholeExponent(base, inclusiveLowerBound)));

  // The rest of the elements.
  while (currentExponent <= inclusiveUpperBound &&
         currentExponent <= std::numeric_limits<T>::max() / base) {
    elements.push_back(currentExponent);
    currentExponent *= base;
  }
  return elements;
}

/*
@brief Generate a sorted,inclusive interval of all natural numbers inside
`[inclusiveLowerBound, inclusiveUpperBound]`.
*/
CPP_template(typename T)(requires ad_utility::Arithmetic<T>) static std::vector<
    T> generateNaturalNumberSequenceInterval(T inclusiveLowerBound,
                                             T inclusiveUpperBound) {
  if constexpr (std::floating_point<T>) {
    inclusiveLowerBound = std::ceil(inclusiveLowerBound);
    inclusiveUpperBound = std::floor(inclusiveUpperBound);
  }
  std::vector<T> elements(
      inclusiveUpperBound < inclusiveLowerBound
          ? 0UL
          : static_cast<size_t>(inclusiveUpperBound - inclusiveLowerBound + 1),
      inclusiveLowerBound);
  std::iota(elements.begin(), elements.end(), inclusiveLowerBound);

  return elements;
}

// Merge multiple sorted vectors into one sorted vector, where every element is
// unique.
CPP_template(typename T)(requires ad_utility::Arithmetic<T>) static std::vector<
    T> mergeSortedVectors(const std::vector<std::vector<T>>& intervals) {
  std::vector<T> mergedVector{};

  // Merge.
  ql::ranges::for_each(intervals, [&mergedVector](std::vector<T> elements) {
    if (mergedVector.empty() || elements.empty()) {
      ql::ranges::copy(elements, std::back_inserter(mergedVector));
      return;
    }
    const size_t idxOldLastElem = mergedVector.size() - 1;
    ql::ranges::copy(elements, std::back_inserter(mergedVector));
    if (mergedVector.at(idxOldLastElem) > mergedVector.at(idxOldLastElem + 1)) {
      ql::ranges::inplace_merge(
          mergedVector,
          ql::ranges::next(mergedVector.begin(), idxOldLastElem + 1));
    }
  });

  // Delete duplicates.
  const auto ret = std::ranges::unique(mergedVector);
  mergedVector.erase(ret.begin(), ret.end());
  return mergedVector;
}

/*
@brief Approximate the amount of memory (in bytes), that a `IdTable` needs.

@param numRows, numColumns How many rows and columns the `IdTable` has.
*/
ad_utility::MemorySize approximateMemoryNeededByIdTable(
    const size_t& numRows, const size_t& numColumns) {
  /*
  The overhead can be, more or less, ignored. We are just concerned over
  the space needed for the entries.
  */
  constexpr size_t memoryPerIdTableEntryInByte = sizeof(IdTable::value_type);

  // In case of overflow, throw an error.
  if (numRows > getMaxValue<size_t>() / numColumns ||
      numRows * numColumns >
          getMaxValue<size_t>() / memoryPerIdTableEntryInByte) {
    throwOverflowError<size_t>(absl::StrCat("memory size of an 'IdTable' with ",
                                            numRows, " and ", numColumns,
                                            " columns"));
  }

  return ad_utility::MemorySize::bytes(numRows * numColumns *
                                       memoryPerIdTableEntryInByte);
}

/*
@brief Approximate the amount of rows (rounded down) in an `IdTable` with the
given number of columns and memory size.
*/
static size_t approximateNumIdTableRows(const ad_utility::MemorySize& m,
                                        const size_t numColumns) {
  AD_CORRECTNESS_CHECK(numColumns > 0);
  AD_CORRECTNESS_CHECK(m.getBytes() > 0);
  return (m.getBytes() / sizeof(IdTable::value_type)) / numColumns;
}

// A struct for holding the variables, that the configuration options will write
// to.
struct ConfigVariables {
  /*
  The benchmark classes always make tables, where one attribute of the
  `IdTables` gets bigger with every row, while the other attributes stay the
  same.
  For the attributes, that don't stay the same, inclusive boundaries are
  defined. Sometimes implicitly via other configuration option. (With the
  exception of the sample size ratios for the special benchmarking class
  `BmSampleSizeRatio`.)

  For an explanation, what a member variables does, see the created
  `ConfigOption`s in `GeneralInterfaceImplementation`.
  */
  size_t minSmallerTableRows_;
  size_t minBiggerTableRows_;
  size_t maxBiggerTableRows_;
  size_t smallerTableNumColumns_;
  size_t biggerTableNumColumns_;
  float overlapChance_;
  float smallerTableJoinColumnSampleSizeRatio_;
  float biggerTableJoinColumnSampleSizeRatio_;
  float minRatioRows_;
  float maxRatioRows_;
  std::vector<float> benchmarkSampleSizeRatios_;

  /*
  The max time for a single measurement and the max memory, that a single
  `IdTable` is allowed to take up.
  Both are set via `ConfigManager` and the user at runtime, but because their
  pure form contains coded information, mainly that `0` stands for `infinite`,
  there are getter, which transform them into easier to understand forms.
  */
  float maxTimeSingleMeasurement_;
  std::string configVariableMaxMemory_;

  /*
  The random seed, that we use to create random seeds for our random
  generators. Transformed into the more fitting type via getter.
  */
  size_t randomSeed_;

  /*
  Get the value of the corresponding configuration option. Empty optional stands
  for `infinite` time.
  */
  std::optional<float> maxTimeSingleMeasurement() const {
    if (maxTimeSingleMeasurement_ != 0) {
      return {maxTimeSingleMeasurement_};
    } else {
      return {std::nullopt};
    }
  }

  /*
  @brief The maximum amount of memory, the bigger table, and by simple logic
  also the smaller table, is allowed to take up. Iff returns the same value as
  `maxMemory()`, if the `max-memory` configuration option, interpreted as a
  `MemorySize`, wasn't set to `0`. (Which stand for infinite memory.)
  */
  ad_utility::MemorySize maxMemoryBiggerTable() const {
    return maxMemory().value_or(approximateMemoryNeededByIdTable(
        maxBiggerTableRows_, biggerTableNumColumns_));
  }

  /*
  The maximum amount of memory, any table is allowed to take up.
  Returns the value of the `max-memory` configuration option interpreted as a
  `MemorySize`, if it wasn't set to `0`. (Which stand for infinite memory.) In
  the case of `0`, returns an empty optional.
  */
  std::optional<ad_utility::MemorySize> maxMemory() const {
    ad_utility::MemorySize maxMemory{
        ad_utility::MemorySize::parse(configVariableMaxMemory_)};
    if (maxMemory != 0_B) {
      return {std::move(maxMemory)};
    } else {
      return {std::nullopt};
    }
  }

  /*
  Getter for the the random seed, that we use to create random seeds for our
  random generators.
  */
  ad_utility::RandomSeed randomSeed() const {
    /*
    Note: The static cast is needed, because a random generator seed is always
    `unsigned int`:
    */
    return ad_utility::RandomSeed::make(static_cast<unsigned int>(randomSeed_));
  }
};

/*
Partly implements the interface `BenchmarkInterface`, by:

- Providing the member variables, that the benchmark classes here need
using the `ConfigManager`.

- A helper function for adding the general metadata, that every benchmarking
class adds..

- A general function for generating a specific `ResultTable`, that measures the
join algorithms for the given configuration options.
*/
class GeneralInterfaceImplementation : public BenchmarkInterface {
  // The variables, that our configuration options will write to.
  ConfigVariables configVariables_;

 public:
  const ConfigVariables& getConfigVariables() const { return configVariables_; }

  // The default constructor, which sets up the `ConfigManager`.
  GeneralInterfaceImplementation() {
    ad_utility::ConfigManager& config = getConfigManager();

    decltype(auto) minSmallerTableRows = config.addOption(
        "min-smaller-table-rows",
        "The minimum amount of rows for the smaller 'IdTable' in benchmarking "
        "tables.",
        &configVariables_.minSmallerTableRows_, 10000UL);

    decltype(auto) minBiggerTableRows = config.addOption(
        "min-bigger-table-rows",
        "The minimum amount of rows for the bigger 'IdTable' in benchmarking "
        "tables.",
        &configVariables_.minBiggerTableRows_, 100000UL);
    decltype(auto) maxBiggerTableRows = config.addOption(
        "max-bigger-table-rows",
        "The maximum amount of rows for the bigger 'IdTable' in benchmarking "
        "tables.",
        &configVariables_.maxBiggerTableRows_, 10000000UL);

    decltype(auto) smallerTableNumColumns =
        config.addOption("smaller-table-num-columns",
                         "The amount of columns in the smaller IdTable.",
                         &configVariables_.smallerTableNumColumns_, 20UL);
    decltype(auto) biggerTableNumColumns =
        config.addOption("bigger-table-num-columns",
                         "The amount of columns in the bigger IdTable.",
                         &configVariables_.biggerTableNumColumns_, 20UL);

    decltype(auto) overlapChance = config.addOption(
        "overlap-chance",
        "Chance for all occurrences of a distinc element in the join column of "
        "the smaller 'IdTable' to be the same value as a distinc element in "
        "the join column of the bigger 'IdTable'. Must be in the range "
        "$(0,100]$.",
        &configVariables_.overlapChance_, 50.f);

    decltype(auto) smallerTableSampleSizeRatio = config.addOption(
        "smaller-table-join-column-sample-size-ratio",
        "Join column entries of the smaller tables are picked from a sample "
        "space with size 'Amount of smaller table rows' via discrete uniform "
        "distribution. This option adjusts the sample space size to 'Amount of "
        "rows * ratio' (rounded up), which affects the possibility of "
        "duplicates.",
        &configVariables_.smallerTableJoinColumnSampleSizeRatio_, 1.f);
    decltype(auto) biggerTableSampleSizeRatio = config.addOption(
        "bigger-table-join-column-sample-size-ratio",
        "Join column entries of the bigger tables are picked from a sample "
        "space with size 'Amount of bigger table rows' via discrete uniform "
        "distribution. This option adjusts the sample space size to 'Amount of "
        "rows * ratio' (rounded up), which affects the possibility of "
        "duplicates.",
        &configVariables_.biggerTableJoinColumnSampleSizeRatio_, 1.f);

    decltype(auto) randomSeed = config.addOption(
        "random-seed",
        "The seed used for random generators. Note: The default value is a "
        "non-deterministic random value, which changes with every execution.",
        &configVariables_.randomSeed_,
        static_cast<size_t>(std::random_device{}()));
    using randomSeedValueType = decltype(randomSeed)::value_type;

    decltype(auto) minRatioRows = config.addOption(
        "min-ratio-rows",
        "The minimum row ratio between the smaller and the bigger 'IdTable' "
        "for a benchmark table in the benchmark class 'Benchmarktables, where "
        "the smaller table grows and the ratio between tables stays the "
        "same.' and 'Benchmarktables, where the smaller table stays at the "
        "same amount of rows and the bigger tables keeps getting bigger.'. "
        "Also used for the calculation of the number of rows in the smaller "
        "table in 'Benchmarktables, where only the sample size ratio "
        "changes.'.",
        &configVariables_.minRatioRows_, 1.f);
    decltype(auto) maxRatioRows = config.addOption(
        "max-ratio-rows",
        "The maximum row ratio between the smaller and the bigger 'IdTable' "
        "for a benchmark table in the benchmark class 'Benchmarktables, where "
        "the smaller table grows and the ratio between tables stays the "
        "same.'.",
        &configVariables_.maxRatioRows_, 1000.f);

    decltype(auto) maxMemoryInStringFormat = config.addOption(
        "max-memory",
        "Max amount of memory that any 'IdTable' is allowed to take up. '0' "
        "for unlimited memory. When set to anything else than '0', "
        "configuration option 'max-bigger-table-rows' is ignored. Example: "
        "4kB, "
        "8MB, 24B, etc. ...",
        &configVariables_.configVariableMaxMemory_, "0B"s);

    decltype(auto) maxTimeSingleMeasurement = config.addOption(
        "max-time-single-measurement",
        "The maximal amount of time, in seconds, any function measurement is "
        "allowed to take. '0' for unlimited time. Note: This can only be "
        "checked, after a measurement was taken.",
        &configVariables_.maxTimeSingleMeasurement_, 0.f);

    decltype(auto) benchmarkSampleSizeRatios = config.addOption(
        "benchmark-sample-size-ratios",
        absl::StrCat(
            "The sample size ratios for the benchmark class 'Benchmarktables, "
            "where only the sample size ratio changes.', where the sample size "
            "ratio for the smaller and bigger table are set to every element "
            "combination in the cartesian product of this set with itself. "
            "Example for '{1.0, 2.0}':\n",
            ad_utility::addIndentation(
                "- Both to '1.0'.\n- Both to '2.0'.\n- The smaller table "
                "sample size ratio to '1.0' and the bigger table sample size "
                "ratio to '2.0'.\n- The smaller table sample size ratio to "
                "'2.0' and the bigger table sample size ratio to '1.0'.",
                "    ")),
        &configVariables_.benchmarkSampleSizeRatios_,
        std::vector{0.1f, 1.f, 10.f});
    using benchmarkSampleSizeRatiosValueType =
        decltype(benchmarkSampleSizeRatios)::value_type;

    // Helper function for generating lambdas for validators.

    /*
    @brief The generated lambda returns true, iff if it is called with a value,
    that is bigger than the given minimum value

    @param canBeEqual If true, the generated lambda also returns true, if the
    values are equal.
    */
    auto generateBiggerEqualLambda = [](const auto& minimumValue,
                                        bool canBeEqual) {
      using T = std::decay_t<decltype(minimumValue)>;
      return [minimumValue, canBeEqual](const T& valueToCheck) {
        return valueToCheck > minimumValue ||
               (canBeEqual && valueToCheck == minimumValue);
      };
    };
    auto generateBiggerEqualLambdaDesc =
        []<typename OptionType,
           typename = std::enable_if_t<ad_utility::isInstantiation<
               OptionType, ad_utility::ConstConfigOptionProxy>>>(
            const OptionType& option, const auto& minimumValue,
            bool canBeEqual) {
      return absl::StrCat("'", option.getConfigOption().getIdentifier(),
                          "' must be bigger than",
                          canBeEqual ? ", or equal to," : "", " ", minimumValue,
                          ".");
    };

    // Object with a `operator()` for the `<=` operator.
    auto lessEqualLambda = std::less_equal<size_t>{};
    auto generateLessEqualLambdaDesc =
        []<typename OptionType,
           typename = std::enable_if_t<ad_utility::isInstantiation<
               OptionType, ad_utility::ConstConfigOptionProxy>>>(
            const OptionType& lhs, const OptionType& rhs) {
      return absl::StrCat("'", lhs.getConfigOption().getIdentifier(),
                          "' must be smaller than, or equal to, "
                          "'",
                          rhs.getConfigOption().getIdentifier(), "'.");
    };

    // Adding the validators.

    /*
    Is `max-memory` big enough, to even allow for minimum amount of row of the
    smaller table, bigger table, or the table resulting from joining the smaller
    and bigger table?
    If not, return an error message.
    */
    auto checkIfMaxMemoryBigEnoughForMinNumRows =
        [&maxMemoryInStringFormat](const ad_utility::MemorySize maxMemory,
                                   const std::string_view tableName,
                                   const size_t numRows,
                                   const size_t numColumns)
        -> std::optional<ad_utility::ErrorMessage> {
      const ad_utility::MemorySize memoryNeededForMinNumRows =
          approximateMemoryNeededByIdTable(numRows, numColumns);
      // Remember: `0` is for unlimited memory.
      if (maxMemory == 0_B || memoryNeededForMinNumRows <= maxMemory) {
        return std::nullopt;
      } else {
        return std::make_optional<ad_utility::ErrorMessage>(absl::StrCat(
            "'", maxMemoryInStringFormat.getConfigOption().getIdentifier(),
            "' (", maxMemory.asString(), ") must be big enough, for at least ",
            numRows, " rows in the ", tableName, ", which requires at least ",
            memoryNeededForMinNumRows.asString(), "."));
      }
    };
    config.addValidator(
        [checkIfMaxMemoryBigEnoughForMinNumRows](std::string_view maxMemory,
                                                 size_t smallerTableNumColumns,
                                                 size_t minNumRows) {
          return checkIfMaxMemoryBigEnoughForMinNumRows(
              ad_utility::MemorySize::parse(maxMemory), "smaller table",
              minNumRows, smallerTableNumColumns);
        },
        absl::StrCat("'",
                     maxMemoryInStringFormat.getConfigOption().getIdentifier(),
                     "' must be big enough, for at least the minimum number of "
                     "rows in the smaller table."),
        maxMemoryInStringFormat, smallerTableNumColumns, minSmallerTableRows);
    config.addValidator(
        [checkIfMaxMemoryBigEnoughForMinNumRows](std::string_view maxMemory,
                                                 size_t biggerTableNumColumns,
                                                 size_t minNumRows) {
          return checkIfMaxMemoryBigEnoughForMinNumRows(
              ad_utility::MemorySize::parse(maxMemory), "bigger table",
              minNumRows, biggerTableNumColumns);
        },
        absl::StrCat("'",
                     maxMemoryInStringFormat.getConfigOption().getIdentifier(),
                     "' must be big enough, for at least the minimum number of "
                     "rows in the bigger table."),
        maxMemoryInStringFormat, biggerTableNumColumns, minBiggerTableRows);
    config.addValidator(
        [checkIfMaxMemoryBigEnoughForMinNumRows](std::string_view maxMemory,
                                                 size_t smallerTableNumColumns,
                                                 size_t biggerTableNumColumns) {
          return checkIfMaxMemoryBigEnoughForMinNumRows(
              ad_utility::MemorySize::parse(maxMemory),
              "result of joining the smaller and bigger table", 1,
              smallerTableNumColumns + biggerTableNumColumns - 1);
        },
        absl::StrCat("'",
                     maxMemoryInStringFormat.getConfigOption().getIdentifier(),
                     "' must be big enough, for at least one row in the result "
                     "of joining the smaller and bigger table."),
        maxMemoryInStringFormat, smallerTableNumColumns, biggerTableNumColumns);

    // Are `min-smaller-table-rows` and `min-bigger-table-rows` big enough, to
    // deliver interesting measurements?
    const auto addValidatorEnoughRowsToBeInteresting =
        [&config, &generateBiggerEqualLambda, &generateBiggerEqualLambdaDesc](
            ad_utility::ConstConfigOptionProxy<size_t> option) {
          config.addValidator(
              generateBiggerEqualLambda(
                  option.getConfigOption().getDefaultValue<size_t>(), true),
              absl::StrCat("'", option.getConfigOption().getIdentifier(),
                           "' is to small. Interesting measurement values "
                           "start at ",
                           option.getConfigOption().getDefaultValueAsString(),
                           " rows, or more."),
              absl::StrCat(
                  generateBiggerEqualLambdaDesc(
                      option,
                      option.getConfigOption().getDefaultValueAsString(), true),
                  " Interesting measurement values only appear from that point "
                  "onwards."),
              option);
        };
    addValidatorEnoughRowsToBeInteresting(minSmallerTableRows);
    addValidatorEnoughRowsToBeInteresting(minBiggerTableRows);

    // Is `minSmallerTableRows` smaller, or equal, to `minBiggerTableRows`?
    config.addValidator(
        lessEqualLambda,
        generateLessEqualLambdaDesc(minSmallerTableRows, minBiggerTableRows),
        generateLessEqualLambdaDesc(minSmallerTableRows, minBiggerTableRows),
        minSmallerTableRows, minBiggerTableRows);

    // Is `minBiggerTableRows` smaller, or equal, to `max-bigger-table-rows`?
    config.addValidator(
        lessEqualLambda,
        generateLessEqualLambdaDesc(minBiggerTableRows, maxBiggerTableRows),
        generateLessEqualLambdaDesc(minBiggerTableRows, maxBiggerTableRows),
        minBiggerTableRows, maxBiggerTableRows);

    // Do we have at least 1 column?
    config.addValidator(
        generateBiggerEqualLambda(1UL, true),
        generateBiggerEqualLambdaDesc(smallerTableNumColumns, 1UL, true),
        generateBiggerEqualLambdaDesc(smallerTableNumColumns, 1UL, true),
        smallerTableNumColumns);
    config.addValidator(
        generateBiggerEqualLambda(1UL, true),
        generateBiggerEqualLambdaDesc(biggerTableNumColumns, 1UL, true),
        generateBiggerEqualLambdaDesc(biggerTableNumColumns, 1UL, true),
        biggerTableNumColumns);

    // Is `overlap-chance` bigger than 0?
    config.addValidator(
        generateBiggerEqualLambda(0.f, false),
        generateBiggerEqualLambdaDesc(overlapChance, 0.f, false),
        generateBiggerEqualLambdaDesc(overlapChance, 0.f, false),
        overlapChance);

    // Are the sample size ratios bigger than 0?
    config.addValidator(
        generateBiggerEqualLambda(0.f, false),
        generateBiggerEqualLambdaDesc(smallerTableSampleSizeRatio, 0.f, false),
        generateBiggerEqualLambdaDesc(smallerTableSampleSizeRatio, 0.f, false),
        smallerTableSampleSizeRatio);
    config.addValidator(
        generateBiggerEqualLambda(0.f, false),
        generateBiggerEqualLambdaDesc(biggerTableSampleSizeRatio, 0.f, false),
        generateBiggerEqualLambdaDesc(biggerTableSampleSizeRatio, 0.f, false),
        biggerTableSampleSizeRatio);
    const std::string benchmarkSampleSizeRatioBiggerThanZeroValidatorDesc{
        absl::StrCat(
            "All entries in '",
            benchmarkSampleSizeRatios.getConfigOption().getIdentifier(),
            "' must be bigger than, or equal to, 0.")};
    config.addValidator(
        [](const benchmarkSampleSizeRatiosValueType& vec) {
          return ql::ranges::all_of(
              vec,
              [](const benchmarkSampleSizeRatiosValueType::value_type ratio) {
                return ratio >= 0.f;
              });
        },
        benchmarkSampleSizeRatioBiggerThanZeroValidatorDesc,
        benchmarkSampleSizeRatioBiggerThanZeroValidatorDesc,
        benchmarkSampleSizeRatios);

    /*
    We later signal, that the row generation for the sample size ration tables
    should be stopped, by returning a float, that is the biggest entry in the
    vector plus 1.
    So, of course, that has to be possible.
    */
    const std::string benchmarkSampleSizeRatiosMaxSizeValidatorDesc{
        absl::StrCat(
            "All entries in '",
            benchmarkSampleSizeRatios.getConfigOption().getIdentifier(),
            "' must be smaller than, or equal to, ",
            getMaxValue<benchmarkSampleSizeRatiosValueType::value_type>() -
                static_cast<benchmarkSampleSizeRatiosValueType::value_type>(1),
            ".")};
    config.addValidator(
        [](const benchmarkSampleSizeRatiosValueType& vec) {
          return ql::ranges::max(vec) <=
                 getMaxValue<benchmarkSampleSizeRatiosValueType::value_type>() -
                     1.f;
        },
        benchmarkSampleSizeRatiosMaxSizeValidatorDesc,
        benchmarkSampleSizeRatiosMaxSizeValidatorDesc,
        benchmarkSampleSizeRatios);

    /*
    Is `randomSeed_` smaller/equal than the max value for seeds?
    Note: The static cast is needed, because a random generator seed is always
    `unsigned int`.
    */
    const std::string randomSeedMaxSizeValidatorDesc{
        absl::StrCat("'", randomSeed.getConfigOption().getIdentifier(),
                     "' must be smaller than, or equal to, ",
                     ad_utility::RandomSeed::max().get(), ".")};
    config.addValidator(
        [maxSeed = static_cast<randomSeedValueType>(
             ad_utility::RandomSeed::max().get())](
            const randomSeedValueType& seed) { return seed <= maxSeed; },
        randomSeedMaxSizeValidatorDesc, randomSeedMaxSizeValidatorDesc,
        randomSeed);

    // Is `max-time-single-measurement` a positive number?
    config.addValidator(
        generateBiggerEqualLambda(0.f, true),
        generateBiggerEqualLambdaDesc(maxTimeSingleMeasurement, 0.f, true),
        generateBiggerEqualLambdaDesc(maxTimeSingleMeasurement, 0.f, true),
        maxTimeSingleMeasurement);

    // Is the min ratio of rows bigger than the default?
    config.addValidator(
        generateBiggerEqualLambda(
            minRatioRows.getConfigOption().getDefaultValue<float>(), true),
        generateBiggerEqualLambdaDesc(
            minRatioRows,
            minRatioRows.getConfigOption().getDefaultValue<float>(), true),
        generateBiggerEqualLambdaDesc(
            minRatioRows,
            minRatioRows.getConfigOption().getDefaultValue<float>(), true),
        minRatioRows);

    // Is `min-ratio-rows` smaller than `max-ratio-rows`?
    config.addValidator(std::less_equal<float>{},
                        generateLessEqualLambdaDesc(minRatioRows, maxRatioRows),
                        generateLessEqualLambdaDesc(minRatioRows, maxRatioRows),
                        minRatioRows, maxRatioRows);

    /*
    Is the row ratio needed for the combination of `min-smaller-table-rows` and
    `min-bigger-table-rows` inside the range set via `min-ratio-rows` and
    `max-ratio-rows`?
    */
    const std::string minTableRowNumPossibleDesc{absl::StrCat(
        "The row ratio needed for a bigger table with ",
        minBiggerTableRows.getConfigOption().getValueAsString(), " ('",
        minBiggerTableRows.getConfigOption().getIdentifier(),
        "') rows, when the smaller table has ",
        minSmallerTableRows.getConfigOption().getValueAsString(), " rows ('",
        minSmallerTableRows.getConfigOption().getIdentifier(),
        "'), must be in the range described via '",
        minRatioRows.getConfigOption().getIdentifier(), "' (",
        minRatioRows.getConfigOption().getValueAsString(), ") and '",
        maxRatioRows.getConfigOption().getIdentifier(), "' (",
        maxRatioRows.getConfigOption().getValueAsString(), ").")};
    config.addValidator(
        [](const size_t numRowsSmallerTable, const size_t numRowsBiggerTable,
           const float minRatio, const float maxRatio) {
          const float neededRatio{
              static_cast<float>(static_cast<double>(numRowsBiggerTable) /
                                 static_cast<double>(numRowsSmallerTable))};
          // Checking for equality is more effort.
          return !(neededRatio < minRatio || maxRatio < neededRatio);
        },
        minTableRowNumPossibleDesc, minTableRowNumPossibleDesc,
        minSmallerTableRows, minBiggerTableRows, minRatioRows, maxRatioRows);

    // Can the options be cast to double, while keeping their values? (Needed
    // for calculations later.)
    const auto addCastableValidator =
        [&config](ad_utility::ConstConfigOptionProxy<size_t> option) {
          /*
          As far as I know, it's compiler dependent, which type is bigger:
          `size_t` or `double`.
          */
          const std::string descriptor{absl::StrCat(
              "'", option.getConfigOption().getIdentifier(),
              "' must preserve its value when being cast to 'double'.")};
          config.addValidator(
              [](const auto& val) {
                return isValuePreservingCast<double>(val);
              },
              descriptor, descriptor, option);
        };
    ql::ranges::for_each(std::vector{minBiggerTableRows, maxBiggerTableRows,
                                     minSmallerTableRows},
                         addCastableValidator);
  }

  /*
  @brief Add metadata information about the class, that is always interesting
  and not dependent on the created `ResultTable`.
  */
  void addDefaultMetadata(BenchmarkMetadata* meta) const {
    // Information, that is interesting for all the benchmarking classes.
    meta->addKeyValuePair("random-seed",
                          getConfigVariables().randomSeed().get());
    meta->addKeyValuePair("smaller-table-num-columns",
                          getConfigVariables().smallerTableNumColumns_);
    meta->addKeyValuePair("bigger-table-num-columns",
                          getConfigVariables().biggerTableNumColumns_);

    /*
    Add metadata information about:
    - "max-time-single-measurement"
    - "maxMemory"
    */
    auto addInfiniteWhen0 = [&meta](const std::string& name,
                                    const auto& value) {
      if (value != 0) {
        meta->addKeyValuePair(name, value);
      } else {
        meta->addKeyValuePair(name, "infinite");
      }
    };
    addInfiniteWhen0("max-time-single-measurement",
                     configVariables_.maxTimeSingleMeasurement_);
    addInfiniteWhen0("max-memory",
                     getConfigVariables().maxMemory().value_or(0_B).getBytes());
  }

  // A default `stopFunction` for `makeGrowingBenchmarkTable`, that takes
  // everything and always returns false.
  static constexpr auto alwaysFalse = [](const auto&...) { return false; };

  /*
  @brief Create a benchmark table for join algorithm, with the given
  parameters for the IdTables, which will keep getting more rows, until the
  `max-time-single-measurement` getter value, the `maxMemoryBiggerTable()`
  getter value, or the `maxMemory()` getter value of the configuration options
  was reached/surpassed. It will additionally stop, iff `stopFunction` returns
  true. The columns will be:
  - Return values of the parameter, you gave a function for.
  - Time needed for sorting `IdTable`s.
  - Time needed for merge/galloping join.
  - Time needed for sorting and merge/galloping added together.
  - Time needed for the hash join.
  - How many rows the result of joining the tables has.
  - How much faster the hash join is. For example: Two times faster.

  @tparam T1, T2, T6, T7 Must be a float, or a function, that takes the row
  number of the next to be generated row as `const size_t&`, and returns a
  float. Can only be a function, if all other template `T` parameter are
  vectors.
  @tparam T3, T4, T5 Must be a size_t, or a function, that takes the row
  number of the next to be generated row as `const size_t&`, and returns a
  size_t. Can only be a function, if all other template `T` parameter are
  vectors.

  @param results The `BenchmarkResults`, in which you want to create a new
  benchmark table.
  @param tableDescriptor A identifier for the to be created benchmark table, so
  that it can be easier identified later.
  @param parameterName The names of the parameter, you gave a growth function
  for. Will be set as the name of the column, which will show the returned
  values of the growth function.
  @param stopFunction Before generating the next row, this function will be
  called with the values of the function call arguments `overlap`, `ratioRows`,
  `smallerTableNumRows`, `smallerTableNumColumns`, `biggerTableNumColumns`,
  `smallerTableJoinColumnSampleSizeRatio` and
  `biggerTableJoinColumnSampleSizeRatio` for the to be generated row. Iff. this
  function returns true, the row will not be generated and the table generation
  stopped.
  @param overlap The height of the probability for any distinct join column
  element of smallerTable to be overwritten by a random disintct join column
  element of biggerTable. That is, all the occurrences of this element.
  @param resultTableNumRows How many rows the result of joining the two
  generated tables should have. The function argument `overlap` will be ignored,
  if a value for this parameter was given. Note: The actual result table size
  will be as close as possible to the wanted size, but it can not be guaranteed
  to be exactly the same.
  @param randomSeed Seed for the random generators.
  @param smallerTableSorted, biggerTableSorted Should the bigger/smaller table
  be sorted by his join column before being joined? More specifically, some
  join algorithm require one, or both, of the IdTables to be sorted. If this
  argument is false, the time needed for sorting the required table will
  added to the time of the join algorithm.
  @param ratioRows How many more rows than the smaller table should the
  bigger table have? In more mathematical words: Number of rows of the
  bigger table divided by the number of rows of the smaller table is equal
  to `ratioRows`.
  @param smallerTableNumRows How many rows should the smaller table have?
  @param smallerTableNumColumns, biggerTableNumColumns How many columns
  should the bigger/smaller tables have?
  @param smallerTableJoinColumnSampleSizeRatio,
  biggerTableJoinColumnSampleSizeRatio The join column of the tables normally
  get random entries out of a sample space with the same amount of possible
  numbers as there are rows in the table. (With every number having the same
  chance to be picked.) This adjusts the number of elements in the sample size
  to `Amount of rows * ratio`, which affects the possibility of duplicates.
   */
  template <QL_CONCEPT_OR_TYPENAME(ad_utility::InvocableWithExactReturnType<
                                   bool, float, size_t, size_t, size_t, size_t,
                                   float, float>) StopFunction,
            isTypeOrGrowthFunction<float> T1, isTypeOrGrowthFunction<float> T2,
            isTypeOrGrowthFunction<size_t> T3,
            isTypeOrGrowthFunction<size_t> T4,
            isTypeOrGrowthFunction<size_t> T5,
            isTypeOrGrowthFunction<float> T6 = float,
            isTypeOrGrowthFunction<float> T7 = float>
  requires exactlyOneGrowthFunction<T1, T2, T3, T4, T5, T6, T7>
  ResultTable& makeGrowingBenchmarkTable(
      BenchmarkResults* results, const std::string& tableDescriptor,
      std::string parameterName, StopFunction stopFunction, const T1& overlap,
      const std::optional<size_t>& resultTableNumRows,
      ad_utility::RandomSeed randomSeed, const bool smallerTableSorted,
      const bool biggerTableSorted, const T2& ratioRows,
      const T3& smallerTableNumRows, const T4& smallerTableNumColumns,
      const T5& biggerTableNumColumns,
      const T6& smallerTableJoinColumnSampleSizeRatio,
      const T7& biggerTableJoinColumnSampleSizeRatio) const {
    constexpr IsGrowthFunction isGrowthFunction;
    // Returns the first argument, that is a growth function.
    auto returnFirstGrowthFunction =
        [&isGrowthFunction](auto&... args) -> auto& {
      // Put them into a tuple, so that we can easily look them up.
      auto tup = std::tuple<decltype(args)...>{AD_FWD(args)...};

      // Get the index of the first growth function.
      constexpr static size_t idx = ad_utility::getIndexOfFirstTypeToPassCheck<
          isGrowthFunction, std::decay_t<decltype(args)>...>();

      // Do we have a valid index?
      static_assert(idx < sizeof...(args),
                    "There was no growth function in this parameter pack.");

      return std::get<idx>(tup);
    };

    /*
    @brief Call the growth function with the number of the next row to be
    created, if it's a function, and return the result. Otherwise  return the
    given `possibleGrowthFunction`.
    */
    auto returnOrCall = [&isGrowthFunction](const auto& possibleGrowthFunction,
                                            const size_t nextRowIdx) {
      using T = std::decay_t<decltype(possibleGrowthFunction)>;
      if constexpr (isGrowthFunction.template operator()<T>()) {
        return possibleGrowthFunction(nextRowIdx);
      } else {
        return possibleGrowthFunction;
      }
    };

    // For creating a new random seed for every new row.
    RandomSeedGenerator seedGenerator{std::move(randomSeed)};

    ResultTable& table = initializeBenchmarkTable(results, tableDescriptor,
                                                  std::move(parameterName));

    /*
    Adding measurements to the table, as long as possible.
    */
    while (true) {
      // What's the row number of the next to be added row?
      const size_t rowIdx = table.numRows();

      // Does the addition to the table work?
      if (!addNewRowToBenchmarkTable(
              &table,
              returnFirstGrowthFunction(
                  overlap, ratioRows, smallerTableNumRows,
                  smallerTableNumColumns, biggerTableNumColumns,
                  smallerTableJoinColumnSampleSizeRatio,
                  biggerTableJoinColumnSampleSizeRatio)(rowIdx),
              stopFunction, returnOrCall(overlap, rowIdx), resultTableNumRows,
              std::invoke(seedGenerator), smallerTableSorted, biggerTableSorted,
              returnOrCall(ratioRows, rowIdx),
              returnOrCall(smallerTableNumRows, rowIdx),
              returnOrCall(smallerTableNumColumns, rowIdx),
              returnOrCall(biggerTableNumColumns, rowIdx),
              returnOrCall(smallerTableJoinColumnSampleSizeRatio, rowIdx),
              returnOrCall(biggerTableJoinColumnSampleSizeRatio, rowIdx))) {
        break;
      }
    }
    addPostMeasurementInformationsToBenchmarkTable(&table);
    return table;
  }

 protected:
  /*
  @brief Initialize a new `ResultTable`, without any rows, in the given
  `BenchmarkResults` for use with the `makeGrowingBenchmarkTable` function.

  @param tableDescriptor The descriptor for the initialized table.
  @param changingParameterColumnDesc Name/Description for the first column.
  */
  static ResultTable& initializeBenchmarkTable(
      BenchmarkResults* results, const std::string& tableDescriptor,
      std::string changingParameterColumnDesc) {
    return results->addTable(
        tableDescriptor, {},
        {std::move(changingParameterColumnDesc), "Time for sorting",
         "Merge/Galloping join", "Sorting + merge/galloping join", "Hash join",
         "Number of rows in resulting IdTable", "Speedup of hash join"});
  };

  /*
  @brief Set the columns
  `GeneratedTableColumn::TimeForSortingAndMergeGallopingJoin` and
  `GeneratedTableColumn::JoinAlgorithmSpeedup` based on the content of measured
  execution time columns. Requires the columns with the measurements to hold no
  empty entries.
  */
  static void addPostMeasurementInformationsToBenchmarkTable(
      ResultTable* table) {
    using enum GeneratedTableColumn;
    // Adding together the time for sorting the `IdTables` and then joining
    // them via merge/galloping join.
    sumUpColumns(
        table,
        ColumnNumWithType<float>{
            toUnderlying(TimeForSortingAndMergeGallopingJoin)},
        ColumnNumWithType<float>{toUnderlying(TimeForSorting)},
        ColumnNumWithType<float>{toUnderlying(TimeForMergeGallopingJoin)});

    // Calculate, how much of a speedup the hash join algorithm has in
    // comparison to the merge/galloping join algorithm.
    calculateSpeedupOfColumn(
        table, {toUnderlying(JoinAlgorithmSpeedup)},
        {toUnderlying(TimeForHashJoin)},
        {toUnderlying(TimeForSortingAndMergeGallopingJoin)});
  }

  /*
  @brief Add a new row to the benchmark table in `makeGrowingBenchmarkTable` and
  set the function time measurements (join algorithms and sorting) in that row.
  If setting the function time measurements was not successful, then the row
  will not be added. For an explanation of the parameters, see
  `makeGrowingBenchmarkTable`.

  @param changingParameterValue What value to set the first column in the new
  row.

  @return True, if the row and all measurements were added. False, if the
  addition of the row and the measurements was stopped, because either a
  `IdTable` required more memory than was allowed via configuration options, a
  single measurement took longer than was allowed via configuration options, or
  the stop function returned `true`.
  */
  bool addNewRowToBenchmarkTable(
      ResultTable* table,
      const QL_CONCEPT_OR_NOTHING(
          ad_utility::SameAsAny<float, size_t>) auto changingParameterValue,
      QL_CONCEPT_OR_NOTHING(ad_utility::InvocableWithExactReturnType<
                            bool, float, size_t, size_t, size_t, size_t, float,
                            float>) auto stopFunction,
      const float overlap, const std::optional<size_t>& resultTableNumRows,
      ad_utility::RandomSeed randomSeed, const bool smallerTableSorted,
      const bool biggerTableSorted, const float& ratioRows,
      const size_t& smallerTableNumRows, const size_t& smallerTableNumColumns,
      const size_t& biggerTableNumColumns,
      const float smallerTableJoinColumnSampleSizeRatio,
      const float biggerTableJoinColumnSampleSizeRatio) const {
    /*
    Checking, if `smallerTableJoinColumnSampleSizeRatio` and
    `biggerTableJoinColumnSampleSizeRatio` are floats bigger than 0.
    Otherwise , they don't make sense.
    */
    AD_CONTRACT_CHECK(smallerTableJoinColumnSampleSizeRatio > 0);
    AD_CONTRACT_CHECK(biggerTableJoinColumnSampleSizeRatio > 0);

    // Make sure, that things can be casted (in later calculations) without
    // changing values.
    AD_CORRECTNESS_CHECK(isValuePreservingCast<double>(smallerTableNumRows));

    // Nothing to do, if the stop function returns true.
    if (std::invoke(stopFunction, overlap, ratioRows, smallerTableNumRows,
                    smallerTableNumColumns, biggerTableNumColumns,
                    smallerTableJoinColumnSampleSizeRatio,
                    biggerTableJoinColumnSampleSizeRatio)) {
      return false;
    }

    /*
    Check if the smaller and bigger `IdTable` are not to big. Size of the
    result table is only checked, if the configuration option for it was set.
    */
    if (const auto& maxSizeInputTable{
            getConfigVariables().maxMemoryBiggerTable()};
        approximateMemoryNeededByIdTable(
            smallerTableNumRows, smallerTableNumColumns) > maxSizeInputTable ||
        approximateMemoryNeededByIdTable(
            static_cast<size_t>(static_cast<double>(smallerTableNumRows) *
                                ratioRows),
            biggerTableNumColumns) > maxSizeInputTable) {
      return false;
    }

    // The lambdas for the join algorithms.
    auto hashJoinLambda = makeHashJoinLambda();
    auto joinLambda = makeJoinLambda();

    /*
    Create new `IdTable`s.
    First we calculate the value boundaries for the join column entries. These
    are needed for the creation of randomly filled tables.
    Reminder: The $-1$ in the upper bounds is, because a range [a, b]
    of natural numbers has $b - a + 1$ elements.
    */
    const size_t smallerTableJoinColumnLowerBound = 0;
    /*
    Check for overflow, before calculating `smallerTableJoinColumnUpperBound`.

    Short math explanation: Because `size_t` is bigger than `float`,
    `static_cast<size_t>(...) - 1` can never be cause for an overflow. Because
    `smallerTableJoinColumnSampleSizeRatio` is always bigger than `0`,
    `static_cast<size_t>(std::ceil(...))` is also always `>=1` and can never
    cause an underflow. So, `static_cast<size_t>(...) - 1` can be safely
    ignored.

    `std::ceil(...)` however, can have a overflow.
    If there is no overflow
    `std::ceil(static_cast<float>(smallerTableNumRows) *
    smallerTableJoinColumnSampleSizeRatio) <=
    std::floor(getMaxValue<float>())` must be true. Which is
    true, iff, `static_cast<float>(smallerTableNumRows) *
    smallerTableJoinColumnSampleSizeRatio <=
    std::floor(getMaxValue<float>())` is true.
    We negate the second clause, transform it into an overflow safe expression
    and get our if clause.
    */
    if (static_cast<double>(smallerTableNumRows) >
        std::floor(getMaxValue<double>()) /
            smallerTableJoinColumnSampleSizeRatio) {
      throwOverflowError<double>(
          absl::StrCat("multiplication of the number of smaller table rows (",
                       smallerTableNumRows,
                       ") with 'smaller-table-join-column-sample-size-ratio' (",
                       smallerTableJoinColumnSampleSizeRatio, ")"));
    }
    const size_t smallerTableJoinColumnUpperBound =
        static_cast<size_t>(std::ceil(static_cast<double>(smallerTableNumRows) *
                                      smallerTableJoinColumnSampleSizeRatio)) -
        1;

    // Check for overflow.
    if (smallerTableJoinColumnUpperBound >
        getMaxValue<decltype(smallerTableJoinColumnUpperBound)>() - 1) {
      throwOverflowError<double>(
          absl::StrCat("multiplication of the number of smaller table rows (",
                       smallerTableNumRows,
                       ") with 'smaller-table-join-column-sample-size-ratio' (",
                       smallerTableJoinColumnSampleSizeRatio, "), plus 1,"));
    }
    const size_t biggerTableJoinColumnLowerBound =
        smallerTableJoinColumnUpperBound + 1;

    /*
    Check for overflow, before calculating `biggerTableJoinColumnUpperBound`.
    Short math explanation: I check the intermediate results, before using
    them, and also use the same trick as with `smallerTableJoinColumnUpperBound`
    to check, that `std::ceil(...)` is neither overflow, nor underflow.
    */
    if (static_cast<double>(smallerTableNumRows) >
        getMaxValue<double>() / ratioRows) {
      throwOverflowError<double>(absl::StrCat(
          " the number of bigger table rows (",
          static_cast<double>(smallerTableNumRows) * ratioRows, ")"));
    } else if (static_cast<double>(smallerTableNumRows) * ratioRows >
               std::floor(getMaxValue<double>()) /
                   biggerTableJoinColumnSampleSizeRatio) {
      throwOverflowError<double>(
          absl::StrCat("multiplication of the number of bigger table rows (",
                       static_cast<double>(smallerTableNumRows) * ratioRows,
                       ") with 'bigger-table-join-column-sample-size-ratio' (",
                       biggerTableJoinColumnSampleSizeRatio, ")"));
    } else if (biggerTableJoinColumnLowerBound - 1 >
               getMaxValue<size_t>() -
                   static_cast<size_t>(std::ceil(
                       static_cast<double>(smallerTableNumRows) * ratioRows *
                       biggerTableJoinColumnSampleSizeRatio))) {
      throw std::runtime_error(absl::StrCat(
          "size_t overflow error: The multiplication (rounded up) of the "
          "number of smaller table rows (",
          smallerTableNumRows,
          ") with 'smaller-table-join-column-sample-size-ratio' (",
          smallerTableJoinColumnSampleSizeRatio,
          "), minus 1, added to the multiplication (rounded up) of the "
          "number "
          "of bigger table rows (",
          static_cast<double>(smallerTableNumRows) * ratioRows,
          ") with 'bigger-table-join-column-sample-size-ratio' (",
          biggerTableJoinColumnSampleSizeRatio,
          ") is bigger than the size_t type maximum ", getMaxValue<size_t>(),
          ". Try reducing the values for the configuration options."));
    }
    const size_t biggerTableJoinColumnUpperBound =
        biggerTableJoinColumnLowerBound +
        static_cast<size_t>(
            std::ceil(static_cast<double>(smallerTableNumRows) * ratioRows *
                      biggerTableJoinColumnSampleSizeRatio)) -
        1;

    // Seeds for the random generators, so that things are less similar
    // between the tables.
    const std::array<ad_utility::RandomSeed, 5> seeds =
        createArrayOfRandomSeeds<5>(std::move(randomSeed));

    // Now we create two randomly filled `IdTable`, which have no overlap, and
    // save them together with the information, where their join column is.
    IdTableAndJoinColumn smallerTable{
        createRandomlyFilledIdTable(
            smallerTableNumRows, smallerTableNumColumns,
            JoinColumnAndBounds{0, smallerTableJoinColumnLowerBound,
                                smallerTableJoinColumnUpperBound, seeds.at(0)},
            seeds.at(1)),
        0};
    IdTableAndJoinColumn biggerTable{
        createRandomlyFilledIdTable(
            static_cast<size_t>(static_cast<double>(smallerTableNumRows) *
                                ratioRows),
            biggerTableNumColumns,
            JoinColumnAndBounds{0, biggerTableJoinColumnLowerBound,
                                biggerTableJoinColumnUpperBound, seeds.at(2)},
            seeds.at(3)),
        0};

    // Index of the row, that is being added.
    const size_t rowIdx{table->numRows()};
    table->addRow();
    table->setEntry(rowIdx,
                    toUnderlying(GeneratedTableColumn::ChangingParamter),
                    changingParameterValue);

    // The number of rows, that the joined `ItdTable`s end up having.
    size_t numRowsOfResult{0};

    /*
    Creating overlap, if wanted.
    Note: The value for `numRowsOfResult` is correct, because the content of
    the `IdTable`s is disjunct.
    */
    if (resultTableNumRows.has_value()) {
      numRowsOfResult = createOverlapRandomly(
          &smallerTable, biggerTable, resultTableNumRows.value(), seeds.at(4));
    } else if (overlap > 0) {
      numRowsOfResult = createOverlapRandomly(&smallerTable, biggerTable,
                                              overlap, seeds.at(4));
    }

    /*
    Check, if the size of the result table isn't to big, if the `maxMemory`
    configuration option was set.
    */
    if (const auto& maxMemory{getConfigVariables().maxMemory()};
        maxMemory.value_or(ad_utility::MemorySize::max()) <
        approximateMemoryNeededByIdTable(
            numRowsOfResult,
            smallerTableNumColumns + biggerTableNumColumns - 1)) {
      table->deleteRow(rowIdx);
      return false;
    }

    // Sort the `IdTables`, if they should be.
    if (smallerTableSorted) {
      sortIdTableByJoinColumnInPlace(smallerTable);
    }
    if (biggerTableSorted) {
      sortIdTableByJoinColumnInPlace(biggerTable);
    }
    const auto isOverMaxTime =
        [maxTime = getConfigVariables().maxTimeSingleMeasurement(), &table,
         &rowIdx](const GeneratedTableColumn& columnIdx) {
          /*
          Simply make the comparison trivial, if there was no maximum time
          set.
          */
          return table->getEntry<float>(rowIdx, toUnderlying(columnIdx)) >
                 maxTime.value_or(getMaxValue<decltype(maxTime)::value_type>());
        };

    /*
    Adding the benchmark measurements to the current row.

    Hash join first, because merge/galloping join sorts all tables, if
    needed, before joining them.
    */
    table->addMeasurement(
        rowIdx, toUnderlying(GeneratedTableColumn::TimeForHashJoin),
        [&smallerTable, &biggerTable, &hashJoinLambda]() {
          useJoinFunctionOnIdTables(smallerTable, biggerTable, hashJoinLambda);
        });
    if (isOverMaxTime(GeneratedTableColumn::TimeForHashJoin)) {
      table->deleteRow(rowIdx);
      return false;
    }

    /*
    The sorting of the `IdTables`. That must be done before the
    merge/galloping, otherwise their algorithm won't result in a correct
    result.
    */
    table->addMeasurement(rowIdx,
                          toUnderlying(GeneratedTableColumn::TimeForSorting),
                          [&smallerTable, &smallerTableSorted, &biggerTable,
                           &biggerTableSorted]() {
                            if (!smallerTableSorted) {
                              sortIdTableByJoinColumnInPlace(smallerTable);
                            }
                            if (!biggerTableSorted) {
                              sortIdTableByJoinColumnInPlace(biggerTable);
                            }
                          });
    if (isOverMaxTime(GeneratedTableColumn::TimeForSorting)) {
      table->deleteRow(rowIdx);
      return false;
    }

    // The merge/galloping join.
    table->addMeasurement(
        rowIdx, toUnderlying(GeneratedTableColumn::TimeForMergeGallopingJoin),
        [&smallerTable, &biggerTable, &joinLambda]() {
          useJoinFunctionOnIdTables(smallerTable, biggerTable, joinLambda);
        });
    if (isOverMaxTime(GeneratedTableColumn::TimeForMergeGallopingJoin)) {
      table->deleteRow(rowIdx);
      return false;
    }

    // Adding the number of rows of the result.
    table->setEntry(rowIdx,
                    toUnderlying(GeneratedTableColumn::NumRowsOfJoinResult),
                    numRowsOfResult);
    return true;
  }
};

/*
@brief Return a lambda function, which returns `prefixValues.at(rowIdx)` until
the vector is exhausted. After that, the lambda function calculates and returns
$base^(x+rowIdx-prefixValues.size())$. With $rowIdx$ being the single `size_t`
argument of the function and $x$ being $log_base(startingPoint)$ rounded up.


@tparam ReturnType The return type of the created lambda function.
*/
template <typename ReturnType>
requires std::convertible_to<ReturnType, double> &&
         std::convertible_to<double, ReturnType>
auto createDefaultGrowthLambda(const ReturnType& base,
                               const ReturnType& startingPoint,
                               std::vector<ReturnType> prefixValues = {}) {
  return [base, prefixValues = std::move(prefixValues),
          startingExponent{calculateNextWholeExponent(base, startingPoint)}](
             const size_t& rowIdx) {
    if (rowIdx < prefixValues.size()) {
      return prefixValues.at(rowIdx);
    } else {
      AD_CONTRACT_CHECK(isValuePreservingCast<double>(base));
      AD_CONTRACT_CHECK(
          isValuePreservingCast<double>(rowIdx - prefixValues.size()));
      AD_CONTRACT_CHECK(isValuePreservingCast<ReturnType>(
          std::pow(static_cast<double>(base),
                   startingExponent +
                       static_cast<double>(rowIdx - prefixValues.size()))));
      return static_cast<ReturnType>(
          std::pow(static_cast<double>(base),
                   startingExponent +
                       static_cast<double>(rowIdx - prefixValues.size())));
    }
  };
}

/*
Create benchmark tables, where the smaller table stays at the same amount of
rows and the bigger tables keeps getting bigger. Amount of columns stays the
same.
*/
class BmOnlyBiggerTableSizeChanges final
    : public GeneralInterfaceImplementation {
 public:
  std::string name() const override {
    return "Benchmarktables, where the smaller table stays at the same amount "
           "of rows and the bigger tables keeps getting bigger.";
  }

  BenchmarkResults runAllBenchmarks() override {
    BenchmarkResults results{};
    /*
    Making a benchmark table for all combination of IdTables being sorted and
    any possible number of rows for the smaller `IdTable`.
    */
    for (const bool smallerTableSorted : {false, true}) {
      for (const bool biggerTableSorted : {false, true}) {
        for (const size_t smallerTableNumRows : generateExponentInterval(
                 10UL, getConfigVariables().minSmallerTableRows_,
                 static_cast<size_t>(
                     static_cast<double>(
                         getConfigVariables().minBiggerTableRows_) /
                     getConfigVariables().minRatioRows_))) {
          const std::string& tableName =
              absl::StrCat("Smaller table stays at ", smallerTableNumRows,
                           " rows, ratio to rows of bigger table grows.");
          const float minRatio{static_cast<float>(std::ceil(
              static_cast<double>(getConfigVariables().minBiggerTableRows_) /
              static_cast<double>(smallerTableNumRows)))};
          auto growthFunction = createDefaultGrowthLambda<float>(
              10.f, ql::ranges::max(minRatio, 10.f),
              generateNaturalNumberSequenceInterval(minRatio, 9.f));
          ResultTable& table = makeGrowingBenchmarkTable(
              &results, tableName, "Row ratio", alwaysFalse,
              getConfigVariables().overlapChance_, std::nullopt,
              getConfigVariables().randomSeed(), smallerTableSorted,
              biggerTableSorted, growthFunction, smallerTableNumRows,
              getConfigVariables().smallerTableNumColumns_,
              getConfigVariables().biggerTableNumColumns_,
              getConfigVariables().smallerTableJoinColumnSampleSizeRatio_,
              getConfigVariables().biggerTableJoinColumnSampleSizeRatio_);

          // Add the metadata, that changes with every call and can't be
          // generalized.
          BenchmarkMetadata& meta = table.metadata();
          meta.addKeyValuePair("smaller-table-num-rows", smallerTableNumRows);
          meta.addKeyValuePair("smaller-table-sorted", smallerTableSorted);
          meta.addKeyValuePair("bigger-table-sorted", biggerTableSorted);
        }
      }
    }

    // Add the general metadata.
    BenchmarkMetadata& meta{getGeneralMetadata()};
    meta.addKeyValuePair("value-changing-with-every-row", "ratio-rows");
    meta.addKeyValuePair(
        "smaller-table-join-column-sample-size-ratio",
        getConfigVariables().smallerTableJoinColumnSampleSizeRatio_);
    meta.addKeyValuePair(
        "bigger-table-join-column-sample-size-ratio",
        getConfigVariables().biggerTableJoinColumnSampleSizeRatio_);
    meta.addKeyValuePair("overlap-chance", getConfigVariables().overlapChance_);
    GeneralInterfaceImplementation::addDefaultMetadata(&meta);

    return results;
  }
};

// Create benchmark tables, where the smaller table grows and the ratio
// between tables stays the same. As does the amount of columns.
class BmOnlySmallerTableSizeChanges final
    : public GeneralInterfaceImplementation {
 public:
  std::string name() const override {
    return "Benchmarktables, where the smaller table grows and the ratio "
           "between tables stays the same.";
  }

  BenchmarkResults runAllBenchmarks() override {
    BenchmarkResults results{};

    // Making a benchmark table for all combination of IdTables being sorted.
    for (const bool smallerTableSorted : {false, true}) {
      for (const bool biggerTableSorted : {false, true}) {
        // We also make multiple tables for different row ratios.
        for (const float ratioRows : mergeSortedVectors<float>(
                 {generateNaturalNumberSequenceInterval(
                      getConfigVariables().minRatioRows_,
                      ql::ranges::min(getConfigVariables().maxRatioRows_,
                                      10.f)),
                  generateExponentInterval(
                      10.f, getConfigVariables().minRatioRows_,
                      getConfigVariables().maxRatioRows_)})) {
          const std::string tableName = absl::StrCat(
              "The amount of rows in the smaller table grows and the ratio, to "
              "the amount of rows in the bigger table, stays at ",
              ratioRows, ".");

          // Returns the amount of rows in the smaller `IdTable`, used for the
          // measurements in a given row.
          auto growthFunction = createDefaultGrowthLambda(
              10UL, ql::ranges::max(
                        static_cast<size_t>(
                            static_cast<double>(
                                getConfigVariables().minBiggerTableRows_) /
                            ratioRows),
                        getConfigVariables().minSmallerTableRows_));

          ResultTable& table = makeGrowingBenchmarkTable(
              &results, tableName, "Amount of rows in the smaller table",
              alwaysFalse, getConfigVariables().overlapChance_, std::nullopt,
              getConfigVariables().randomSeed(), smallerTableSorted,
              biggerTableSorted, ratioRows, growthFunction,
              getConfigVariables().smallerTableNumColumns_,
              getConfigVariables().biggerTableNumColumns_,
              getConfigVariables().smallerTableJoinColumnSampleSizeRatio_,
              getConfigVariables().biggerTableJoinColumnSampleSizeRatio_);

          // Add the metadata, that changes with every call and can't be
          // generalized.
          BenchmarkMetadata& meta = table.metadata();
          meta.addKeyValuePair("smaller-table-sorted", smallerTableSorted);
          meta.addKeyValuePair("bigger-table-sorted", biggerTableSorted);
          meta.addKeyValuePair("ratio-rows", ratioRows);
        }
      }
    }

    // Add the general metadata.
    BenchmarkMetadata& meta{getGeneralMetadata()};
    meta.addKeyValuePair("value-changing-with-every-row",
                         "smaller-table-num-rows");
    meta.addKeyValuePair(
        "smaller-table-join-column-sample-size-ratio",
        getConfigVariables().smallerTableJoinColumnSampleSizeRatio_);
    meta.addKeyValuePair(
        "bigger-table-join-column-sample-size-ratio",
        getConfigVariables().biggerTableJoinColumnSampleSizeRatio_);
    meta.addKeyValuePair("overlap-chance", getConfigVariables().overlapChance_);
    GeneralInterfaceImplementation::addDefaultMetadata(&meta);
    return results;
  }
};

// Create benchmark tables, where the tables are the same size and
// both just get more rows.
class BmSameSizeRowGrowth final : public GeneralInterfaceImplementation {
 public:
  std::string name() const override {
    return "Benchmarktables, where the tables are the same size and both just "
           "get more rows.";
  }

  BenchmarkResults runAllBenchmarks() override {
    BenchmarkResults results{};
    const std::string tableName =
        "Both tables always have the same amount of rows and that amount "
        "grows.";

    // Returns the amount of rows in the smaller `IdTable`, used for the
    // measurements in a given row.
    auto growthFunction = createDefaultGrowthLambda(
        10UL, getConfigVariables().minBiggerTableRows_);

    // Making a benchmark table for all combination of IdTables being sorted.
    for (const bool smallerTableSorted : {false, true}) {
      for (const bool biggerTableSorted : {false, true}) {
        ResultTable& table = makeGrowingBenchmarkTable(
            &results, tableName, "Amount of rows", alwaysFalse,
            getConfigVariables().overlapChance_, std::nullopt,
            getConfigVariables().randomSeed(), smallerTableSorted,
            biggerTableSorted, 1.f, growthFunction,
            getConfigVariables().smallerTableNumColumns_,
            getConfigVariables().biggerTableNumColumns_,
            getConfigVariables().smallerTableJoinColumnSampleSizeRatio_,
            getConfigVariables().biggerTableJoinColumnSampleSizeRatio_);

        // Add the metadata, that changes with every call and can't be
        // generalized.
        BenchmarkMetadata& meta = table.metadata();
        meta.addKeyValuePair("smaller-table-sorted", smallerTableSorted);
        meta.addKeyValuePair("bigger-table-sorted", biggerTableSorted);
      }
    }

    // Add the general metadata.
    BenchmarkMetadata& meta{getGeneralMetadata()};
    meta.addKeyValuePair("value-changing-with-every-row",
                         "smaller-table-num-rows");
    meta.addKeyValuePair("ratio-rows", 1);
    meta.addKeyValuePair(
        "smaller-table-join-column-sample-size-ratio",
        getConfigVariables().smallerTableJoinColumnSampleSizeRatio_);
    meta.addKeyValuePair(
        "bigger-table-join-column-sample-size-ratio",
        getConfigVariables().biggerTableJoinColumnSampleSizeRatio_);
    meta.addKeyValuePair("overlap-chance", getConfigVariables().overlapChance_);
    GeneralInterfaceImplementation::addDefaultMetadata(&meta);
    return results;
  }
};

// Create benchmark tables, where the tables are the same size and
// only the number of possible elements in their join column change.
class BmSampleSizeRatio final : public GeneralInterfaceImplementation {
 public:
  std::string name() const override {
    return "Benchmarktables, where only the sample size ratio changes.";
  }

  BenchmarkResults runAllBenchmarks() override {
    BenchmarkResults results{};
    const auto& ratios{getConfigVariables().benchmarkSampleSizeRatios_};
    const float maxSampleSizeRatio{ql::ranges::max(ratios)};

    /*
    We work with the biggest possible smaller and bigger table. That should make
    any difference in execution time easier to find.
    Note: Strings are for the generation of error messages.
    */
    const float ratioRows{getConfigVariables().minRatioRows_};
    constexpr std::string_view ratioRowsDescription{"'min-ratio-rows'"};
    const size_t smallerTableNumRows{
        static_cast<size_t>(static_cast<double>(approximateNumIdTableRows(
                                getConfigVariables().maxMemoryBiggerTable(),
                                getConfigVariables().biggerTableNumColumns_)) /
                            getConfigVariables().minRatioRows_)};
    std::string smallerTableNumRowsDescription{};
    std::string smallerTableNumRowsConfigurationOptions{};
    if (getConfigVariables().maxMemory().has_value()) {
      smallerTableNumRowsDescription =
          "division of the maximum number of rows, under the given "
          "'max-memory' "
          "and 'bigger-table-num-columns', with 'min-ratio-rows'";
      smallerTableNumRowsConfigurationOptions =
          "'max-memory' and 'bigger-table-num-columns'";
    } else {
      smallerTableNumRowsDescription =
          "division of 'max-bigger-table-rows' with 'min-ratio-rows'";
      smallerTableNumRowsConfigurationOptions = "'max-bigger-table-rows'";
    }

    /*
    Growth function. Simply walks through the sample size ratios, until it runs
    out of them. Then it returns the biggest sample size ratio plus 1.
    */
    const auto growthFunction = [&ratios,
                                 &maxSampleSizeRatio](const size_t& rowIdx) {
      if (rowIdx < ratios.size()) {
        return ratios.at(rowIdx);
      } else {
        return maxSampleSizeRatio + 1.f;
      }
    };

    // Stop, if we run out of sample size ratios.
    const auto stopFunction = [&maxSampleSizeRatio](
                                  float, size_t, size_t, size_t, size_t,
                                  float smallerTableJoinColumnSampleSizeRatio,
                                  float biggerTableJoinColumnSampleSizeRatio) {
      return !(smallerTableJoinColumnSampleSizeRatio <= maxSampleSizeRatio &&
               biggerTableJoinColumnSampleSizeRatio <= maxSampleSizeRatio);
    };

    /*
    Calculate the expected number of rows in the result for the simplified
    creation model of input tables join columns and overlaps, with the biggest
    sample size ratio used for both input tables. The simplified creation model
    assumes, that:
    - The join column elements of the smaller and bigger table are not disjoint
    at creation time. In reality, both join columns are created disjoint and any
    overlaps are inserted later.
    - The join column entries of the smaller table have a uniform distribution,
    are made up out of the join column elements of both tables (smaller and
    bigger) and the generation of one row entry is independent from the
    generation of all other row entries.
    - The join column entries of the bigger table have a uniform distribution,
    are made up out of only the elements of the bigger tables and the generation
    of one row entry is independent from the generation of all other row
    entries.
    - The generation of join column entries in the smaller table is independent
    from the generation in the bigger table.

    Note: In reality, the set of possible join column entries for the smaller
    table has size `|Number of rows in the smaller table| * sampleSizeRatio`
    (rounded up) possible elements for entries. `|Number of rows in the bigger
    table| * sampleSizeRatio` (rounded up) for the bigger table.

    In this simplified creation model the expected number of rows in the result
    is:
    (|Number of rows in the smaller table|*|Number of rows in the bigger table|)
    / (|Number of rows in the smaller table|*sampleSizeRatio(rounded up)+|Number
    of rows in the bigger table|*sampleSizeRatio(rounded up))
    */
    if (static_cast<double>(smallerTableNumRows) >
        getMaxValue<double>() / static_cast<double>(smallerTableNumRows)) {
      throwOverflowError<double>(absl::StrCat(smallerTableNumRowsDescription,
                                              " ,multiplied with itself,"));
    } else if (static_cast<double>(smallerTableNumRows * smallerTableNumRows) >
               getMaxValue<double>() / static_cast<double>(ratioRows)) {
      throwOverflowError<double>(
          absl::StrCat(smallerTableNumRowsDescription,
                       " ,multiplied with itself, and then multiplied with ",
                       ratioRowsDescription));
    } else if (static_cast<double>(smallerTableNumRows) >
               getMaxValue<double>() / maxSampleSizeRatio) {
      throwOverflowError<double>(
          absl::StrCat(smallerTableNumRowsDescription,
                       " ,multiplied with the biggest entry in "
                       "'benchmark-sample-size-ratios'"));
    } else if (static_cast<double>(smallerTableNumRows) *
                   static_cast<double>(ratioRows) >
               getMaxValue<double>() / maxSampleSizeRatio) {
      throwOverflowError<double>(
          absl::StrCat(smallerTableNumRowsDescription,
                       " ,multiplied with the biggest entry in "
                       "'benchmark-sample-size-ratios' and ",
                       ratioRowsDescription));
    } else if (std::ceil(static_cast<double>(smallerTableNumRows) *
                         maxSampleSizeRatio) >
               getMaxValue<double>() -
                   std::ceil(static_cast<double>(smallerTableNumRows) *
                             static_cast<double>(ratioRows) *
                             maxSampleSizeRatio)) {
      throwOverflowError<double>(absl::StrCat(
          "addition of the ", smallerTableNumRowsDescription,
          ",multiplied with the biggest entry in "
          "'benchmark-sample-size-ratios', to the ",
          smallerTableNumRowsDescription, ", multiplied with ",
          ratioRowsDescription,
          " and the biggest entry in 'benchmark-sample-size-ratios',"));
    } else if (!isValuePreservingCast<size_t>(std::floor(
                   static_cast<double>(smallerTableNumRows) *
                   static_cast<double>(smallerTableNumRows) * ratioRows /
                   (std::ceil(static_cast<double>(smallerTableNumRows) *
                              maxSampleSizeRatio) +
                    std::ceil(static_cast<double>(smallerTableNumRows) *
                              ratioRows * maxSampleSizeRatio))))) {
      throw std::runtime_error(absl::StrCat(
          "size_t casting error: The calculated wanted result size in '",
          name(),
          "' has to be castable to size_t. Try increasing the values for the "
          "configuration options 'min-ratio-rows' or the biggest entry in "
          "'benchmark-sample-size-ratios'. Or decreasing the value of ",
          smallerTableNumRowsConfigurationOptions, "."));
    }
    /*
    If 'maxMemory' was set and our normal result is to big, we simply calculate
    the number of rows, rounded down, that a result with the maximum memory size
    would have.
    */
    auto resultWantedNumRows{static_cast<size_t>(
        static_cast<double>(smallerTableNumRows) *
        static_cast<double>(smallerTableNumRows) * ratioRows /
        (std::ceil(static_cast<double>(smallerTableNumRows) *
                   maxSampleSizeRatio) +
         std::ceil(static_cast<double>(smallerTableNumRows) * ratioRows *
                   maxSampleSizeRatio)))};
    const size_t resultNumColumns{getConfigVariables().smallerTableNumColumns_ +
                                  getConfigVariables().biggerTableNumColumns_ -
                                  1};
    if (const auto& maxMemory{getConfigVariables().maxMemory()};
        maxMemory.has_value() &&
        maxMemory.value() < approximateMemoryNeededByIdTable(
                                resultWantedNumRows, resultNumColumns)) {
      resultWantedNumRows =
          approximateNumIdTableRows(maxMemory.value(), resultNumColumns);
    }

    // Making a benchmark table for all combination of IdTables being sorted.
    for (const bool smallerTableSorted : {false, true}) {
      for (const bool biggerTableSorted : {false, true}) {
        for (const float smallerTableSampleSizeRatio : ratios) {
          const std::string tableName = absl::StrCat(
              "Tables, where the sample size ratio of the smaller table is ",
              smallerTableSampleSizeRatio,
              " and the sample size ratio for the bigger table changes.");
          ResultTable& table = makeGrowingBenchmarkTable(
              &results, tableName, "Bigger table sample size ratio",
              stopFunction, getConfigVariables().overlapChance_,
              resultWantedNumRows, getConfigVariables().randomSeed(),
              smallerTableSorted, biggerTableSorted, ratioRows,
              smallerTableNumRows, getConfigVariables().smallerTableNumColumns_,
              getConfigVariables().biggerTableNumColumns_,
              smallerTableSampleSizeRatio, growthFunction);

          // Add the metadata, that changes with every call and can't be
          // generalized.
          BenchmarkMetadata& meta = table.metadata();
          meta.addKeyValuePair("smaller-table-sorted", smallerTableSorted);
          meta.addKeyValuePair("bigger-table-sorted", biggerTableSorted);
          meta.addKeyValuePair("smaller-table-join-column-sample-size-ratio",
                               smallerTableSampleSizeRatio);
        }
      }
    }

    // Add the general metadata.
    BenchmarkMetadata& meta{getGeneralMetadata()};
    meta.addKeyValuePair("value-changing-with-every-row",
                         "bigger-table-join-column-sample-size-ratio");
    meta.addKeyValuePair("ratio-rows", ratioRows);
    meta.addKeyValuePair("smaller-table-num-rows", smallerTableNumRows);
    meta.addKeyValuePair("wanted-result-table-size", resultWantedNumRows);
    meta.addKeyValuePair("benchmark-sample-size-ratios",
                         getConfigVariables().benchmarkSampleSizeRatios_);
    GeneralInterfaceImplementation::addDefaultMetadata(&meta);
    return results;
  }
};

// Benchmark tables, where the smaller table grows and the bigger table has a
// static number of rows.
class BmSmallerTableGrowsBiggerTableRemainsSameSize final
    : public GeneralInterfaceImplementation {
 public:
  std::string name() const override {
    return "Benchmarktables, where the smaller table grows and the size of the "
           "bigger table remains the same.";
  }

  BenchmarkResults runAllBenchmarks() override {
    BenchmarkResults results{};
    // Making a benchmark table for all combination of IdTables being sorted and
    // all possibles sizes for the bigger table.
    for (const bool smallerTableSorted : {false, true}) {
      for (const bool biggerTableSorted : {false, true}) {
        for (const size_t biggerTableNumRows : generateExponentInterval(
                 10UL, getConfigVariables().minBiggerTableRows_,
                 approximateNumIdTableRows(
                     getConfigVariables().maxMemoryBiggerTable(),
                     getConfigVariables().biggerTableNumColumns_))) {
          /*
          Calculate the wanted row ratios, transform them into the fitting
          number of smaller table rows, and then create a growth function out
          of that. The growth function works because, in this special case, the
          table generation stops when the smaller table is bigger than the
          bigger table.
          */
          const float biggestRowRatio{static_cast<float>(
              static_cast<double>(biggerTableNumRows) /
              static_cast<double>(getConfigVariables().minSmallerTableRows_))};
          std::vector<size_t> smallerTableRows;
          ql::ranges::transform(
              mergeSortedVectors<float>(
                  {generateNaturalNumberSequenceInterval(
                       1.f, ql::ranges::min(10.f, biggestRowRatio)),
                   generateExponentInterval(10.f, 10.f, biggestRowRatio)}),
              std::back_inserter(smallerTableRows),
              [&biggerTableNumRows](const float ratio) {
                return static_cast<size_t>(
                    static_cast<double>(biggerTableNumRows) / ratio);
              });
          ql::ranges::reverse(smallerTableRows);
          const size_t lastSmallerTableRow{smallerTableRows.back()};
          auto growthFunction = createDefaultGrowthLambda(
              10UL, lastSmallerTableRow + 1UL, std::move(smallerTableRows));

          // Build the table.
          const std::string tableName = absl::StrCat(
              "Table, where the smaller table grows and the bigger always has ",
              biggerTableNumRows, " rows.");
          ResultTable& table =
              makeSmallerTableGrowsAndBiggerTableSameSizeBenchmarkTable(
                  &results, tableName, smallerTableSorted, biggerTableSorted,
                  growthFunction, biggerTableNumRows);

          // Add the metadata, that changes with every call and can't be
          // generalized.
          BenchmarkMetadata& meta = table.metadata();
          meta.addKeyValuePair("smaller-table-sorted", smallerTableSorted);
          meta.addKeyValuePair("bigger-table-sorted", biggerTableSorted);
          meta.addKeyValuePair("bigger-table-num-rows", biggerTableNumRows);
        }
      }
    }

    // Add the general metadata.
    BenchmarkMetadata& meta{getGeneralMetadata()};
    meta.addKeyValuePair("value-changing-with-every-row",
                         std::vector{"smaller-table-num-rows", "ratio-rows"});
    meta.addKeyValuePair(
        "smaller-table-join-column-sample-size-ratio",
        getConfigVariables().smallerTableJoinColumnSampleSizeRatio_);
    meta.addKeyValuePair(
        "bigger-table-join-column-sample-size-ratio",
        getConfigVariables().biggerTableJoinColumnSampleSizeRatio_);
    meta.addKeyValuePair("overlap-chance", getConfigVariables().overlapChance_);
    GeneralInterfaceImplementation::addDefaultMetadata(&meta);
    return results;
  }

 private:
  /*
  @brief Create a benchmark table for join algorithm, with the given
  parameters for the IdTables and the remaining IdTable parameters, that were
  not given, taken directly from the configuration options, which will keep
  getting more rows, until the `max-time-single-measurement` getter value, the
  `maxMemoryBiggerTable()` getter value, or the `maxMemory()` getter value of
  the configuration options was reached/surpassed.The columns will be:
  - Number of rows in the smaller table.
  - Time needed for sorting `IdTable`s.
  - Time needed for merge/galloping join.
  - Time needed for sorting and merge/galloping added together.
  - Time needed for the hash join.
  - How many rows the result of joining the tables has.
  - How much faster the hash join is. For example: Two times faster.

  @param results The `BenchmarkResults`, in which you want to create a new
  benchmark table.
  @param tableDescriptor A identifier for the to be created benchmark table, so
  that it can be easier identified later.
  @param smallerTableSorted, biggerTableSorted Should the bigger/smaller table
  be sorted by his join column before being joined? More specifically, some
  join algorithm require one, or both, of the IdTables to be sorted. If this
  argument is false, the time needed for sorting the required table will
  added to the time of the join algorithm.
  @param smallerTableNumRows How many rows, should the smaller table have in the
  given benchmarking table row?
  @param biggerTableNumRows Number of rows in the bigger table.
  */
  ResultTable& makeSmallerTableGrowsAndBiggerTableSameSizeBenchmarkTable(
      BenchmarkResults* results, const std::string& tableDescriptor,
      const bool smallerTableSorted, const bool biggerTableSorted,
      const growthFunction<size_t> auto& smallerTableNumRows,
      const size_t biggerTableNumRows) const {
    // For creating a new random seed for every new row.
    RandomSeedGenerator seedGenerator{getConfigVariables().randomSeed()};

    ResultTable& table = initializeBenchmarkTable(
        results, tableDescriptor, "Amount of rows in the smaller table");

    /*
    Stop function, so that the smaller table doesn't becomes bigger than the
    bigger table.
    */
    auto smallerTableIsSmallerThanBiggerTable =
        [](const bool, const float ratioRows, const auto...) {
          return ratioRows < 1.f;
        };

    /*
    Adding measurements to the table, as long as possible.
    */
    while (addNewRowToBenchmarkTable(
        &table, smallerTableNumRows(table.numRows()),
        smallerTableIsSmallerThanBiggerTable,
        getConfigVariables().overlapChance_, std::nullopt,
        std::invoke(seedGenerator), smallerTableSorted, biggerTableSorted,
        static_cast<float>(
            static_cast<double>(biggerTableNumRows) /
            static_cast<double>(smallerTableNumRows(table.numRows()))),
        smallerTableNumRows(table.numRows()),
        getConfigVariables().smallerTableNumColumns_,
        getConfigVariables().biggerTableNumColumns_,
        getConfigVariables().smallerTableJoinColumnSampleSizeRatio_,
        getConfigVariables().biggerTableJoinColumnSampleSizeRatio_)) {
      // Nothing to actually do here.
    }
    addPostMeasurementInformationsToBenchmarkTable(&table);
    return table;
  }
};
// Registering the benchmarks
AD_REGISTER_BENCHMARK(BmSameSizeRowGrowth);
AD_REGISTER_BENCHMARK(BmOnlySmallerTableSizeChanges);
AD_REGISTER_BENCHMARK(BmOnlyBiggerTableSizeChanges);
AD_REGISTER_BENCHMARK(BmSampleSizeRatio);
AD_REGISTER_BENCHMARK(BmSmallerTableGrowsBiggerTableRemainsSameSize);
}  // namespace ad_benchmark
