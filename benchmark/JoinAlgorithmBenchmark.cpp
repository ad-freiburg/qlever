// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (January of 2023, schlegea@informatik.uni-freiburg.de)
// Author of the file this file is based on: Björn Buchhold
// (buchhold@informatik.uni-freiburg.de)
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
#include <limits>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <tuple>
#include <type_traits>
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
#include "engine/ResultTable.h"
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
static constexpr bool isValuePreservingCast(const Source& source) {
  return static_cast<Source>(static_cast<Target>(source)) == source;
}

/*
@brief Return biggest possible value for the given arithmetic type.
*/
template <ad_utility::Arithmetic Type>
consteval Type getMaxValue() {
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
@brief Create an overlap between the join columns of the IdTables, by randomly
choosing distinct elements from the join column of the smaller table and
overiding all their occurrences in the join column with randomly choosen
distinct elements from the join column of the bigger table.

@param smallerTable The table, where distinct join column elements will be
overwritten.
@param biggerTable The table, where distinct join column elements will be copied
from.
@param probabilityToCreateOverlap The height of the probability for any distinct
join column element of smallerTable to be overwritten by a random distinct join
column element of biggerTable.
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

  /*
  All the distinct elements in the join column of the bigger table.
  Needed, so that I can randomly (uniform distribution) choose a distinct
  element in O(1).
  */
  std::vector<std::reference_wrapper<const ValueId>>
      biggerTableJoinColumnDistinctElements;

  /*
  How many instances of a value are inside the join column of the bigger
  table.
  */
  ad_utility::HashMap<ValueId, size_t>
      numOccurrencesValueInBiggertTableJoinColumn{};
  std::ranges::for_each(
      biggerTableJoinColumnRef,
      [&numOccurrencesValueInBiggertTableJoinColumn,
       &biggerTableJoinColumnDistinctElements](const ValueId& val) {
        if (auto numOccurrencesIterator =
                numOccurrencesValueInBiggertTableJoinColumn.find(val);
            numOccurrencesIterator !=
            numOccurrencesValueInBiggertTableJoinColumn.end()) {
          (numOccurrencesIterator->second)++;
        } else {
          numOccurrencesValueInBiggertTableJoinColumn.emplace(val, 1UL);
          biggerTableJoinColumnDistinctElements.emplace_back(val);
        }
      });

  // Seeds for the random generators, so that things are less similiar.
  const std::array<ad_utility::RandomSeed, 2> seeds =
      createArrayOfRandomSeeds<2>(std::move(randomSeed));

  /*
  Creating the generator for choosing a random distinct join column element
  in the bigger table.
  */
  ad_utility::SlowRandomIntGenerator<size_t> randomBiggerTableElement(
      0, biggerTableJoinColumnDistinctElements.size() - 1, seeds.at(0));

  // Generator for checking, if an overlap should be created.
  ad_utility::RandomDoubleGenerator randomDouble(0, 100, seeds.at(1));

  /*
  The amount of new overlap matches, that are generated by this
  function.
  */
  size_t newOverlapMatches{0};

  // Create the overlap.
  ad_utility::HashMap<ValueId, std::reference_wrapper<const ValueId>>
      smallerTableElementToNewValue{};
  std::ranges::for_each(
      smallerTableJoinColumnRef,
      [&randomDouble, &probabilityToCreateOverlap,
       &smallerTableElementToNewValue,
       &numOccurrencesValueInBiggertTableJoinColumn, &randomBiggerTableElement,
       &newOverlapMatches, &biggerTableJoinColumnDistinctElements](auto& id) {
        /*
        If a value has no hash map value, with which it will be overwritten, we
        either assign it its own value, or an element from the bigger table.
        */
        if (auto newValueIterator = smallerTableElementToNewValue.find(id);
            newValueIterator != smallerTableElementToNewValue.end()) {
          const ValueId& newValue = newValueIterator->second;
          // Values, that are only found in the smaller table, are added to the
          // hash map with value 0.
          const size_t numOccurrences{
              numOccurrencesValueInBiggertTableJoinColumn[newValue]};

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
          smallerTableElementToNewValue.emplace(
              id, biggerTableJoinColumnDistinctElements.at(
                      randomBiggerTableElement()));
        } else {
          /*
          Assign the value to itself.
          This is needed, because so we can indirectly track the first
          'encounter' with an distinct element in the smaller table and save
          ressources.
          */
          smallerTableElementToNewValue.emplace(id, id);
        }
      });

  return newOverlapMatches;
}

/*
@brief Create overlaps between the join columns of the IdTables until it is no
longer possible to grow closer to the wanted number of overlap matches.
Note: Because of runtime complexity concerns, not all overlap possibilities can
be tried and the end result can be quite unoptimal.

@param smallerTable The table, where distinct join column elements will be
overwritten.
@param biggerTable The table, where distinct join column elements will be copied
from.
@param wantedNumNewOverlapMatches How many new overlap matches should be
created. Note, that it is not always possible to reach the wanted number
exactly.
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

  /*
  All the distinct elements in the join columns. Needed, so that we can randomly
  choose them with an uniform distribution.
  */
  using DistinctElementsVector =
      std::vector<std::reference_wrapper<const ValueId>>;
  DistinctElementsVector smallerTableJoinColumnDistinctElements;
  DistinctElementsVector biggerTableJoinColumnDistinctElements;

  // How many instances of a value are inside the join column of a table.
  using NumOccurencesMap = ad_utility::HashMap<ValueId, size_t>;
  NumOccurencesMap numOccurrencesElementInSmallerTableJoinColumn{};
  NumOccurencesMap numOccurrencesElementInBiggerTableJoinColumn{};

  // Collect the number of occurences and the distinct elements.
  for (size_t idx = 0; idx < smallerTableJoinColumnRef.size() ||
                       idx < biggerTableJoinColumnRef.size();
       idx++) {
    const auto loopBody =
        [&idx](const std::span<const ValueId>& joinColumnReference,
               DistinctElementsVector& distinctElementsVec,
               NumOccurencesMap& numOccurrencesMap) {
          // Only do anything, if the index is not to far yet.
          if (idx < joinColumnReference.size()) {
            const auto& id{joinColumnReference[idx]};

            /*
            Use the hash map of the occurrences, to keep track of, if we already
            'encountered' a value.
            */
            if (auto numOccurrencesIterator = numOccurrencesMap.find(id);
                numOccurrencesIterator != numOccurrencesMap.end()) {
              (numOccurrencesIterator->second)++;
            } else {
              numOccurrencesMap.emplace(id, 1UL);
              distinctElementsVec.emplace_back(id);
            }
          }
        };
    loopBody(smallerTableJoinColumnRef, smallerTableJoinColumnDistinctElements,
             numOccurrencesElementInSmallerTableJoinColumn);
    loopBody(biggerTableJoinColumnRef, biggerTableJoinColumnDistinctElements,
             numOccurrencesElementInBiggerTableJoinColumn);
  }

  // Seeds for the random generators, so that things are less similiar.
  const std::array<ad_utility::RandomSeed, 2> seeds =
      createArrayOfRandomSeeds<2>(std::move(randomSeed));

  /*
  Creating the generator for choosing a random distinct join column element
  in the bigger table.
  */
  ad_utility::SlowRandomIntGenerator<size_t> randomBiggerTableElement(
      0, biggerTableJoinColumnDistinctElements.size() - 1, seeds.at(0));

  /*
  Assign distinct join column elements of the smaller table to be overwritten by
  the bigger table, until we created enough new overlap matches.
  */
  randomShuffle(smallerTableJoinColumnDistinctElements.begin(),
                smallerTableJoinColumnDistinctElements.end(), seeds.at(1));
  size_t newOverlapMatches{0};
  ad_utility::HashMap<ValueId, std::reference_wrapper<const ValueId>>
      smallerTableElementToNewElement{};
  /*
  In case, our smallest overshot is still closer to `wantedNumNewOverlapMatches`
  than our biggest undershot.
  First value is the smaller table value, second value the bigger table value
  and the `size_t` is the number of new matches created, when replacing the
  smaller table value with the bigger table value in the smaller table join
  column.
  */
  std::optional<std::tuple<std::reference_wrapper<const ValueId>,
                           std::reference_wrapper<const ValueId>, size_t>>
      smallestOvershot;
  ;
  std::ranges::for_each(
      smallerTableJoinColumnDistinctElements,
      [&biggerTableJoinColumnDistinctElements, &randomBiggerTableElement,
       &numOccurrencesElementInSmallerTableJoinColumn,
       &numOccurrencesElementInBiggerTableJoinColumn,
       &wantedNumNewOverlapMatches, &newOverlapMatches,
       &smallerTableElementToNewElement,
       &smallestOvershot](const ValueId& smallerTableId) {
        const auto& biggerTableId{biggerTableJoinColumnDistinctElements.at(
            randomBiggerTableElement())};

        // Skip this possibilty, if we have an overflow.
        size_t newMatches{
            numOccurrencesElementInSmallerTableJoinColumn.at(smallerTableId)};
        if (const size_t numOccurencesBiggerTable{
                numOccurrencesElementInBiggerTableJoinColumn.at(biggerTableId)};
            newMatches > getMaxValue<size_t>() / numOccurencesBiggerTable) {
          return;
        } else {
          newMatches *= numOccurencesBiggerTable;
        }

        // Just add as long as possible and always save the smallest overshot.
        if (newMatches <= wantedNumNewOverlapMatches &&
            newOverlapMatches <= wantedNumNewOverlapMatches - newMatches) {
          smallerTableElementToNewElement.emplace(smallerTableId,
                                                  biggerTableId);
          newOverlapMatches += newMatches;
        } else if (!smallestOvershot.has_value() ||
                   std::get<2>(smallestOvershot.value()) > newMatches) {
          smallestOvershot.emplace(smallerTableId, biggerTableId, newMatches);
        }
      });

  /*
  Add the smallest overshot, if it leaves us closer to the wanted amount of
  matches and will not result in an overflow.
  */
  if (smallestOvershot.has_value() &&
      std::get<2>(smallestOvershot.value()) <=
          getMaxValue<size_t>() - newOverlapMatches &&
      (std::get<2>(smallestOvershot.value()) + newOverlapMatches) -
              wantedNumNewOverlapMatches <
          wantedNumNewOverlapMatches - newOverlapMatches) {
    const auto& [smallerTableElement, biggerTableElement,
                 newMatches]{smallestOvershot.value()};
    smallerTableElementToNewElement.emplace(smallerTableElement,
                                            biggerTableElement);
    newOverlapMatches += newMatches;
  }

  // Overwrite the designated values in the smaller table.
  std::ranges::for_each(
      smallerTableJoinColumnRef, [&smallerTableElementToNewElement](auto& id) {
        if (auto newValueIterator = smallerTableElementToNewElement.find(id);
            newValueIterator != smallerTableElementToNewElement.end()) {
          id = newValueIterator->second;
        }
      });

  return newOverlapMatches;
}

/*
The columns of the automaticly generated benchmark tables contain the following
informations:
- The parameter, that changes with every row.
- Time needed for sorting `IdTable`s.
- Time needed for merge/galloping join.
- Time needed for sorting and merge/galloping added togehter.
- Time needed for the hash join.
- How many rows the result of joining the tables has.
- How much faster the hash join is. For example: Two times faster.

The following enum exists, in order to make the information about the order of
columns explicit.
*/
enum struct GeneratedTableColumn : unsigned long {
  ChangingParamter = 0,
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

/*
@brief Calculates the smalles whole exponent, so that $base^n$ is equal, or
bigger, than the `startingPoint`.
*/
static size_t calculateNextWholeExponent(const size_t& base,
                                         const auto& startingPoint) {
  // This is a rather simple calculation: We calculate
  // $log_(base)(startingPoint)$ and round up.
  return static_cast<size_t>(
      std::ceil(std::log(startingPoint) / std::log(base)));
}

/*
 * @brief Returns a vector of exponents $base^x$, with $x$ being a natural
 * number and $base^x$ being all possible numbers of this type in a given
 * range.
 *
 * @param base The base of the exponents.
 * @param startingPoint What the smalles exponent must at minimum be.
 * @param stoppingPoint What the biggest exponent is allowed to be.
 *
 * @return `{base^i, base^(i+1), ...., base^(i+n)}` with
 * `base^(i-1) < startingPoint`, `startingPoint <= base^i`,
 * `base^(i+n) <= stoppingPoint`and
 * `stoppingPoint < base^(i+n+1)`.
 */
static std::vector<size_t> createExponentVectorUntilSize(
    const size_t& base, const size_t& startingPoint,
    const size_t& stoppingPoint) {
  // Quick check, if the given arguments make sense.
  AD_CONTRACT_CHECK(startingPoint <= stoppingPoint);

  std::vector<size_t> exponentVector{};

  /*
  We can calculate our first exponent by using the logarithm.
  A short explanation: We calculate $log_(base)(startingPoint)$ and
  round up. This should give us the $x$ of the first $base^x$ bigger
  than `startingPoint`.
  */
  auto currentExponent = static_cast<size_t>(
      std::pow(base, calculateNextWholeExponent(base, startingPoint)));

  // The rest of the exponents.
  while (currentExponent <= stoppingPoint) {
    exponentVector.push_back(currentExponent);
    currentExponent *= base;
  }

  return exponentVector;
}

/*
@brief Approximates the amount of memory (in byte), that a `IdTable` needs.

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

// A struct for holding the variables, that the configuration options will write
// to.
struct ConfigVariables {
  /*
  The benchmark classes always make tables, where one attribute of the
  `IdTables` gets bigger with every row, while the other attributes stay the
  same.
  For the attributes, that don't stay the same, they create a list of
  exponents, with basis $2$, from $1$ up to an upper boundary for the
  exponents. The variables, that define such boundaries, always begin with
  `max`.

  For an explanation, what a member variables does, see the created
  `ConfigOption`s in `GeneralInterfaceImplementation`.
  */
  size_t smallerTableNumRows_;
  size_t minBiggerTableRows_;
  size_t maxBiggerTableRows_;
  size_t smallerTableNumColumns_;
  size_t biggerTableNumColumns_;
  float overlapChance_;
  float smallerTableJoinColumnSampleSizeRatio_;
  float biggerTableJoinColumnSampleSizeRatio_;
  size_t minRatioRows_;
  size_t maxRatioRows_;
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
  `maxMemory()`, if the `maxMemory` configuration option, interpreted as a
  `MemorySize`, wasn't set to `0`. (Which stand for infinite memory.)
  */
  ad_utility::MemorySize maxMemoryBiggerTable() const {
    return maxMemory().value_or(approximateMemoryNeededByIdTable(
        maxBiggerTableRows_, biggerTableNumColumns_));
  }

  /*
  The maximum amount of memory, any table is allowed to take up.
  Returns the value of the `maxMemory` configuration option interpreted as a
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

- Providing the member variables, that the benchmark classes here set
using the `ConfigManager`.

- A default `getMetadata`, that adds the date and time, where the benchmark
measurements were taken.

- A general function for generating a specific `ResultTable`, that measures the
join algorithms.
*/
class GeneralInterfaceImplementation : public BenchmarkInterface {
  // The variables, that our configuration option will write to.
  ConfigVariables configVariables_;

 public:
  // The variables, that the configuration option of this class will write to.
  const ConfigVariables& getConfigVariables() const { return configVariables_; }

  // The default constructor, which sets up the `ConfigManager`.
  GeneralInterfaceImplementation() {
    ad_utility::ConfigManager& config = getConfigManager();

    decltype(auto) smallerTableNumRows =
        config.addOption("smallerTableNumRows",
                         "Amount of rows for the smaller 'IdTable' in "
                         "the benchmarking class 'Benchmarktables, where the "
                         "smaller table stays at the same amount  of rows and "
                         "the bigger tables keeps getting bigger.'.",
                         &configVariables_.smallerTableNumRows_, 1000UL);

    decltype(auto) minBiggerTableRows = config.addOption(
        "minBiggerTableRows",
        "The minimum amount of rows for the bigger 'IdTable' in benchmarking "
        "tables.",
        &configVariables_.minBiggerTableRows_, 100000UL);
    decltype(auto) maxBiggerTableRows = config.addOption(
        "maxBiggerTableRows",
        "The maximum amount of rows for the bigger 'IdTable' in benchmarking "
        "tables.",
        &configVariables_.maxBiggerTableRows_, 10000000UL);

    decltype(auto) smallerTableNumColumns =
        config.addOption("smallerTableNumColumns",
                         "The amount of columns in the smaller IdTable.",
                         &configVariables_.smallerTableNumColumns_, 20UL);
    decltype(auto) biggerTableNumColumns = config.addOption(
        "biggerTableNumColumns", "The amount of columns in the bigger IdTable.",
        &configVariables_.biggerTableNumColumns_, 20UL);

    decltype(auto) overlapChance = config.addOption(
        "overlapChance",
        "Chance for all occurrences of a distinc element in the join column of "
        "the smaller 'IdTable' to be the same value as a distinc element in "
        "the join column of the bigger 'IdTable'. Must be in the range "
        "$(0,100]$.",
        &configVariables_.overlapChance_, 42.f);

    decltype(auto) smallerTableSampleSizeRatio = config.addOption(
        "smallerTableJoinColumnSampleSizeRatio",
        "Join column entries of the smaller tables are picked from a sample "
        "space with size 'Amount of smaller table rows' via discrete uniform "
        "distribution. This option adjusts the size to 'Amount of rows * "
        "ratio' (rounded up), which affects the possibility of duplicates.",
        &configVariables_.smallerTableJoinColumnSampleSizeRatio_, 1.f);
    decltype(auto) biggerTableSampleSizeRatio = config.addOption(
        "biggerTableJoinColumnSampleSizeRatio",
        "Join column entries of the bigger tables are picked from a sample "
        "space with size 'Amount of bigger table rows' via discrete uniform "
        "distribution. This option adjusts the size to 'Amount of rows * "
        "ratio' (rounded up), which affects the possibility of duplicates.",
        &configVariables_.biggerTableJoinColumnSampleSizeRatio_, 1.f);

    decltype(auto) randomSeed = config.addOption(
        "randomSeed",
        "The seed used for random generators. Note: The default value is a "
        "non-deterministic random value, which changes with every execution.",
        &configVariables_.randomSeed_,
        static_cast<size_t>(std::random_device{}()));

    decltype(auto) minRatioRows = config.addOption(
        "minRatioRows",
        "The minimum row ratio between the smaller and the "
        "bigger 'IdTable' for a benchmark table in the benchmark class "
        "'Benchmarktables, where the smaller table grows and the ratio between "
        "tables stays the same.'",
        &configVariables_.minRatioRows_, 10UL);
    decltype(auto) maxRatioRows = config.addOption(
        "maxRatioRows",
        "The maximum row ratio between the smaller and the "
        "bigger 'IdTable' for a benchmark table in the benchmark class "
        "'Benchmarktables, where the smaller table grows and the ratio between "
        "tables stays the same.'",
        &configVariables_.maxRatioRows_, 1000UL);

    decltype(auto) maxMemoryInStringFormat = config.addOption(
        "maxMemory",
        "Max amount of memory that any 'IdTable' is allowed to take up. '0' "
        "for unlimited memory. When set to anything else than '0', "
        "configuration option 'maxBiggerTableRows' is ignored. Example: 4kB, "
        "8MB, 24B, etc. ...",
        &configVariables_.configVariableMaxMemory_, "0B"s);

    decltype(auto) maxTimeSingleMeasurement = config.addOption(
        "maxTimeSingleMeasurement",
        "The maximal amount of time, in seconds, any function measurement is "
        "allowed to take. '0' for unlimited time. Note: This can only be "
        "checked, after a measurement was taken.",
        &configVariables_.maxTimeSingleMeasurement_, 0.f);

    decltype(auto) benchmarkSampleSizeRatios = config.addOption(
        "benchmarkSampleSizeRatios",
        "The sample size ratios for the benchmark class 'Benchmarktables, "
        "where only the sample size ratio changes.', where the sample size "
        "ratio for the smaller and bigger table are set to every element "
        "combination in the cartesian product of this set with itself. "
        "Example: For '{1.0, 2.0}' we would set both sample size ratios to "
        "'1.0', to '2.0', the smaller table sample size ratio to '1.0' and the "
        "bigger table sample size ratio to '2.0', and the smaller table sample "
        "size ratio to '2.0' and the bigger table sample size ratio to '1.0'.",
        &configVariables_.benchmarkSampleSizeRatios_,
        std::vector{0.1f, 1.f, 10.f});

    // Helper function for generating lambdas for validators.

    /*
    @brief The generated lambda returns true, iff if it is called with a value,
    that is bigger than the given minimum value

    @param canBeEqual If true, the generated lamba also returns true, if the
    values are equal.
    */
    auto generateBiggerEqualLambda = []<typename T>(const T& minimumValue,
                                                    bool canBeEqual) {
      return [minimumValue, canBeEqual](const T& valueToCheck) {
        return valueToCheck > minimumValue ||
               (canBeEqual && valueToCheck == minimumValue);
      };
    };

    // Object with a `operator()` for the `<=` operator.
    auto lessEqualLambda = std::less_equal<size_t>{};

    // Adding the validators.

    /*
    Is `maxMemory` big enough, to even allow for one row of the smaller
    table, bigger table, or the table resulting from joining the smaller and
    bigger table?`
    If not, return an `ErrorMessage`.
    */
    auto checkIfMaxMemoryBigEnoughForOneRow =
        [&maxMemoryInStringFormat](const ad_utility::MemorySize maxMemory,
                                   const std::string_view tableName,
                                   const size_t numColumns)
        -> std::optional<ad_utility::ErrorMessage> {
      const ad_utility::MemorySize memoryNeededForOneRow =
          approximateMemoryNeededByIdTable(1, numColumns);
      // Remember: `0` is for unlimited memory.
      if (maxMemory == 0_B || memoryNeededForOneRow <= maxMemory) {
        return std::nullopt;
      } else {
        return std::make_optional<ad_utility::ErrorMessage>(absl::StrCat(
            "'", maxMemoryInStringFormat.getConfigOption().getIdentifier(),
            "' (", maxMemory.asString(),
            ") must be big enough, for at least one row "
            "in the ",
            tableName, ", which requires at least ",
            memoryNeededForOneRow.asString(), "."));
      }
    };
    config.addValidator(
        [checkIfMaxMemoryBigEnoughForOneRow](std::string_view maxMemory,
                                             size_t smallerTableNumColumns) {
          return checkIfMaxMemoryBigEnoughForOneRow(
              ad_utility::MemorySize::parse(maxMemory), "smaller table",
              smallerTableNumColumns);
        },
        absl::StrCat(
            "'", maxMemoryInStringFormat.getConfigOption().getIdentifier(),
            "' must be big enough, for at least one row in the smaller table."),
        maxMemoryInStringFormat, smallerTableNumColumns);
    config.addValidator(
        [checkIfMaxMemoryBigEnoughForOneRow](std::string_view maxMemory,
                                             size_t biggerTableNumColumns) {
          return checkIfMaxMemoryBigEnoughForOneRow(
              ad_utility::MemorySize::parse(maxMemory), "bigger table",
              biggerTableNumColumns);
        },
        absl::StrCat(
            "'", maxMemoryInStringFormat.getConfigOption().getIdentifier(),
            "' must be big enough, for at least one row in the bigger table."),
        maxMemoryInStringFormat, biggerTableNumColumns);
    config.addValidator(
        [checkIfMaxMemoryBigEnoughForOneRow](std::string_view maxMemory,
                                             size_t smallerTableNumColumns,
                                             size_t biggerTableNumColumns) {
          return checkIfMaxMemoryBigEnoughForOneRow(
              ad_utility::MemorySize::parse(maxMemory),
              "result of joining the smaller and bigger table",
              smallerTableNumColumns + biggerTableNumColumns - 1);
        },
        absl::StrCat("'",
                     maxMemoryInStringFormat.getConfigOption().getIdentifier(),
                     "' must be big enough, for at least one row in the result "
                     "of joining the smaller and bigger table."),
        maxMemoryInStringFormat, smallerTableNumColumns, biggerTableNumColumns);

    // Is `smallerTableNumRows` a valid value?
    config.addValidator(
        generateBiggerEqualLambda(1UL, true),
        absl::StrCat("'", smallerTableNumRows.getConfigOption().getIdentifier(),
                     "' must be at least 1."),
        absl::StrCat("'", smallerTableNumRows.getConfigOption().getIdentifier(),
                     "' must be at least 1."),
        smallerTableNumRows);

    // Is `smallerTableNumRows` smaller than `minBiggerTableRows`?
    config.addValidator(
        lessEqualLambda,
        absl::StrCat("'", smallerTableNumRows.getConfigOption().getIdentifier(),
                     "' must be smaller than, or equal to, '",
                     minBiggerTableRows.getConfigOption().getIdentifier(),
                     "'."),
        absl::StrCat("'", smallerTableNumRows.getConfigOption().getIdentifier(),
                     "' must be smaller than, or equal to, '",
                     minBiggerTableRows.getConfigOption().getIdentifier(),
                     "'."),
        smallerTableNumRows, minBiggerTableRows);

    // Is `minBiggerTableRows` big enough, to deliver interesting measurements?
    config.addValidator(
        generateBiggerEqualLambda(
            minBiggerTableRows.getConfigOption().getDefaultValue<size_t>(),
            true),
        absl::StrCat(
            "'", minBiggerTableRows.getConfigOption().getIdentifier(),
            "' is to small. Interessting measurement values "
            "start at ",
            minBiggerTableRows.getConfigOption().getDefaultValueAsString(),
            " rows, or more."),
        absl::StrCat(
            "'", minBiggerTableRows.getConfigOption().getIdentifier(),
            "' is to small. Interessting measurement values "
            "start at ",
            minBiggerTableRows.getConfigOption().getDefaultValueAsString(),
            " rows, or more."),
        minBiggerTableRows);

    // Is `minBiggerTableRows` smaller, or equal, to `maxBiggerTableRows`?
    config.addValidator(
        lessEqualLambda,
        absl::StrCat("'", minBiggerTableRows.getConfigOption().getIdentifier(),
                     "' must be smaller than, or equal to, "
                     "'",
                     maxBiggerTableRows.getConfigOption().getIdentifier(),
                     "'."),
        absl::StrCat("'", minBiggerTableRows.getConfigOption().getIdentifier(),
                     "' must be smaller than, or equal to, "
                     "'",
                     maxBiggerTableRows.getConfigOption().getIdentifier(),
                     "'."),
        minBiggerTableRows, maxBiggerTableRows);

    // Do we have at least 1 column?
    config.addValidator(
        generateBiggerEqualLambda(1UL, true),
        absl::StrCat("'",
                     smallerTableNumColumns.getConfigOption().getIdentifier(),
                     "' must be at least 1."),
        absl::StrCat("'",
                     smallerTableNumColumns.getConfigOption().getIdentifier(),
                     "' must be at least 1."),
        smallerTableNumColumns);
    config.addValidator(
        generateBiggerEqualLambda(1UL, true),
        absl::StrCat("'",
                     biggerTableNumColumns.getConfigOption().getIdentifier(),
                     "' must be at least 1."),
        absl::StrCat("'",
                     biggerTableNumColumns.getConfigOption().getIdentifier(),
                     "' must be at least 1."),
        biggerTableNumColumns);

    // Is `overlapChance_` bigger than 0?
    config.addValidator(
        generateBiggerEqualLambda(0.f, false),
        absl::StrCat("'", overlapChance.getConfigOption().getIdentifier(),
                     "' must be bigger than 0."),
        absl::StrCat("'", overlapChance.getConfigOption().getIdentifier(),
                     "' must be bigger than 0."),
        overlapChance);

    // Are the sample size ratios bigger than 0?
    config.addValidator(
        generateBiggerEqualLambda(0.f, false),
        absl::StrCat(
            "'", smallerTableSampleSizeRatio.getConfigOption().getIdentifier(),
            "' must be bigger than 0."),
        absl::StrCat(
            "'", smallerTableSampleSizeRatio.getConfigOption().getIdentifier(),
            "' must be bigger than 0."),
        smallerTableSampleSizeRatio);
    config.addValidator(
        generateBiggerEqualLambda(0.f, false),
        absl::StrCat(
            "'", biggerTableSampleSizeRatio.getConfigOption().getIdentifier(),
            "' must be bigger than 0."),
        absl::StrCat(
            "'", biggerTableSampleSizeRatio.getConfigOption().getIdentifier(),
            "' must be bigger than 0."),
        biggerTableSampleSizeRatio);
    config.addValidator(
        [](const std::vector<float>& vec) {
          return std::ranges::all_of(
              vec, [](const float ratio) { return ratio >= 0.f; });
        },
        absl::StrCat(
            "All entries in '",
            benchmarkSampleSizeRatios.getConfigOption().getIdentifier(),
            "' must be bigger than, or equal to, 0."),
        absl::StrCat(
            "All entries in '",
            benchmarkSampleSizeRatios.getConfigOption().getIdentifier(),
            "' must be bigger than, or equal to, 0."),
        benchmarkSampleSizeRatios);

    /*
    We later signal, that the row generation for the sample size ration tables
    should be stopped, by returning a float, that is the biggest entry in the
    vector plus 1.
    So, of course, that has to be possible.
    */
    config.addValidator(
        [](const std::vector<float>& vec) {
          return std::ranges::max(vec) <=
                 getMaxValue<std::decay_t<decltype(vec)>::value_type>() - 1.f;
        },
        absl::StrCat(
            "All entries in '",
            benchmarkSampleSizeRatios.getConfigOption().getIdentifier(),
            "' must be smaller than, or equal to, ", getMaxValue<float>() - 1.f,
            "."),
        absl::StrCat(
            "All entries in '",
            benchmarkSampleSizeRatios.getConfigOption().getIdentifier(),
            "' must be smaller than, or equal to, ", getMaxValue<float>() - 1.f,
            "."),
        benchmarkSampleSizeRatios);

    /*
    Is `randomSeed_` smaller/equal than the max value for seeds?
    Note: The static cast is needed, because a random generator seed is always
    `unsigned int`.
    */
    config.addValidator(
        [maxSeed = static_cast<size_t>(ad_utility::RandomSeed::max().get())](
            const size_t seed) { return seed <= maxSeed; },
        absl::StrCat("'", randomSeed.getConfigOption().getIdentifier(),
                     "' must be smaller than, or equal to, ",
                     ad_utility::RandomSeed::max().get(), "."),
        absl::StrCat("'", randomSeed.getConfigOption().getIdentifier(),
                     "' must be smaller than, or equal to, ",
                     ad_utility::RandomSeed::max().get(), "."),
        randomSeed);

    // Is `maxTimeSingleMeasurement` a positive number?
    config.addValidator(
        generateBiggerEqualLambda(0.f, true),
        absl::StrCat("'",
                     maxTimeSingleMeasurement.getConfigOption().getIdentifier(),
                     "' must be bigger than, or equal to, 0."),
        absl::StrCat("'",
                     maxTimeSingleMeasurement.getConfigOption().getIdentifier(),
                     "' must be bigger than, or equal to, 0."),
        maxTimeSingleMeasurement);

    // Is the ratio of rows at least 10?
    config.addValidator(
        generateBiggerEqualLambda(10UL, true),
        absl::StrCat("'", minRatioRows.getConfigOption().getIdentifier(),
                     "' must be at least 10."),
        absl::StrCat("'", minRatioRows.getConfigOption().getIdentifier(),
                     "' must be at least 10."),
        minRatioRows);

    // Is `minRatioRows` smaller than `maxRatioRows`?
    config.addValidator(
        lessEqualLambda,
        absl::StrCat("'", minRatioRows.getConfigOption().getIdentifier(),
                     "' must be smaller than, or equal to, "
                     "'",
                     maxRatioRows.getConfigOption().getIdentifier(), "'."),
        absl::StrCat("'", minRatioRows.getConfigOption().getIdentifier(),
                     "' must be smaller than, or equal to, "
                     "'",
                     maxRatioRows.getConfigOption().getIdentifier(), "'."),
        minRatioRows, maxRatioRows);

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
    std::ranges::for_each(
        std::vector{smallerTableNumRows, minBiggerTableRows, maxBiggerTableRows,
                    minRatioRows, maxRatioRows},
        addCastableValidator);
  }

  /*
  @brief Add metadata information about the class, that is always interesting
  and not dependent on the created `ResultTable`.
  */
  void addDefaultMetadata(BenchmarkMetadata* meta) const {
    // Information, that is interesting for all the benchmarking classes.
    meta->addKeyValuePair("randomSeed",
                          getConfigVariables().randomSeed().get());
    meta->addKeyValuePair("smallerTableNumColumns",
                          getConfigVariables().smallerTableNumColumns_);
    meta->addKeyValuePair("biggerTableNumColumns",
                          getConfigVariables().biggerTableNumColumns_);

    /*
    Add metadata information over:
    - "maxTimeSingleMeasurement"
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
    addInfiniteWhen0("maxTimeSingleMeasurement",
                     configVariables_.maxTimeSingleMeasurement_);
    addInfiniteWhen0("maxMemory",
                     getConfigVariables().maxMemory().value_or(0_B).getBytes());
  }

  // A default `stopFunction` for `makeGrowingBenchmarkTable`, that takes
  // everything and always returns false.
  static constexpr auto alwaysFalse = [](const auto&...) { return false; };

  /*
  @brief Create a benchmark table for join algorithm, with the given
  parameters for the IdTables, which will keep getting more rows, until the
  `maxTimeSingleMeasurement` getter value, the `maxMemoryBiggerTable()` getter
  value, or the `maxMemory()` getter value of the configuration options was
  reached/surpassed. It will additionally stop, iff `stopFunction` returns true.
  The rows will be the return values of the parameter, you gave a function
  for, and the columns will be:
  - Return values of the parameter, you gave a function for.
  - Time needed for sorting `IdTable`s.
  - Time needed for merge/galloping join.
  - Time needed for sorting and merge/galloping added togehter.
  - Time needed for the hash join.
  - How many rows the result of joining the tables has.
  - How much faster the hash join is. For example: Two times faster.

  @tparam T1, T6, T7 Must be a float, or a function, that takes the row number
  of the next to be generated row as `const size_t&`, and returns a float. Can
  only be a function, if all other template `T` parameter are vectors.
  @tparam T2, T3, T4, T5 Must be a size_t, or a function, that takes the row
  number of the next to be generated row as `const size_t&`, and returns a
  size_t. Can only be a function, if all other template `T` parameter are
  vectors.

  @param results The BenchmarkResults, in which you want to create a new
  benchmark table.
  @param tableDescriptor A identifier to the to be created benchmark table, so
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
  be sorted by his join column before being joined? More specificly, some
  join algorithm require one, or both, of the IdTables to be sorted. If this
  argument is false, the time needed for sorting the required table will
  added to the time of the join algorithm.
  @param ratioRows How many more rows than the smaller table should the
  bigger table have? In more mathematical words: Number of rows of the
  bigger table divided by the number of rows of the smaller table is equal
  to ratioRows.
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
  template <ad_utility::InvocableWithExactReturnType<
                bool, float, size_t, size_t, size_t, size_t, float, float>
                StopFunction,
            isTypeOrGrowthFunction<float> T1, isTypeOrGrowthFunction<size_t> T2,
            isTypeOrGrowthFunction<size_t> T3,
            isTypeOrGrowthFunction<size_t> T4,
            isTypeOrGrowthFunction<size_t> T5,
            isTypeOrGrowthFunction<float> T6 = float,
            isTypeOrGrowthFunction<float> T7 = float>
  requires exactlyOneGrowthFunction<T1, T2, T3, T4, T5, T6, T7>
  ResultTable& makeGrowingBenchmarkTable(
      BenchmarkResults* results, const std::string_view tableDescriptor,
      std::string parameterName, StopFunction stopFunction, const T1& overlap,
      std::optional<size_t> resultTableNumRows,
      ad_utility::RandomSeed randomSeed, const bool smallerTableSorted,
      const bool biggerTableSorted, const T2& ratioRows,
      const T3& smallerTableNumRows, const T4& smallerTableNumColumns,
      const T5& biggerTableNumColumns,
      const T6& smallerTableJoinColumnSampleSizeRatio,
      const T7& biggerTableJoinColumnSampleSizeRatio) const {
    // Is something a growth function?
    constexpr auto isGrowthFunction = []<typename T>() {
      /*
      We have to cheat a bit, because being a function is not something, that
      can easily be checked for to my knowledge. Instead, we simply check, if
      it's one of the limited variation of growth function, that we allow.
      */
      if constexpr (growthFunction<T, size_t> || growthFunction<T, float>) {
        return true;
      } else {
        return false;
      }
    };

    // Returns the first argument, that is a growth function.
    auto returnFirstGrowthFunction =
        [&isGrowthFunction]<typename... Ts>(Ts&... args) -> auto& {
      // Put them into a tuple, so that we can easly look them up.
      auto tup = std::tuple<Ts&...>{AD_FWD(args)...};

      // Get the index of the first growth function.
      constexpr static size_t idx =
          ad_utility::getIndexOfFirstTypeToPassCheck<isGrowthFunction, Ts...>();

      // Do we have a valid index?
      static_assert(idx < sizeof...(Ts),
                    "There was no growth function in this parameter pack.");

      return std::get<idx>(tup);
    };

    /*
    @brief Calls the growth function with the number of the next row to be
    created, if it's a function, and returns the result. Otherwise just
    returns the given `possibleGrowthFunction`.
    */
    auto returnOrCall = [&isGrowthFunction]<typename T>(
                            const T& possibleGrowthFunction,
                            const size_t nextRowIdx) {
      if constexpr (isGrowthFunction.template operator()<T>()) {
        return possibleGrowthFunction(nextRowIdx);
      } else {
        return possibleGrowthFunction;
      }
    };

    // For creating a new random seed for every new row.
    RandomSeedGenerator seedGenerator{std::move(randomSeed)};

    /*
    Now on to creating the benchmark table. Because we don't know, how many
    row names we will have, we just create a table without row names.
    */
    ResultTable& table = results->addTable(
        std::string{tableDescriptor}, {},
        {std::move(parameterName), "Time for sorting", "Merge/Galloping join",
         "Sorting + merge/galloping join", "Hash join",
         "Number of rows in resulting IdTable", "Speedup of hash join"});

    /*
    Adding measurements to the table, as long as possible.
    */
    while (true) {
      // What's the row number of the next to be added row?
      const size_t rowIdx = table.numRows();

      // Collect the parameter for this row.
      auto overlapValue{returnOrCall(overlap, rowIdx)};
      auto ratioRowsValue{returnOrCall(ratioRows, rowIdx)};
      auto smallerTableNumRowsValue{returnOrCall(smallerTableNumRows, rowIdx)};
      auto smallerTableNumColumnsValue{
          returnOrCall(smallerTableNumColumns, rowIdx)};
      auto biggerTableNumColumnsValue{
          returnOrCall(biggerTableNumColumns, rowIdx)};
      auto smallerTableJoinColumnSampleSizeRatioValue{
          returnOrCall(smallerTableJoinColumnSampleSizeRatio, rowIdx)};
      auto biggerTableJoinColumnSampleSizeRatioValue{
          returnOrCall(biggerTableJoinColumnSampleSizeRatio, rowIdx)};

      // Add a new row without content.
      table.addRow();
      table.setEntry(
          rowIdx, toUnderlying(GeneratedTableColumn::ChangingParamter),
          returnFirstGrowthFunction(
              overlap, ratioRows, smallerTableNumRows, smallerTableNumColumns,
              biggerTableNumColumns, smallerTableJoinColumnSampleSizeRatio,
              biggerTableJoinColumnSampleSizeRatio)(rowIdx));

      /*
      Stop and delete the newest row, if the stop function is against its
      generation, or if the addition of measurements wasn't successfull.
      A partly filled row is of no use to anyone.
      */
      if (std::invoke(stopFunction, overlapValue, ratioRowsValue,
                      smallerTableNumRowsValue, smallerTableNumColumnsValue,
                      biggerTableNumColumnsValue,
                      smallerTableJoinColumnSampleSizeRatioValue,
                      biggerTableJoinColumnSampleSizeRatioValue) ||
          !addMeasurementsToRowOfBenchmarkTable(
              &table, rowIdx, overlapValue, resultTableNumRows,
              std::invoke(seedGenerator), smallerTableSorted, biggerTableSorted,
              ratioRowsValue, smallerTableNumRowsValue,
              smallerTableNumColumnsValue, biggerTableNumColumnsValue,
              smallerTableJoinColumnSampleSizeRatioValue,
              biggerTableJoinColumnSampleSizeRatioValue)) {
        table.deleteRow(rowIdx);
        break;
      }
    }

    // Adding together the time for sorting the `IdTables` and then joining
    // them using merge/galloping join.
    sumUpColumns(
        &table,
        ColumnNumWithType<float>{toUnderlying(
            GeneratedTableColumn::TimeForSortingAndMergeGallopingJoin)},
        ColumnNumWithType<float>{
            toUnderlying(GeneratedTableColumn::TimeForSorting)},
        ColumnNumWithType<float>{
            toUnderlying(GeneratedTableColumn::TimeForMergeGallopingJoin)});

    // Calculate, how much of a speedup the hash join algorithm has in
    // comparison to the merge/galloping join algrithm.
    calculateSpeedupOfColumn(
        &table, {toUnderlying(GeneratedTableColumn::JoinAlgorithmSpeedup)},
        {toUnderlying(GeneratedTableColumn::TimeForHashJoin)},
        {toUnderlying(
            GeneratedTableColumn::TimeForSortingAndMergeGallopingJoin)});

    // For more specific adjustments.
    return table;
  }

 private:
  /*
  @brief Adds the function time measurements to a row of the benchmark table
  in `makeGrowingBenchmarkTable`.
  For an explanation of the parameters, see `makeGrowingBenchmarkTable`.

  @return True, if all measurements were added. False, if the addition of
  measurements was stopped, because either a `IdTable` required more memory
  than was allowed via configuration options, or if a single measurement took
  longer than was allowed via configuration options. Note: In the case of
  `false`, some measurements may have been added, but never all.
  */
  bool addMeasurementsToRowOfBenchmarkTable(
      ResultTable* table, const size_t& rowIdx, const float overlap,
      std::optional<size_t> resultTableNumRows,
      ad_utility::RandomSeed randomSeed, const bool smallerTableSorted,
      const bool biggerTableSorted, const size_t& ratioRows,
      const size_t& smallerTableNumRows, const size_t& smallerTableNumColumns,
      const size_t& biggerTableNumColumns,
      const float smallerTableJoinColumnSampleSizeRatio,
      const float biggerTableJoinColumnSampleSizeRatio) const {
    // Checking, if smallerTableJoinColumnSampleSizeRatio and
    // biggerTableJoinColumnSampleSizeRatio are floats bigger than 0.
    // Otherwise , they don't make sense.
    AD_CONTRACT_CHECK(smallerTableJoinColumnSampleSizeRatio > 0);
    AD_CONTRACT_CHECK(biggerTableJoinColumnSampleSizeRatio > 0);

    // Make sure, that things can be casted (in later calculations) without
    // changing values.
    AD_CORRECTNESS_CHECK(isValuePreservingCast<double>(smallerTableNumRows));
    AD_CORRECTNESS_CHECK(isValuePreservingCast<double>(ratioRows));

    /*
    Check if the smaller and bigger `IdTable` are not to big. Size of the
    result table is only checked, if the configuration option for it was set.
    */
    if (const auto& maxSizeInputTable{
            getConfigVariables().maxMemoryBiggerTable()};
        approximateMemoryNeededByIdTable(
            smallerTableNumRows, smallerTableNumColumns) > maxSizeInputTable ||
        approximateMemoryNeededByIdTable(smallerTableNumRows * ratioRows,
                                         biggerTableNumColumns) >
            maxSizeInputTable) {
      return false;
    }

    // The lambdas for the join algorithms.
    auto hashJoinLambda = makeHashJoinLambda();
    auto joinLambda = makeJoinLambda();

    // Create new `IdTable`s.

    /*
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
                       ") with 'smallerTableJoinColumnSampleSizeRatio' (",
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
                       ") with 'smallerTableJoinColumnSampleSizeRatio' (",
                       smallerTableJoinColumnSampleSizeRatio, "), plus 1,"));
    }
    const size_t biggerTableJoinColumnLowerBound =
        smallerTableJoinColumnUpperBound + 1;

    /*
    Check for overflow, before calculating `biggerTableJoinColumnUpperBound`.
    Short math explanation: I check the Intermediate results, before using
    them, and also use the same trick as with
    `smallerTableJoinColumnUpperBound` to check, that `std::ceil(...)` is
    neither overflow, nor underflow.
    */
    if (static_cast<double>(smallerTableNumRows) >
        getMaxValue<double>() / static_cast<double>(ratioRows)) {
      throwOverflowError<double>(
          absl::StrCat(" the number of bigger table rows (",
                       smallerTableNumRows * ratioRows, ")"));
    } else if (static_cast<double>(smallerTableNumRows) *
                   static_cast<double>(ratioRows) >
               std::floor(getMaxValue<double>()) /
                   biggerTableJoinColumnSampleSizeRatio) {
      throwOverflowError<double>(
          absl::StrCat("multiplication of the number of bigger table rows (",
                       smallerTableNumRows * ratioRows,
                       ") with 'biggerTableJoinColumnSampleSizeRatio' (",
                       biggerTableJoinColumnSampleSizeRatio, ")"));
    } else if (biggerTableJoinColumnLowerBound - 1 >
               getMaxValue<size_t>() -
                   static_cast<size_t>(
                       std::ceil(static_cast<double>(smallerTableNumRows) *
                                 static_cast<double>(ratioRows) *
                                 biggerTableJoinColumnSampleSizeRatio))) {
      throw std::runtime_error(absl::StrCat(
          "size_t overflow error: The multiplication (rounded up) of the "
          "number of smaller table rows (",
          smallerTableNumRows,
          ") with 'smallerTableJoinColumnSampleSizeRatio' (",
          smallerTableJoinColumnSampleSizeRatio,
          "), minus 1, added to the multiplication (rounded up) of the "
          "number "
          "of bigger table rows (",
          smallerTableNumRows * ratioRows,
          ") with 'biggerTableJoinColumnSampleSizeRatio' (",
          biggerTableJoinColumnSampleSizeRatio,
          ") is bigger than the size_t type maximum ", getMaxValue<size_t>(),
          ". Try reducing the values for the configuration options."));
    }
    const size_t biggerTableJoinColumnUpperBound =
        biggerTableJoinColumnLowerBound +
        static_cast<size_t>(std::ceil(static_cast<double>(smallerTableNumRows) *
                                      static_cast<double>(ratioRows) *
                                      biggerTableJoinColumnSampleSizeRatio)) -
        1;

    // Seeds for the random generators, so that things are less similiar
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
            smallerTableNumRows * ratioRows, biggerTableNumColumns,
            JoinColumnAndBounds{0, biggerTableJoinColumnLowerBound,
                                biggerTableJoinColumnUpperBound, seeds.at(2)},
            seeds.at(3)),
        0};

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
      return false;
    }

    // Sort the `IdTables`, if they should be.
    if (smallerTableSorted) {
      sortIdTableByJoinColumnInPlace(smallerTable);
    }
    if (biggerTableSorted) {
      sortIdTableByJoinColumnInPlace(biggerTable);
    }

    // Adding the benchmark measurements to the current row.
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

    // Hash join first, because merge/galloping join sorts all tables, if
    // needed, before joining them.
    table->addMeasurement(
        rowIdx, toUnderlying(GeneratedTableColumn::TimeForHashJoin),
        [&smallerTable, &biggerTable, &hashJoinLambda]() {
          useJoinFunctionOnIdTables(smallerTable, biggerTable, hashJoinLambda);
        });
    if (isOverMaxTime(GeneratedTableColumn::TimeForHashJoin)) {
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
      return false;
    }

    // The merge/galloping join.
    table->addMeasurement(
        rowIdx, toUnderlying(GeneratedTableColumn::TimeForMergeGallopingJoin),
        [&smallerTable, &biggerTable, &joinLambda]() {
          useJoinFunctionOnIdTables(smallerTable, biggerTable, joinLambda);
        });
    if (isOverMaxTime(GeneratedTableColumn::TimeForMergeGallopingJoin)) {
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
@brief Returns a lambda function, which calculates and returns
$base^(x+rowIdx)$. With $rowIdx$ being the single `size_t` argument of the
function and $x$ being $log_base(startingPoint)$ rounded up.
*/
auto createDefaultGrowthLambda(const size_t& base,
                               const size_t& startingPoint) {
  return [base, startingExponent{calculateNextWholeExponent(
                    base, startingPoint)}](const size_t& rowIdx) {
    return static_cast<size_t>(std::pow(base, startingExponent + rowIdx));
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

    // Making a benchmark table for all combination of IdTables being sorted.
    for (const bool smallerTableSorted : {false, true}) {
      for (const bool biggerTableSorted : {false, true}) {
        const std::string& tableName =
            absl::StrCat("Smaller table stays at ",
                         getConfigVariables().smallerTableNumRows_,
                         " rows, ratio to rows of bigger table grows.");

        // Returns the ratio used for the measurements in a given row.
        auto growthFunction = createDefaultGrowthLambda(
            10, getConfigVariables().minBiggerTableRows_ /
                    getConfigVariables().smallerTableNumRows_);

        ResultTable& table = makeGrowingBenchmarkTable(
            &results, tableName, "Row ratio", alwaysFalse,
            getConfigVariables().overlapChance_, std::nullopt,
            getConfigVariables().randomSeed(), smallerTableSorted,
            biggerTableSorted, growthFunction,
            getConfigVariables().smallerTableNumRows_,
            getConfigVariables().smallerTableNumColumns_,
            getConfigVariables().biggerTableNumColumns_,
            getConfigVariables().smallerTableJoinColumnSampleSizeRatio_,
            getConfigVariables().biggerTableJoinColumnSampleSizeRatio_);

        // Add the metadata, that changes with every call and can't be
        // generalized.
        BenchmarkMetadata& meta = table.metadata();
        meta.addKeyValuePair("smallerTableSorted", smallerTableSorted);
        meta.addKeyValuePair("biggerTableSorted", biggerTableSorted);
      }
    }

    // Add the general metadata.
    BenchmarkMetadata& meta{getGeneralMetadata()};
    meta.addKeyValuePair("Value changing with every row", "ratioRows");
    meta.addKeyValuePair("smallerTableNumRows",
                         getConfigVariables().smallerTableNumRows_);
    meta.addKeyValuePair(
        "smallerTableJoinColumnSampleSizeRatio",
        getConfigVariables().smallerTableJoinColumnSampleSizeRatio_);
    meta.addKeyValuePair(
        "biggerTableJoinColumnSampleSizeRatio",
        getConfigVariables().biggerTableJoinColumnSampleSizeRatio_);
    meta.addKeyValuePair("overlapChance", getConfigVariables().overlapChance_);
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
        for (const size_t ratioRows : createExponentVectorUntilSize(
                 10, getConfigVariables().minRatioRows_,
                 getConfigVariables().maxRatioRows_)) {
          const std::string& tableName = absl::StrCat(
              "The amount of rows in the smaller table grows and the ratio, to "
              "the amount of rows in the bigger table, stays at ",
              ratioRows, ".");

          // Returns the amount of rows in the smaller `IdTable`, used for the
          // measurements in a given row.
          auto growthFunction = createDefaultGrowthLambda(
              10, getConfigVariables().minBiggerTableRows_ / ratioRows);

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
          meta.addKeyValuePair("smallerTableSorted", smallerTableSorted);
          meta.addKeyValuePair("biggerTableSorted", biggerTableSorted);
          meta.addKeyValuePair("ratioRows", ratioRows);
        }
      }
    }

    // Add the general metadata.
    BenchmarkMetadata& meta{getGeneralMetadata()};
    meta.addKeyValuePair("Value changing with every row",
                         "smallerTableNumRows");
    meta.addKeyValuePair(
        "smallerTableJoinColumnSampleSizeRatio",
        getConfigVariables().smallerTableJoinColumnSampleSizeRatio_);
    meta.addKeyValuePair(
        "biggerTableJoinColumnSampleSizeRatio",
        getConfigVariables().biggerTableJoinColumnSampleSizeRatio_);
    meta.addKeyValuePair("overlapChance", getConfigVariables().overlapChance_);
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

    // Making a benchmark table for all combination of IdTables being sorted.
    for (const bool smallerTableSorted : {false, true}) {
      for (const bool biggerTableSorted : {false, true}) {
        constexpr std::string_view tableName =
            "Both tables always have the same amount of rows and that amount "
            "grows.";

        // Returns the amount of rows in the smaller `IdTable`, used for the
        // measurements in a given row.
        auto growthFunction = createDefaultGrowthLambda(
            10, getConfigVariables().minBiggerTableRows_);

        ResultTable& table = makeGrowingBenchmarkTable(
            &results, tableName, "Amount of rows", alwaysFalse,
            getConfigVariables().overlapChance_, std::nullopt,
            getConfigVariables().randomSeed(), smallerTableSorted,
            biggerTableSorted, 1UL, growthFunction,
            getConfigVariables().smallerTableNumColumns_,
            getConfigVariables().biggerTableNumColumns_,
            getConfigVariables().smallerTableJoinColumnSampleSizeRatio_,
            getConfigVariables().biggerTableJoinColumnSampleSizeRatio_);

        // Add the metadata, that changes with every call and can't be
        // generalized.
        BenchmarkMetadata& meta = table.metadata();
        meta.addKeyValuePair("smallerTableSorted", smallerTableSorted);
        meta.addKeyValuePair("biggerTableSorted", biggerTableSorted);
      }
    }

    // Add the general metadata.
    BenchmarkMetadata& meta{getGeneralMetadata()};
    meta.addKeyValuePair("Value changing with every row",
                         "smallerTableNumRows");
    meta.addKeyValuePair("ratioRows", 1);
    meta.addKeyValuePair(
        "smallerTableJoinColumnSampleSizeRatio",
        getConfigVariables().smallerTableJoinColumnSampleSizeRatio_);
    meta.addKeyValuePair(
        "biggerTableJoinColumnSampleSizeRatio",
        getConfigVariables().biggerTableJoinColumnSampleSizeRatio_);
    meta.addKeyValuePair("overlapChance", getConfigVariables().overlapChance_);
    GeneralInterfaceImplementation::addDefaultMetadata(&meta);
    return results;
  }
};

// Create benchmark tables, where the tables are the same size and
// both just get more rows.
class BmSampleSizeRatio final : public GeneralInterfaceImplementation {
 public:
  std::string name() const override {
    return "Benchmarktables, where only the sample size ratio changes.";
  }

  BenchmarkResults runAllBenchmarks() override {
    BenchmarkResults results{};
    const auto& ratios{getConfigVariables().benchmarkSampleSizeRatios_};
    const float maxSampleSizeRatio{std::ranges::max(ratios)};

    /*
    @brief Calculate the number of rows (rounded down) in an `IdTable` with the
    given memory size and number of columns.
    */
    auto memorySizeToIdTableRows = [](const ad_utility::MemorySize& m,
                                      const size_t numColumns) {
      AD_CORRECTNESS_CHECK(numColumns > 0);
      AD_CORRECTNESS_CHECK(m.getBytes() > 0);
      return (m.getBytes() / sizeof(IdTable::value_type)) / numColumns;
    };

    /*
    We work with the biggest possible smaller and bigger table. That should make
    any difference in execution time easier to find.
    Note: Strings are for the generation of error messages.
    */
    const size_t ratioRows{getConfigVariables().minRatioRows_};
    constexpr std::string_view ratioRowsDescription{"'minRatioRows'"};
    size_t smallerTableNumRows{};
    std::string smallerTableNumRowsDescription{};
    std::string smallerTableNumRowsConfigurationOptions{};
    if (const auto& maxMemory{getConfigVariables().maxMemory()};
        maxMemory.has_value()) {
      smallerTableNumRows =
          memorySizeToIdTableRows(maxMemory.value(),
                                  getConfigVariables().biggerTableNumColumns_) /
          getConfigVariables().minRatioRows_;
      smallerTableNumRowsDescription =
          "division of the maximum number of rows, under the given 'maxMemory' "
          "and 'biggerTableNumColumns', with 'minRatioRows'";
      smallerTableNumRowsConfigurationOptions =
          "'maxMemory' and 'biggerTableNumColumns'";
    } else {
      smallerTableNumRows = getConfigVariables().maxBiggerTableRows_ /
                            getConfigVariables().minRatioRows_;
      smallerTableNumRowsDescription =
          "divison of 'maxBiggerTableRows' with 'minRatioRows'";
      smallerTableNumRowsConfigurationOptions = "'maxBiggerTableRows'";
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
    Calculate the expexcted number of rows in the result for the simplified
    creation model of input tables join columns and overlaps, with the biggest
    sample size ratio used for both input tables. The simplified creation model
    assumes, that:
    - The join column elements of the smaller and bigger table are not disjoint
    at creation time. In reality, both join columns are created disjoint and any
    overlaps are inserted later.
    - The join column entries of the smaller table have a uniform distribution,
    are made up out of the join column elements of both tables (smaller and
    bigger) and the generation of one row entry is independet from the
    generation of all other row entries.
    - The join column entries of the bigger table have a uniform distribution,
    are made up out of only the elements of the bigger tables and the generation
    of one row entry is independet from the generation of all other row entries.
    - The generation of join column entries in the smaller table is independet
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
                       "'benchmarkSampleSizeRatios'"));
    } else if (static_cast<double>(smallerTableNumRows * ratioRows) >
               getMaxValue<double>() / maxSampleSizeRatio) {
      throwOverflowError<double>(
          absl::StrCat(smallerTableNumRowsDescription,
                       " ,multiplied with the biggest entry in "
                       "'benchmarkSampleSizeRatios' and ",
                       ratioRowsDescription));
    } else if (std::ceil(static_cast<double>(smallerTableNumRows) *
                         maxSampleSizeRatio) >
               getMaxValue<double>() -
                   std::ceil(
                       static_cast<double>(smallerTableNumRows * ratioRows) *
                       maxSampleSizeRatio)) {
      throwOverflowError<double>(absl::StrCat(
          "addition of the ", smallerTableNumRowsDescription,
          ",multiplied with the biggest entry in "
          "'benchmarkSampleSizeRatios', to the ",
          smallerTableNumRowsDescription, ", multiplied with ",
          ratioRowsDescription,
          " and the biggest entry in 'benchmarkSampleSizeRatios',"));
    } else if (!isValuePreservingCast<size_t>(std::floor(
                   static_cast<double>(smallerTableNumRows *
                                       smallerTableNumRows * ratioRows) /
                   (std::ceil(static_cast<double>(smallerTableNumRows) *
                              maxSampleSizeRatio) +
                    std::ceil(
                        static_cast<double>(smallerTableNumRows * ratioRows) *
                        maxSampleSizeRatio))))) {
      throw std::runtime_error(absl::StrCat(
          "size_t casting error: The calculated wanted result size in '",
          name(),
          "' has to be castable to size_t. Try increasing the values for the "
          "configuration options 'minRatioRows' or the biggest entry in "
          "'benchmarkSampleSizeRatios'. Or decreasing the value of "
          "'",
          smallerTableNumRowsConfigurationOptions, "'."));
    }
    /*
    If 'maxMemory' was set and our normal result is to big, we simply calculate
    the number of rows, rounded down, that a result with the maximum memory size
    would have.
    */
    size_t resultWantedNumRows{static_cast<size_t>(
        static_cast<double>(smallerTableNumRows * smallerTableNumRows *
                            ratioRows) /
        (std::ceil(static_cast<double>(smallerTableNumRows) *
                   maxSampleSizeRatio) +
         std::ceil(static_cast<double>(smallerTableNumRows * ratioRows) *
                   maxSampleSizeRatio)))};
    const size_t resultNumColumns{getConfigVariables().smallerTableNumColumns_ +
                                  getConfigVariables().biggerTableNumColumns_ -
                                  1};
    if (const auto& maxMemory{getConfigVariables().maxMemory()};
        maxMemory.has_value() &&
        maxMemory.value() < approximateMemoryNeededByIdTable(
                                resultWantedNumRows, resultNumColumns)) {
      resultWantedNumRows =
          memorySizeToIdTableRows(maxMemory.value(), resultNumColumns);
    }

    // Making a benchmark table for all combination of IdTables being sorted.
    for (const bool smallerTableSorted : {false, true}) {
      for (const bool biggerTableSorted : {false, true}) {
        for (const float smallerTableSampleSizeRatio : ratios) {
          const std::string& tableName = absl::StrCat(
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
          meta.addKeyValuePair("smallerTableSorted", smallerTableSorted);
          meta.addKeyValuePair("biggerTableSorted", biggerTableSorted);
          meta.addKeyValuePair("smallerTableJoinColumnSampleSizeRatio",
                               smallerTableSampleSizeRatio);
        }
      }
    }

    // Add the general metadata.
    BenchmarkMetadata& meta{getGeneralMetadata()};
    meta.addKeyValuePair("Value changing with every row",
                         "biggerTableJoinColumnSampleSizeRatio");
    meta.addKeyValuePair("ratioRows", ratioRows);
    meta.addKeyValuePair("smallerTableNumRows", smallerTableNumRows);
    meta.addKeyValuePair("wantesResultTableSize", resultWantedNumRows);
    meta.addKeyValuePair("benchmarkSampleSizeRatios",
                         getConfigVariables().benchmarkSampleSizeRatios_);
    GeneralInterfaceImplementation::addDefaultMetadata(&meta);
    return results;
  }
};

// Registering the benchmarks
AD_REGISTER_BENCHMARK(BmSameSizeRowGrowth);
AD_REGISTER_BENCHMARK(BmOnlySmallerTableSizeChanges);
AD_REGISTER_BENCHMARK(BmOnlyBiggerTableSizeChanges);
AD_REGISTER_BENCHMARK(BmSampleSizeRatio);
}  // namespace ad_benchmark
