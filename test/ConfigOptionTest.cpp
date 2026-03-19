// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (May of 2023, schlegea@informatik.uni-freiburg.de)
//
// Copyright 2025, Bayerische Motoren Werke Aktiengesellschaft (BMW AG)

#include <gtest/gtest.h>

#include <optional>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include "../test/util/ConfigOptionHelpers.h"
#include "backports/type_traits.h"
#include "util/ConfigManager/ConfigExceptions.h"
#include "util/ConfigManager/ConfigOption.h"
#include "util/ConstexprUtils.h"
#include "util/Exception.h"
#include "util/GTestHelpers.h"
#include "util/json.h"

namespace ad_utility {
/*
Not all identifiers are allowed for configuration options.
*/
TEST(ConfigOptionTest, ConstructorException) {
  bool notUsed;

  // No name.
  ASSERT_THROW(ConfigOption("", "", &notUsed),
               ad_utility::NotValidShortHandNameException);

  // Names with spaces.
  ASSERT_THROW(ConfigOption("Option 1", "", &notUsed),
               ad_utility::NotValidShortHandNameException);

  // The variable pointer is a null pointer.
  int* ptr = nullptr;
  ASSERT_THROW(ConfigOption("Option", "", ptr),
               ad_utility::ConfigOptionConstructorNullPointerException);
}

/*
The form of a generic test case, for when a value gets converted to it's string,
or json, representation. Other the other way around.
*/
template <typename T>
struct ConversionTestCase {
  const T value;
  const nlohmann::json jsonRepresentation;
};

/*
@brief Return a `ConversionTestCase` case for the given type.
*/
template <typename T>
ConversionTestCase<T> getConversionTestCase() {
  if constexpr (std::is_same_v<T, bool>) {
    return ConversionTestCase<bool>{
        true,
        nlohmann::json::parse(R"--(true)--"),
    };
  } else if constexpr (std::is_same_v<T, std::string>) {
    return ConversionTestCase<std::string>{
        std::string{"set"}, nlohmann::json::parse(R"--("set")--")};
  } else if constexpr (std::is_same_v<T, int>) {
    return ConversionTestCase<int>{-42, nlohmann::json::parse(R"--(-42)--")};
  } else if constexpr (std::is_same_v<T, size_t>) {
    return ConversionTestCase<size_t>{42, nlohmann::json::parse(R"--(42)--")};
  } else if constexpr (std::is_same_v<T, float>) {
    return ConversionTestCase<float>{42.5,
                                     nlohmann::json::parse(R"--(42.5)--")};
  } else if constexpr (std::is_same_v<T, std::vector<bool>>) {
    return ConversionTestCase<std::vector<bool>>{
        std::vector{true, true}, nlohmann::json::parse(R"--([true, true])--")};
  } else if constexpr (std::is_same_v<T, std::vector<std::string>>) {
    return ConversionTestCase<std::vector<std::string>>{
        std::vector{std::string{"str"}, std::string{"str"}},
        nlohmann::json::parse(R"--(["str", "str"])--")};
  } else if constexpr (std::is_same_v<T, std::vector<int>>) {
    return ConversionTestCase<std::vector<int>>{
        std::vector{-42, 42}, nlohmann::json::parse(R"--([-42, 42])--")};
  } else if constexpr (std::is_same_v<T, std::vector<size_t>>) {
    return ConversionTestCase<std::vector<size_t>>{
        std::vector{42uL, 42uL}, nlohmann::json::parse(R"--([42, 42])--")};
  } else {
    // Must be a vector of floats.
    static_assert(std::is_same_v<T, std::vector<float>>);

    return ConversionTestCase<std::vector<float>>{
        {42.5f, 42.5f}, nlohmann::json::parse(R"--([42.5, 42.5])--")};
  }
}

/*
  Checks, if the `ConfigOption::getValue` only works with the actual type
  of the value in the configuration option. All the other types should cause an
  exception.
  */
struct OtherGettersDontWork {
  template <typename WorkingType>
  struct CheckCurrentType {
    const ConfigOption& option;

    template <typename CurrentType>
    void operator()() const {
      if (option.wasSet()) {
        if constexpr (!std::is_same_v<WorkingType, CurrentType>) {
          ASSERT_THROW((option.getValue<CurrentType>()),
                       ad_utility::ConfigOptionGetWrongTypeException);
        } else {
          ASSERT_NO_THROW((option.getValue<CurrentType>()));
        }

        ASSERT_NO_THROW(option.getValueAsJson());
        ASSERT_NO_THROW(option.getValueAsString());

      } else {
        ASSERT_THROW((option.getValue<CurrentType>()),
                     ad_utility::ConfigOptionValueNotSetException);
        ASSERT_ANY_THROW(option.getValueAsJson());
        ASSERT_ANY_THROW(option.getValueAsString());
      }

      if (option.hasDefaultValue()) {
        if constexpr (!std::is_same_v<WorkingType, CurrentType>) {
          ASSERT_THROW((option.getDefaultValue<CurrentType>()),
                       ad_utility::ConfigOptionGetWrongTypeException);
        } else {
          ASSERT_NO_THROW((option.getDefaultValue<CurrentType>()));
        }

        ASSERT_FALSE(option.getDefaultValueAsJson().is_null());
        ASSERT_NE("None", option.getDefaultValueAsString());
      } else {
        ASSERT_THROW((option.getDefaultValue<CurrentType>()),
                     ad_utility::ConfigOptionValueNotSetException);
        ASSERT_TRUE(option.getDefaultValueAsJson().is_null());
        ASSERT_EQ("None", option.getDefaultValueAsString());
      }
    }
  };

  template <typename WorkingType>
  void operator()(const ConfigOption& option) const {
    doForTypeInConfigOptionValueType(CheckCurrentType<WorkingType>{option});
  }
};

/*
Check if the creation of configuration options, their direct setting and the
getter works as intended.
*/
TEST(ConfigOptionTest, CreateSetAndTest) {
  OtherGettersDontWork otherGettersDontWork{};
  /*
  Set the value of a configuration option and check, that it was set
  correctly.
  */
  auto setAndTest =
      [&otherGettersDontWork](
          ConfigOption& option, auto* variablePointer,
          const ConversionTestCase<
              std::remove_pointer_t<decltype(variablePointer)>>& toSetTo) {
        using Type = std::remove_pointer_t<decltype(variablePointer)>;
        ASSERT_FALSE(option.wasSetAtRuntime());

        option.setValue(toSetTo.value);

        ASSERT_TRUE(option.wasSet() && option.wasSetAtRuntime());
        ASSERT_EQ(toSetTo.value, option.getValue<Type>());
        ASSERT_EQ(toSetTo.value, *variablePointer);

        // Make sure, that the other getters don't work.
        otherGettersDontWork.template operator()<Type>(option);
      };

  /*
  Run a normal test case of creating a configuration option, checking it and
  setting it. With or without a default value.
  */
  auto testCaseWithDefault = [&setAndTest,
                              &otherGettersDontWork](const auto& toSetTo) {
    using Type = std::decay_t<decltype(toSetTo.value)>;

    // Every configuration option keeps updating an external variable with
    // the value, that it itself holds. This is the one.
    Type configurationOptionValue;

    // The default value.
    const ConversionTestCase<Type> defaultCase{getConversionTestCase<Type>()};

    ConfigOption option{ConfigOption("With_default", "",
                                     &configurationOptionValue,
                                     std::optional{defaultCase.value})};

    // Can we use the default value correctly?
    ASSERT_TRUE(option.wasSet() && option.hasDefaultValue());
    ASSERT_EQ(defaultCase.value, option.getDefaultValue<Type>());
    ASSERT_EQ(defaultCase.value, option.getValue<Type>());
    ASSERT_EQ(defaultCase.value, configurationOptionValue);
    otherGettersDontWork.template operator()<Type>(option);

    setAndTest.template operator()<Type>(option, &configurationOptionValue,
                                         toSetTo);

    // Is the default value unchanged?
    ASSERT_TRUE(option.hasDefaultValue());
    ASSERT_EQ(defaultCase.value, option.getDefaultValue<Type>());
    ASSERT_EQ(defaultCase.jsonRepresentation, option.getDefaultValueAsJson());
  };

  auto testCaseWithoutDefault = [&otherGettersDontWork,
                                 &setAndTest](const auto& toSetTo) {
    using Type = std::decay_t<decltype(toSetTo.value)>;

    // Every configuration option keeps updating an external variable with
    // the value, that it itself holds. This is the one.
    Type configurationOptionValue;

    ConfigOption option{
        ConfigOption("Without_default", "", &configurationOptionValue)};

    // Make sure, that we truly don't have a default value, that can be
    // gotten.
    ASSERT_TRUE(!option.wasSet() && !option.hasDefaultValue());
    ASSERT_THROW(option.getDefaultValue<Type>(),
                 ad_utility::ConfigOptionValueNotSetException);
    ASSERT_TRUE(option.getDefaultValueAsJson().empty());
    otherGettersDontWork.template operator()<Type>(option);

    setAndTest.template operator()<Type>(option, &configurationOptionValue,
                                         toSetTo);

    // Is it still the case, that we don't have a default value?
    ASSERT_TRUE(!option.hasDefaultValue());
    ASSERT_THROW(option.getDefaultValue<Type>(),
                 ad_utility::ConfigOptionValueNotSetException);
    ASSERT_TRUE(option.getDefaultValueAsJson().empty());
    ASSERT_EQ("None", option.getDefaultValueAsString());
  };

  // Do a test case for every possible type.
  testCaseWithDefault(
      ConversionTestCase<bool>{false, nlohmann::json::parse(R"--(false)--")});
  testCaseWithoutDefault(
      ConversionTestCase<bool>{false, nlohmann::json::parse(R"--(false)--")});

  testCaseWithDefault(ConversionTestCase<std::string>{
      "unset", nlohmann::json::parse(R"--("unset")--")});
  testCaseWithoutDefault(ConversionTestCase<std::string>{
      "unset", nlohmann::json::parse(R"--("unset")--")});

  testCaseWithDefault(
      ConversionTestCase<int>{-40, nlohmann::json::parse(R"--(-40)--")});
  testCaseWithoutDefault(
      ConversionTestCase<int>{-40, nlohmann::json::parse(R"--(-40)--")});

  testCaseWithDefault(
      ConversionTestCase<size_t>{40uL, nlohmann::json::parse(R"--(40)--")});
  testCaseWithoutDefault(
      ConversionTestCase<size_t>{40uL, nlohmann::json::parse(R"--(40)--")});

  testCaseWithDefault(
      ConversionTestCase<float>{40.5, nlohmann::json::parse(R"--(40.5)--")});
  testCaseWithoutDefault(
      ConversionTestCase<float>{40.5, nlohmann::json::parse(R"--(40.5)--")});

  testCaseWithDefault(ConversionTestCase<std::vector<bool>>{
      std::vector{false, true}, nlohmann::json::parse(R"--([false, true])--")});
  testCaseWithoutDefault(ConversionTestCase<std::vector<bool>>{
      std::vector{false, true}, nlohmann::json::parse(R"--([false, true])--")});

  testCaseWithDefault(ConversionTestCase<std::vector<std::string>>{
      std::vector<std::string>{"str1", "str2"},
      nlohmann::json::parse(R"--(["str1", "str2"])--")});
  testCaseWithoutDefault(ConversionTestCase<std::vector<std::string>>{
      std::vector<std::string>{"str1", "str2"},
      nlohmann::json::parse(R"--(["str1", "str2"])--")});

  testCaseWithDefault(ConversionTestCase<std::vector<int>>{
      {-40, 41}, nlohmann::json::parse(R"--([-40, 41])--")});
  testCaseWithoutDefault(ConversionTestCase<std::vector<int>>{
      {-40, 41}, nlohmann::json::parse(R"--([-40, 41])--")});

  testCaseWithDefault(ConversionTestCase<std::vector<size_t>>{
      {40uL, 41uL}, nlohmann::json::parse(R"--([40, 41])--")});
  testCaseWithoutDefault(ConversionTestCase<std::vector<size_t>>{
      {40uL, 41uL}, nlohmann::json::parse(R"--([40, 41])--")});

  testCaseWithDefault(ConversionTestCase<std::vector<float>>{
      {40.7, 40.913}, nlohmann::json::parse(R"--([40.7, 40.913])--")});
  testCaseWithoutDefault(ConversionTestCase<std::vector<float>>{
      {40.7, 40.913}, nlohmann::json::parse(R"--([40.7, 40.913])--")});
}

// `ConfigOption` should always throw an exception, when created
// like this.
TEST(ConfigOptionTest, ExceptionOnCreation) {
  // No identifier.
  bool notUsed;
  ASSERT_THROW(ConfigOption("", "", &notUsed);
               , ad_utility::NotValidShortHandNameException);
}

struct TrySetConfigOptionToAllTypes {
  template <typename WorkingType>
  struct TrySet {
    ConfigOption& option;
    WorkingType& notUsed;

    template <typename T>
    void operator()() const {
      if constexpr (std::is_same_v<T, WorkingType>) {
        ASSERT_NO_THROW(option.setValue(notUsed));
      } else {
        ASSERT_THROW(option.setValue(getConversionTestCase<T>().value),
                     ad_utility::ConfigOptionSetWrongTypeException);
      }
    }
  };

  template <typename WorkingType>
  void operator()() const {
    WorkingType notUsed{getConversionTestCase<WorkingType>().value};
    ConfigOption option("option", "", &notUsed);

    doForTypeInConfigOptionValueType(TrySet<WorkingType>{option, notUsed});
  }
};

// Test, if a config option can only be set to values of the same type, as it is
// meant to hold.
TEST(ConfigOptionTest, SetValueException) {
  // Try every type combination.
  doForTypeInConfigOptionValueType(TrySetConfigOptionToAllTypes{});
}

/*
  Set the value of a configuration option and check, that it was set
  correctly.
  */
struct CheckConfigOptionSetValue {
  template <typename Type>
  struct CheckIfThrows {
    ConfigOption& option;

    template <typename CurrentType>
    void operator()() const {
      if constexpr (!std::is_same_v<Type, CurrentType> &&
                    !(std::is_same_v<Type, int> &&
                      std::is_same_v<
                          CurrentType,
                          size_t>)&&!(std::is_same_v<Type, std::vector<int>> &&
                                      std::is_same_v<CurrentType,
                                                     std::vector<size_t>>)) {
        ASSERT_THROW(
            option.setValueWithJson(
                getConversionTestCase<CurrentType>().jsonRepresentation),
            ad_utility::ConfigOptionSetWrongJsonTypeException);
      }
    }
  };

  template <typename Type>
  void operator()() const {
    // Every configuration option keeps updating an external variable with the
    // value, that it itself holds. This is the one.
    Type configurationOptionValue;

    ConfigOption option{ConfigOption("t", "", &configurationOptionValue)};

    const auto& currentTest = getConversionTestCase<Type>();

    option.setValueWithJson(currentTest.jsonRepresentation);

    // Is it set correctly?
    ASSERT_TRUE(option.wasSet());
    ASSERT_EQ(currentTest.value, option.getValue<Type>());
    ASSERT_EQ(currentTest.value, configurationOptionValue);

    // Does the setter cause an exception, when given any json, that can't be
    // interpreted as the wanted type?
    doForTypeInConfigOptionValueType(CheckIfThrows<Type>{option});

    ASSERT_ANY_THROW(option.setValueWithJson(nlohmann::json::parse(
        R"--("the value is in here " : [true, 4, 4.2])--")));
  }
};

/*
`ConfigOption::setValueWithJson` interprets the given json as the type of
the configuration option. This tests, if this works correctly.
*/
TEST(ConfigOptionTest, SetValueWithJson) {
  // Do the test case for every possible type.
  doForTypeInConfigOptionValueType(CheckConfigOptionSetValue{});
}

// Test for `configOption` with the given type.
struct CheckConfigOptionHoldsType {
  template <typename CorrectType>
  struct CheckType {
    const ConfigOption& opt;

    template <typename WrongType>
    void operator()() const {
      if constexpr (!std::is_same_v<CorrectType, WrongType>) {
        ASSERT_FALSE(opt.holdsType<WrongType>());
      }
    }
  };

  template <typename CorrectType>
  void operator()() const {
    // Correct type.
    CorrectType var;
    const ConfigOption opt("testOption", "", &var);
    ASSERT_TRUE(opt.holdsType<CorrectType>());

    // Wrong type.
    doForTypeInConfigOptionValueType(CheckType<CorrectType>{opt});
  }
};

TEST(ConfigOptionTest, HoldsType) {
  doForTypeInConfigOptionValueType(CheckConfigOptionHoldsType{});
}

}  // namespace ad_utility
