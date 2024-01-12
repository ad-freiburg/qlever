// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (March of 2023, schlegea@informatik.uni-freiburg.de)

#pragma once

#include <absl/container/flat_hash_map.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_replace.h>
#include <gtest/gtest.h>

#include <any>
#include <concepts>
#include <functional>
#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <string_view>
#include <type_traits>
#include <typeinfo>
#include <unordered_set>
#include <variant>
#include <vector>

#include "util/ConfigManager/ConfigExceptions.h"
#include "util/ConfigManager/ConfigOption.h"
#include "util/ConfigManager/ConfigOptionProxy.h"
#include "util/ConfigManager/ConfigUtil.h"
#include "util/ConfigManager/Validator.h"
#include "util/ConfigManager/generated/ConfigShorthandLexer.h"
#include "util/Exception.h"
#include "util/Forward.h"
#include "util/HashMap.h"
#include "util/StringUtils.h"
#include "util/TypeTraits.h"
#include "util/json.h"

namespace ad_utility {
namespace ConfigManagerImpl {

// Shorthand concepts, to reduce code duplication.
class ConfigManager;
template <typename T>
concept ConfigOptionOrManager = SameAsAny<T, ConfigOption, ConfigManager>;

/*
Manages a bunch of `ConfigOption`s.
*/
class ConfigManager {
  /*
  Our hash map always saves either a configuration option, or a sub manager
  (another `ConfigManager`).

  This class makes the handeling more explicit and allows for more information
  to be saved alongside the configuration options and sub managers.
  For example: The order, in which they were created.
  */
  class HashMapEntry {
   public:
    using Data = std::variant<ConfigOption, ConfigManager>;

   private:
    std::unique_ptr<Data> data_;

    // Describes the order of initialization.
    static inline std::atomic_size_t numberOfInstances_{0};
    size_t initializationId_{numberOfInstances_++};

   public:
    // Construct an instance of this class managing `data`.
    explicit HashMapEntry(Data&& data);

    // Does this instance hold a configuration option?
    bool holdsConfigOption() const;

    // Does this instance hold a sub manager?
    bool holdsSubManager() const;

    /*
    Currently held configuration option, if there is one. Note: If there is a
    pointer, it's never a `nullptr`.
    */
    std::optional<ConfigOption*> getConfigOption();
    std::optional<const ConfigOption*> getConfigOption() const;

    /*
    Currently held sub manager, if there is one. Note: If there is a
    pointer, it's never a `nullptr`.
    */
    std::optional<ConfigManager*> getSubManager();
    std::optional<const ConfigManager*> getSubManager() const;

    // Return, how many instances of this class were initialized before this
    // instance.
    size_t getInitializationId() const;

    // Wrapper for calling `std::visit` on the saved `Data`.
    template <typename Visitor>
    requires std::invocable<Visitor, ConfigOption&> &&
             std::invocable<Visitor, ConfigManager&>
    decltype(auto) visit(Visitor&& vis);
    template <typename Visitor>
    requires std::invocable<Visitor, const ConfigOption&> &&
             std::invocable<Visitor, const ConfigManager&>
    decltype(auto) visit(Visitor&& vis) const;

   private:
    // Implementation for `holdsConfigOption` and `holdsSubManager`.
    template <SameAsAnyTypeIn<Data> T>
    bool implHolds() const;

    /*
    @brief Implementation for `getConfigOption` and `getSubManager`. You can
    specify which one via `ReturnType`.

    @tparam ReturnType Should be `(const) ConfigOption`, or `(const)
    ConfigManager`.

    @SameAsAny<const ConfigOption&, ConfigOption& >m instance The `HashMapEntry`
    you want this from.
    */
    template <SimilarToAnyTypeIn<Data> ReturnType>
    requires std::is_object_v<ReturnType>
    static std::optional<ReturnType*> getConfigOptionOrSubManager(
        ad_utility::SimilarTo<HashMapEntry> auto& instance);

    /*
    @brief The implementation for `visit`. Follows the same signature as
    `std::variant::visit`:
    */
    template <typename Visitor, ad_utility::SimilarTo<std::unique_ptr<
                                    ConfigManager::HashMapEntry::Data>>
                                    PointerType>
    requires std::invocable<Visitor, std::conditional_t<
                                         std::is_const_v<PointerType>,
                                         const ConfigOption&, ConfigOption&>> &&
             std::invocable<Visitor, std::conditional_t<
                                         std::is_const_v<PointerType>,
                                         const ConfigManager&, ConfigManager&>>
    static decltype(auto) visitImpl(Visitor&& vis, PointerType& data);
  };

  /*
  The added configuration options. Configuration managers are used by the user
  to describe a json object literal more explicitly.

  A configuration option tends to be placed like a key value pair in a json
  object. For example: `{"object 1" : [{"object 2" : { "The configuration option
  identifier" : "Configuration information"} }]}`

  The string key describes their location in the json object literal, by
  representing a json pointer in string form.
  */
  ad_utility::HashMap<std::string, HashMapEntry> configurationOptions_;

  /*
  List of the added validators. Whenever the values of the options are set,
  they are called afterwards with `verifyWithValidators` to make sure, that
  the new values are valid.
  If the new value isn't valid, it throws an exception.
  */
  std::vector<ConfigOptionValidatorManager> validators_;

 public:
  /*
  @brief Creates and adds a new configuration option without a default value.
  This configuration option must always be set at runtime.

  @tparam OptionType The type of value, the configuration option can hold.

  @param pathToOption Describes a path in json, that points to the value held by
  the configuration option. The last key in the vector is the name of the
  configuration option.
  @param optionDescription A description for the configuration option.
  @param variableToPutValueOfTheOptionIn The value held by the configuration
  option will be copied into this variable, whenever the value in the
  configuration option changes.

  @return A reference to the newly created configuration option. This reference
  will stay valid, even after adding more options.
  */
  template <SupportedConfigOptionType OptionType>
  ConstConfigOptionProxy<OptionType> addOption(
      const std::vector<std::string>& pathToOption,
      std::string_view optionDescription,
      OptionType* variableToPutValueOfTheOptionIn) {
    return addOptionImpl(pathToOption, optionDescription,
                         variableToPutValueOfTheOptionIn,
                         std::optional<OptionType>(std::nullopt));
  }

  /*
  @brief Creates and adds a new configuration option with a default value.
  Setting this option at runtime is optional and not required.

  @tparam OptionType The type of value, the configuration option can hold.

  @param pathToOption Describes a path in json, that points to the value held by
  the configuration option. The last key in the vector is the name of the
  configuration option.
  @param optionDescription A description for the configuration option.
  @param variableToPutValueOfTheOptionIn The value held by the configuration
  option will be copied into this variable, whenever the value in the
  configuration option changes.
  @param defaultValue A default value for the configuration option.

  @return A reference to the newly created configuration option. This reference
  will stay valid, even after adding more options.
  */
  template <SupportedConfigOptionType OptionType,
            std::same_as<OptionType> DefaultValueType = OptionType>
  ConstConfigOptionProxy<OptionType> addOption(
      const std::vector<std::string>& pathToOption,
      std::string_view optionDescription,
      OptionType* variableToPutValueOfTheOptionIn,
      DefaultValueType defaultValue) {
    return addOptionImpl(pathToOption, optionDescription,
                         variableToPutValueOfTheOptionIn,
                         std::optional<OptionType>(std::move(defaultValue)));
  }

  /*
  @brief Creates and adds a new configuration option, just like in the other
  `addOption`. But instead of a `pathToOption`, there is only an
  `optionName`, which describes a path only made out of this single string.

  @return A reference to the newly created configuration option. This reference
  will stay valid, even after adding more options.
  */
  template <SupportedConfigOptionType OptionType>
  ConstConfigOptionProxy<OptionType> addOption(
      std::string optionName, std::string_view optionDescription,
      OptionType* variableToPutValueOfTheOptionIn) {
    return addOption<OptionType>(
        std::vector<std::string>{std::move(optionName)}, optionDescription,
        variableToPutValueOfTheOptionIn);
  }

  /*
  @brief Creates and adds a new configuration option, just like in the other
  `addOption`. But instead of a `pathToOption`, there is only an
  `optionName`, which describes a path only made out of this single string.

  @return A reference to the newly created configuration option. This reference
  will stay valid, even after adding more options.
  */
  template <SupportedConfigOptionType OptionType,
            std::same_as<OptionType> DefaultValueType = OptionType>
  ConstConfigOptionProxy<OptionType> addOption(
      std::string optionName, std::string_view optionDescription,
      OptionType* variableToPutValueOfTheOptionIn,
      DefaultValueType defaultValue) {
    return addOption<OptionType>(
        std::vector<std::string>{std::move(optionName)}, optionDescription,
        variableToPutValueOfTheOptionIn, std::move(defaultValue));
  }

  /*
  @brief Creates and adds a new configuration manager with a prefix path for
  it's internally held configuration options and managers.

  @param pathToOption Describes a path in json, which will be a prefix to all
  the other paths held in the newly created ConfigManager.

  @return A reference to the newly created configuration manager. This reference
  will stay valid, even after adding more options.
  */
  ConfigManager& addSubManager(const std::vector<std::string>& path);

  /*
  @brief Sets the configuration options based on the given json.

  @param j There will be an exception thrown, if:
  - `j` doesn't contain values for all configuration options, that must be set
  at runtime.
  - Same, if there are values for configuration options, that do not exist, or
  are not contained in this manager, or its sub mangangers.
  - `j` is anything but a json object literal.
  - Any of the added validators return false.
  */
  void parseConfig(const nlohmann::json& j);

  /*
  @brief Parses the given short hand and returns it as a json object,
  that contains all the described configuration data.

  @param shortHandString The language of the short hand is defined in
  `generated/ConfigShorthand.g4`. The short hand is a format similar to JSON
  that is more suitable to be set directly from the command line
  */
  static nlohmann::json parseShortHand(const std::string& shortHandString);

  /*
  @brief Returns a string containing a json configuration and, optionally, the
  string representations of all added configuration options, togehter with the
  validator invariants.

  @param detailed Iff false, only print the json configuration. Otherwise, print
  the json configuration, followed by the string representations of all added
  configuration options, which contain more detailed information about the
  configuration options, togehter with the validator invariants
  */
  std::string printConfigurationDoc(bool detailed) const;

  /*
  @brief Add a function to the configuration manager for verifying, that the
  values of the given configuration options are valid. Will always be called
  after parsing.

  @tparam ValidatorParameterTypes The types of the values in the
  `configOptionsToBeChecked`. Can be left up to type deduction.

  @param validatorFunction Checks, if the values of the given configuration
  options are valid. Should return true, if they are valid.
  @param errorMessage A `std::runtime_error` with this as an error message will
  get thrown, if the `validatorFunction` returns false.
  @param validatorDescriptor A description of the invariant, that
  `validatorFunction` imposes.
  @param configOptionsToBeChecked Proxies for the configuration options, whos
  values will be passed to the validator function as function arguments. Will
  keep the same order.
  */
  template <typename... ValidatorParameterTypes>
  void addValidator(
      ValidatorFunction<ValidatorParameterTypes...> auto validatorFunction,
      std::string errorMessage, std::string validatorDescriptor,
      ConstConfigOptionProxy<
          ValidatorParameterTypes>... configOptionsToBeChecked)
      requires(sizeof...(configOptionsToBeChecked) > 0) {
    addValidatorImpl(
        "addValidator",
        []<typename T>(ConstConfigOptionProxy<T> opt) {
          return opt.getConfigOption().template getValue<std::decay_t<T>>();
        },
        transformValidatorIntoExceptionValidator<ValidatorParameterTypes...>(
            validatorFunction, std::move(errorMessage)),
        std::move(validatorDescriptor), configOptionsToBeChecked...);
  }

  /*
  @brief Add a function to the configuration manager for verifying, that the
  values of the given configuration options are valid. Will always be called
  after parsing.

  @tparam ValidatorParameterTypes The types of the values in the
  `configOptionsToBeChecked`. Can be left up to type deduction.

  @param exceptionValidatorFunction Checks, if the values of the given
  configuration options are valid. Return an empty instance of
  `std::optional<ErrorMessage>` if valid. Otherwise, the `ErrorMessage` should
  contain the reason, why the values are none valid.
  @param exceptionValidatorDescriptor A description of the invariant, that
  `exceptionValidatorFunction` imposes.
  @param configOptionsToBeChecked Proxies for the configuration options, the
  values of which will be passed to the exception validator function as function
  arguments. Will keep the same order.
  */
  template <typename... ExceptionValidatorParameterTypes>
  void addValidator(
      ExceptionValidatorFunction<ExceptionValidatorParameterTypes...> auto
          exceptionValidatorFunction,
      std::string exceptionValidatorDescriptor,
      ConstConfigOptionProxy<
          ExceptionValidatorParameterTypes>... configOptionsToBeChecked)
      requires(sizeof...(configOptionsToBeChecked) > 0) {
    addValidatorImpl(
        "addValidator",
        []<typename T>(ConstConfigOptionProxy<T> opt) {
          return opt.getConfigOption().template getValue<std::decay_t<T>>();
        },
        exceptionValidatorFunction, std::move(exceptionValidatorDescriptor),
        configOptionsToBeChecked...);
  }

  /*
  @brief Add a function to the configuration manager for verifying, that the
  given configuration options are valid. Will always be called after parsing.

  @param validatorFunction Checks, if the given configuration options are valid.
  Should return true, if they are valid.
  @param errorMessage A `std::runtime_error` with this as an error message will
  get thrown, if the `validatorFunction` returns false.
  @param validatorDescriptor A description of the invariant, that
  `validatorFunction` imposes.
  @param configOptionsToBeChecked Proxies for the configuration options, who
  will be passed to the validator function as function arguments. Will keep the
  same order.
  */
  template <typename ValidatorT>
  void addOptionValidator(
      ValidatorT validatorFunction, std::string errorMessage,
      std::string validatorDescriptor,
      isInstantiation<ConstConfigOptionProxy> auto... configOptionsToBeChecked)
      requires(
          sizeof...(configOptionsToBeChecked) > 0 &&
          ValidatorFunction<ValidatorT, decltype(configOptionsToBeChecked
                                                     .getConfigOption())...>) {
    addValidatorImpl(
        "addOptionValidator",
        []<typename T>(ConstConfigOptionProxy<T> opt) {
          return opt.getConfigOption();
        },
        transformValidatorIntoExceptionValidator<
            decltype(configOptionsToBeChecked.getConfigOption())...>(
            validatorFunction, std::move(errorMessage)),
        std::move(validatorDescriptor), configOptionsToBeChecked...);
  }

  /*
  @brief Add a function to the configuration manager for verifying, that the
  given configuration options are valid. Will always be called after parsing.

  @param exceptionValidatorFunction Checks, if the given configuration options
  are valid. Return an empty instance of `std::optional<ErrorMessage>` if valid.
  Otherwise, the `ErrorMessage` should contain the reason, why the options are
  none valid.
  @param exceptionValidatorDescriptor A description of the invariant, that
  `exceptionValidatorFunction` imposes.
  @param configOptionsToBeChecked Proxies for the configuration options, who
  will be passed to the validator function as function arguments. Will keep the
  same order.
  */
  template <typename ExceptionValidatorT>
  void addOptionValidator(
      ExceptionValidatorT exceptionValidatorFunction,
      std::string exceptionValidatorDescriptor,
      isInstantiation<ConstConfigOptionProxy> auto... configOptionsToBeChecked)
      requires(sizeof...(configOptionsToBeChecked) > 0 &&
               ExceptionValidatorFunction<
                   ExceptionValidatorT,
                   decltype(configOptionsToBeChecked.getConfigOption())...>) {
    addValidatorImpl(
        "addOptionValidator",
        []<typename T>(ConstConfigOptionProxy<T> opt) {
          return opt.getConfigOption();
        },
        exceptionValidatorFunction, std::move(exceptionValidatorDescriptor),
        configOptionsToBeChecked...);
  }

 private:
  FRIEND_TEST(ConfigManagerTest, ParseConfig);
  FRIEND_TEST(ConfigManagerTest, ParseConfigExceptionTest);
  FRIEND_TEST(ConfigManagerTest, ParseShortHandTest);
  FRIEND_TEST(ConfigManagerTest, ContainsOption);
  FRIEND_TEST(ConfigManagerTest, ValidatorsSorting);
  FRIEND_TEST(ConfigManagerTest, ConfigurationDocValidatorAssignment);

  /*
  @brief Visit the entries of `configurationOptions_` by visiting the content of
  the `HashMapEntry`s, see `std::visit`, paired with the key for the
  `HashMapEntry`.
  Note: Also checks integrity of all entries via `verifyHashMapEntry` before
  passing them to `vis`.

  @param vis The visitor function. Will either be called with
  `std::string_view, ConfigManager&`, or
  `std::string_view, ConfigOption&`. Both represent the content of
  a `HashMapEntry`.
  @param sortByCreationOrder Should `vis` get the entries of
  `configurationOptions_` ordered by their creation order, when set to true, or
  should the order be random?
  @param pathPrefix Only used for improved error messages and not passed to
  `vis`. For example: You have a sub manager with the path `subManagers/sub1`
  and call `visitHashMapEntries` with it. The sub manager doesn't know its own
  path, so that information will only be included in generated error messages,
  if you pass it along.
  */
  template <typename Visitor>
  requires ad_utility::InvocableWithExactReturnType<
               Visitor, void, std::string_view, ConfigManager&> &&
           ad_utility::InvocableWithExactReturnType<
               Visitor, void, std::string_view, ConfigOption&>
  void visitHashMapEntries(Visitor&& vis, bool sortByCreationOrder,
                           std::string_view pathPrefix) const;

  /*
  @brief Collect all `HashMapEntry` contained in the `hashMap`, including the
  ones in sub managern and return them together with their json paths in a
  random order. Note: Also checks integrity of all entries via
  `verifyHashMapEntry`.

  @param pathPrefix This prefix will be added to all json paths, that will be
  returned.
  @param predicate Only the `HashMapEntry` for which a true is returned, will be
  given back.
  */
  template <
      SimilarTo<ad_utility::HashMap<std::string, HashMapEntry>> HashMapType>
  requires std::is_object_v<HashMapType> static std::conditional_t<
      std::is_const_v<HashMapType>,
      const std::vector<std::pair<const std::string, const HashMapEntry&>>,
      std::vector<std::pair<std::string, HashMapEntry&>>>
  allHashMapEntries(HashMapType& hashMap, std::string_view pathPrefix,
                    const ad_utility::InvocableWithSimilarReturnType<
                        bool, const HashMapEntry&> auto& predicate);

  /*
  @brief Creates the string representation of a valid `nlohmann::json` pointer
  based on the given keys.
  */
  static std::string createJsonPointerString(
      const std::vector<std::string>& keys);

  /*
  @brief Verifies, that the given path is a valid path for an option, or sub
  manager. If not, throws exceptions.

  @param pathToOption Describes a path in json.
  */
  void verifyPath(const std::vector<std::string>& path) const;

  /*
  @brief Adds a configuration option, that can be accessed with the given path.

  @param pathToOption Describes a path in json, that points to the value held by
  the configuration option.

  @return The added config option.
  */
  ConfigOption& addConfigOption(const std::vector<std::string>& pathToOption,
                                ConfigOption&& option);

  /*
  @brief Return string representation of a `std::vector<std::string>`.
  */
  static std::string vectorOfKeysForJsonToString(
      const std::vector<std::string>& keys);

  /*
  @brief Creates and adds a new configuration option.

  @tparam OptionType The type of value, the configuration option can hold.

  @param pathToOption Describes a path in json, that points to the value held by
  the configuration option. The last key in the vector is the name of the
  configuration option.
  @param optionDescription A description for the configuration option.
  @param variableToPutValueOfTheOptionIn The value held by the configuration
  option will be copied into this variable, whenever the value in the
  configuration option changes.
  @param defaultValue A default value for the configuration option. If none is
  given, signified by an empty optional, then a value for the configuration
  option MUST be given at runtime.

  @return A reference to the newly created configuration option. Will stay
  valid, even after more options.
  */
  template <
      ad_utility::SameAsAnyTypeIn<ConfigOption::AvailableTypes> OptionType>
  ConstConfigOptionProxy<OptionType> addOptionImpl(
      const std::vector<std::string>& pathToOption,
      std::string_view optionDescription,
      OptionType* variableToPutValueOfTheOptionIn,
      std::optional<OptionType> defaultValue =
          std::optional<OptionType>(std::nullopt)) {
    verifyPath(pathToOption);

    /*
    We can't just create a `ConfigOption` variable, pass it and return it,
    because the hash map has ownership of the newly added `ConfigOption`.
    Which was transfered via creating a new internal `ConfigOption` with move
    constructors.
    Which would mean, that our local `ConfigOption` isn't the `ConfigOption` we
    want to return a reference to.
    */
    return ConstConfigOptionProxy<OptionType>(addConfigOption(
        pathToOption,
        ConfigOption(pathToOption.back(), optionDescription,
                     variableToPutValueOfTheOptionIn, defaultValue)));
  }

  /*
  @brief A vector to all the configuratio options, held by this manager and in a
  random order, represented with their json paths and reference to them. Options
  held by a sub manager, are also included with the path to the sub manager as
  prefix.
  */
  std::vector<std::pair<std::string, ConfigOption&>> configurationOptions();
  std::vector<std::pair<std::string, const ConfigOption&>>
  configurationOptions() const;

  /*
  @brief The implementation for `configurationOptions()`.

  @tparam ReturnReference Should be either `ConfigOption&`, or `const
  ConfigOption&`.
  */
  template <SameAsAny<ConfigOption&, const ConfigOption&> ReturnReference>
  static std::vector<std::pair<std::string, ReturnReference>>
  configurationOptionsImpl(
      SimilarTo<ad_utility::HashMap<std::string, HashMapEntry>> auto&
          configurationOptions);

  /*
  @brief Return all `ConfigOptionValidatorManager` held by this manager and its
  sub managers.

  @param sortByInitialization If true, the order of the returned
  `ConfigOptionValidatorManager` is by initialization order. If false, the order
  is random.
  */
  std::vector<std::reference_wrapper<const ConfigOptionValidatorManager>>
  validators(const bool sortByInitialization) const;

  /*
  @brief Call all the validators to check, if the current value is valid.
  */
  void verifyWithValidators() const;

  /*
  Checks, if the given configuration option is contained in the manager.
  Note: This checks for identity (pointer equality), not semantic equality.
  */
  bool containsOption(const ConfigOption& opt) const;

  /*
  @brief The implementation of `addValidator` and `addOptionValidator`. Note:
  Normal validator functions must first be transformed in to an exception
  validator function via `transformValidatorIntoExceptionValidator`.

  @param addValidatorFunctionName The name of the function, that is calling this
  implementation. So that people can easier identify them.
  @param translationFunction Transforms the given `ConstConfigOptionProxy` in
  the value form for the exception validator function.
  @param exceptionValidatorFunc Should return an empty
  `std::optional<ErrorMessage>`, if the transformed `ConstConfigOptionProxy`
  represent valid values. Otherwise, it should return an error message.
  For example: There configuration options were all set at run time, or contain
  numbers bigger than 10.
  @param exceptionValidatorDescriptor A description of the invariant, that
  `exceptionValidatorFunc` imposes.
  @param configOptionsToBeChecked Proxies for the configuration options, who
  will be passed to the exception validator function as function arguments,
  after being transformed. Will keep the same order.
  */
  template <
      typename TranslationFunction, typename ExceptionValidatorFunc,
      isInstantiation<ConstConfigOptionProxy>... ExceptionValidatorParameter>
  void addValidatorImpl(
      std::string_view addValidatorFunctionName,
      TranslationFunction translationFunction,
      ExceptionValidatorFunc exceptionValidatorFunction,
      std::string exceptionValidatorDescriptor,
      ExceptionValidatorParameter... configOptionsToBeChecked) {
    // Check, if we contain all the configuration options, that were given us.
    auto checkIfContainOption = [this, &addValidatorFunctionName]<typename T>(
                                    ConstConfigOptionProxy<T> opt) {
      if (!containsOption(opt.getConfigOption())) {
        throw std::runtime_error(absl::StrCat(
            "Error while adding validator with ", addValidatorFunctionName,
            ": The given configuration "
            "option '",
            opt.getConfigOption().getIdentifier(),
            "' is not contained in the configuration manager."));
      }
    };
    (checkIfContainOption(configOptionsToBeChecked), ...);

    validators_.emplace_back(std::move(exceptionValidatorFunction),
                             std::move(exceptionValidatorDescriptor),
                             std::move(translationFunction),
                             std::move(configOptionsToBeChecked)...);
  }

  /*
  @brief Throw an exception, if the given entry of `configurationOptions_`
  contains an empty sub manager, which would point to a logic error.

  @param jsonPathToEntry For a better exception message.
  @param entry The object managed by the hash map.
  */
  static void verifyHashMapEntry(std::string_view jsonPathToEntry,
                                 const HashMapEntry& entry);

  /*
  @brief Generate the json representation the current config manager
  configuration.

  @param pathPrefix Only used for improved error messages. For example: You have
  a sub manager with the path `subManagers/sub1` and call `visitHashMapEntries`
  with it. The sub manager doesn't know its own path, so that information will
  only be included in generated error messages, if you pass it along.
  */
  nlohmann::ordered_json generateConfigurationDocJson(
      std::string_view pathPrefix) const;

  /*
  When printing the configuration documentation, the assignment of `Validators`
  isn't 100% clear.
  Should they be printed as part of a sub manager? As part of every
  `ConfigOption`, they check? Or a split up between sub managers and
  `ConfigOption`?
  This class is for clearing up those assignments, by assigning
  `ConfigOptionValidatorManager` to `ConfigOption` and `ConfigManager`.
  Note:
  - Those assignments will be done using memory addresses and pointers. No
  objects will be copied.
  - Keys and values are identified via identity/memory address.
  - The insertion order under a key will be preserved. For example: If you add
  validator `a` and `b` under `ConfigOption C`, then when retrieving the entries
  for `C` they will have the order `a`, followed by `b`.
  */
  class ConfigurationDocValidatorAssignment {
   public:
    // The return type for value getter.
    using ValueGetterReturnType =
        std::vector<std::reference_wrapper<const ConfigOptionValidatorManager>>;

   private:
    /*
    Hash maps of the memory address.
    */
    template <typename PointerObject>
    using MemoryAdressHashMap =
        ad_utility::HashMap<const std::decay_t<PointerObject>*,
                            std::vector<const ConfigOptionValidatorManager*>>;
    MemoryAdressHashMap<ConfigOption> configOption_;
    MemoryAdressHashMap<ConfigManager> configManager_;

   public:
    /*
    @brief Add a validator to the list of validators, that are assigned to a
    `ConfigOption`/`ConfigManager`.
    */
    template <ConfigOptionOrManager T>
    void addEntryUnderKey(const T& key,
                          const ConfigOptionValidatorManager& manager);

    /*
    @brief Retrieve the list of validators, that are assigned to a
    `ConfigOption`/`ConfigManager`.

    @returns If there is no entry for `Key`, return an empty `std::vector`.
    */
    template <ConfigOptionOrManager T>
    ValueGetterReturnType getEntriesUnderKey(const T& key) const;

   private:
    // Return either `configOption_` or `configManager_`, based on type.
    template <ConfigOptionOrManager T>
    constexpr const MemoryAdressHashMap<T>& getHashMapBasedOnType() const {
      if constexpr (std::same_as<T, ConfigOption>) {
        return configOption_;
      } else if constexpr (std::same_as<T, ConfigManager>) {
        return configManager_;
      }
    }
    template <ConfigOptionOrManager T>
    constexpr MemoryAdressHashMap<T>& getHashMapBasedOnType() {
      if constexpr (std::same_as<T, ConfigOption>) {
        return configOption_;
      } else if constexpr (std::same_as<T, ConfigManager>) {
        return configManager_;
      }
    }
  };

  /*
  @brief Create the validator assignments based on this instance. Note, that
  these assignments are done based on all the held sub manager und configuration
  options. In other words, you only need to call it with the top manager.

  Our current assignment strategy is:
  - Validator, that only check a single configuration option, are assigned to
  that option.
  - Validator, that check more than one single configuration option, are
  assigned to the configuration manager, that holds them.
  - Any list of `ConfigOptionValidatorManager` is sorted by their creation
  order.
  */
  ConfigurationDocValidatorAssignment getValidatorAssignment() const;

  /*
  @brief Create a detailed list about the configuration options, with their
  types, values, default values,  etc. shown and organized by the sub managers,
  that hold them. Validator invariant descriptions will be printed according to
  `ConfigurationDocValidatorAssignment`.

  @param pathPrefix Only used for improved error messages. For example: You
  have a sub manager with the path `subManagers/sub1` and call
  `visitHashMapEntries` with it. The sub manager doesn't know its own path, so
  that information will only be included in generated error messages, if you
  pass it along.
  @param assignment Whenever printing a `ConfigOption`/`ConfigManager`, the
  validators assigned to them, will be printed with them.
  */
  std::string generateConfigurationDocDetailedList(
      std::string_view pathPrefix,
      const ConfigurationDocValidatorAssignment& assignment) const;
};
}  // namespace ConfigManagerImpl
using ConfigManagerImpl::ConfigManager;
}  // namespace ad_utility
