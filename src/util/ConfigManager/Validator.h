// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (September of 2023,
// schlegea@informatik.uni-freiburg.de)

#ifndef QLEVER_SRC_UTIL_CONFIGMANAGER_VALIDATOR_H
#define QLEVER_SRC_UTIL_CONFIGMANAGER_VALIDATOR_H

#include <atomic>
#include <optional>
#include <stdexcept>
#include <string>
#include <unordered_set>

#include "backports/functional.h"
#include "util/ConfigManager/ConfigOption.h"
#include "util/ConfigManager/ConfigOptionProxy.h"
#include "util/HashSet.h"
#include "util/StringUtils.h"
#include "util/TypeTraits.h"

namespace ad_utility {
/*
A validator function is any invocable object, who takes the given parameter
types, in the same order and transformed to `const type&`, and returns a
bool.
*/
template <typename Func, typename... ParameterTypes>
CPP_concept ValidatorFunction =
    RegularInvocableWithSimilarReturnType<Func, bool, const ParameterTypes&...>;

// Simple struct, that holds an error message. For use as the return type of
// invocable object, that fulfill `ExceptionValidator`.
struct ErrorMessage {
 private:
  std::string message_;

 public:
  // Constructor.
  explicit ErrorMessage(std::string message) : message_{std::move(message)} {}

  // Getter for the message.
  const std::string& getMessage() const;
};

/*
An exception validator function is any invocable object, who takes the given
parameter types, in the same order and transformed to `const type&`, and returns
an instance of `std::optional<ErrorMessage>`.
*/
template <typename Func, typename... ParameterTypes>
CPP_concept ExceptionValidatorFunction =
    RegularInvocableWithSimilarReturnType<Func, std::optional<ErrorMessage>,
                                          const ParameterTypes&...>;

/*
@brief Transform a validator function into an exception validator function.
That is, instead of returning `bool`, it now returns
`std::optional<ErrorMessage>`.

@param errorMessage Content for `ErrorMessage`.
*/
CPP_template(typename ValidatorFunc, typename... ValidatorParameterTypes)(
    requires ValidatorFunction<
        ValidatorFunc,
        ValidatorParameterTypes...>) inline auto transformValidatorIntoExceptionValidator(ValidatorFunc
                                                                                              validatorFunction,
                                                                                          std::string
                                                                                              errorMessage) {
  // The whole 'transformation' is simply a wrapper.
  return [validatorFunction = std::move(validatorFunction),
          errorMessage = ErrorMessage{std::move(errorMessage)}](
             const ValidatorParameterTypes&... args)
             -> std::optional<ErrorMessage> {
    if (std::invoke(validatorFunction, args...)) {
      return std::nullopt;
    } else {
      return errorMessage;
    }
  };
}

/*
A class for managing a validator, exception or normal, that checks
`ConfigOption`s.

This class saves references to the `ConfigOption`s for the validator function,
calls the function with the references on demand and throws an exception, if the
validator returns a value, that signals, that the given `ConfigOption` weren't
valid.
*/
class ConfigOptionValidatorManager {
  /*
  Instead of actually saving/managing the given validator function, we save a
  wrapper, which implements our wanted behavior and which we construct at
  construction.

  Wanted behavior:
  - Call the given validator function with the current state of the
  `ConfigOption`, that were given at construction, after translating them.
  - If the validator function returns a value, that signals, that the given
  `ConfigOption` weren't valid, throw an exception with a custom error message.
  */
  std::function<void(void)> wrappedValidatorFunction_;

  // A description of the invariant, this validator imposes.
  std::string descriptor_;

  // Pointer to the `configOption`, that will be checked.
  ad_utility::HashSet<const ConfigOption*> configOptionsToBeChecked_;

  // Describes the order of initialization.
  static inline std::atomic_size_t numberOfInstances_{0};
  size_t initializationId_{numberOfInstances_++};

 public:
  /*
  @brief Create a `ConfigOptionValidatorManager`, which will call the given
  exception validator function with the given `ConfigOption`s, after translating
  them via the translator function.

  @param exceptionValidatorFunction Should return an empty
  `std::optional<ErrorMessage>`, if the transformed `ConstConfigOptionProxy`
  represent valid values. Otherwise, it should return an error message. For
  example: There configuration options were all set at run time, or contain
  numbers bigger than 10.
  @param descriptor A description of the invariant, that exception validator
  function imposes.
  @param configOptionsToBeChecked Proxies for the configuration options, who
  will be passed to the exception validator function as function arguments,
  after being transformed. Will keep the same order.
  */
  CPP_template(typename TranslationFunction, typename ExceptionValidatorFunc,
               typename... ExceptionValidatorParameterTypes)(
      requires(...&& isInstantiation<ExceptionValidatorParameterTypes,
                                     ConstConfigOptionProxy>)
          CPP_and(...&& ql::concepts::invocable<
                  TranslationFunction, const ExceptionValidatorParameterTypes>)
              CPP_and ExceptionValidatorFunction<
                  ExceptionValidatorFunc,
                  std::invoke_result_t<
                      TranslationFunction,
                      const ExceptionValidatorParameterTypes>...>)
      ConfigOptionValidatorManager(
          ExceptionValidatorFunc exceptionValidatorFunction,
          std::string descriptor, TranslationFunction translationFunction,
          const ExceptionValidatorParameterTypes... configOptionsToBeChecked)
      : descriptor_{std::move(descriptor)},
        configOptionsToBeChecked_{
            &configOptionsToBeChecked.getConfigOption()...} {
    wrappedValidatorFunction_ = [translationFunction =
                                     std::move(translationFunction),
                                 exceptionValidatorFunction =
                                     std::move(exceptionValidatorFunction),
                                 configOptionsToBeChecked...]() {
      if (const auto errorMessage = std::invoke(
              exceptionValidatorFunction,
              std::invoke(translationFunction, configOptionsToBeChecked)...);
          errorMessage.has_value()) {
        // Create the error message prefix.
        auto generateConfigOptionIdentifierWithValueString =
            [](const ConfigOption& c) {
              // Is there a value, we can show together with the identifier?
              const bool wasSet = c.wasSet();
              return absl::StrCat(
                  "'", c.getIdentifier(), "' (With ",
                  wasSet ? absl::StrCat("value '", c.getValueAsString(), "'")
                         : "no value",
                  ".)");
            };
        const std::array optionNamesWithValues{
            generateConfigOptionIdentifierWithValueString(
                configOptionsToBeChecked.getConfigOption())...};
        std::string errorMessagePrefix =
            absl::StrCat("Validity check of configuration options ",
                         lazyStrJoin(optionNamesWithValues, ", "), " failed: ");

        throw std::runtime_error(absl::StrCat(
            errorMessagePrefix, errorMessage.value().getMessage()));
      }
    };
  }

  /*
  @brief Create a `ConfigOptionValidatorManager`, which will call the given
  validator function with the given `ConfigOption`s, after translating
  them via the translator function.

  @param validatorFunction Checks, if the transformed configuration options are
  valid. Should return true, if they are valid.
  @param errorMessage The error message, that will be shown, when the validator
  returns false.
  @param descriptor A description of the invariant, that validator function
  imposes.
  @param configOptionsToBeChecked Proxies for the configuration options, who
  will be passed to the exception validator function as function arguments,
  after being transformed. Will keep the same order.
  */
  CPP_template(typename TranslationFunction, typename ValidatorFunc,
               typename... ValidatorParameterTypes)(
      requires(...&& isInstantiation<ValidatorParameterTypes,
                                     ConstConfigOptionProxy>)
          CPP_and(... &&
                  (ql::concepts::invocable<TranslationFunction,
                                           const ValidatorParameterTypes>))
              CPP_and(ValidatorFunction<
                      ValidatorFunc,
                      std::invoke_result_t<TranslationFunction,
                                           const ValidatorParameterTypes>...>)
                  CPP_and(sizeof...(ValidatorParameterTypes) > 0))
      ConfigOptionValidatorManager(
          ValidatorFunc validatorFunction, std::string errorMessage,
          std::string descriptor, TranslationFunction translationFunction,
          const ValidatorParameterTypes... configOptionsToBeChecked)
      : ConfigOptionValidatorManager(
            transformValidatorIntoExceptionValidator<
                ValidatorFunc,
                std::invoke_result_t<TranslationFunction,
                                     const ValidatorParameterTypes>...>(
                std::move(validatorFunction), std::move(errorMessage)),
            std::move(descriptor), std::move(translationFunction),
            configOptionsToBeChecked...) {}

  /*
  @brief Call the saved validator function with the references given at
  construction and throws an exception, if the validator returns a value, that
  signals, that the given arguments weren't valid.
  */
  void checkValidator() const;

  /*
  @brief Return a user written description of the invariant, that the internally
  saved validator function, imposes upon the configuration options.
  */
  std::string_view getDescription() const;

  // Return, how many instances of this class were initialized before this
  // instance.
  size_t getInitializationId() const;

  // The `configOption`s, that this validator will check.
  const ad_utility::HashSet<const ConfigOption*>& configOptionToBeChecked()
      const;
};

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_CONFIGMANAGER_VALIDATOR_H
