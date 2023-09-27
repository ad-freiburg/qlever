// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (September of 2023,
// schlegea@informatik.uni-freiburg.de)

#pragma once

#include <concepts>
#include <functional>
#include <optional>
#include <stdexcept>
#include <string>

#include "util/ConfigManager/ConfigOptionProxy.h"

namespace ad_utility {
/*
A validator function is any invocable object, who takes the given parameter
types, in the same order and transformed to `const type&`, and returns a
bool.
*/
template <typename Func, typename... ParameterTypes>
concept ValidatorFunction =
    std::regular_invocable<Func, const ParameterTypes&...> &&
    std::same_as<std::invoke_result_t<Func, const ParameterTypes&...>, bool>;

// Simple struct, that holds an error message. For use as the return type of
// invocable object, that fullfill `ExceptionValidator`.
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
concept ExceptionValidatorFunction =
    std::regular_invocable<Func, const ParameterTypes&...> &&
    std::same_as<std::invoke_result_t<Func, const ParameterTypes&...>,
                 std::optional<ErrorMessage>>;

/*
@brief Transform a validator function into an exception validator function.
That is, instead of returning `bool`, it now returns
`std::optional<ErrorMessage>`.

@param errorMessage Content for `ErrorMessage`.
*/
template <typename... ValidatorParameterTypes>
inline auto transformValidatorIntoExceptionValidator(
    ValidatorFunction<ValidatorParameterTypes...> auto validatorFunction,
    std::string errorMessage) {
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
  @param configOptionsToBeChecked Proxies for the configuration options, who
  will be passed to the exception validator function as function arguments,
  after being transformed. Will keep the same order.
  */
  template <typename TranslationFunction, typename ExceptionValidatorFunc,
            isInstantiation<
                ConstConfigOptionProxy>... ExceptionValidatorParameterTypes>
  requires(std::invocable<TranslationFunction,
                          const ExceptionValidatorParameterTypes> &&
           ...) &&
          ExceptionValidatorFunction<
              ExceptionValidatorFunc,
              std::invoke_result_t<TranslationFunction,
                                   ExceptionValidatorParameterTypes>...> &&
          (sizeof...(ExceptionValidatorParameterTypes) > 0)
  ConfigOptionValidatorManager(
      ExceptionValidatorFunc exceptionValidatorFunction,
      TranslationFunction translationFunction,
      const ExceptionValidatorParameterTypes... configOptionsToBeChecked) {
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
  @param configOptionsToBeChecked Proxies for the configuration options, who
  will be passed to the exception validator function as function arguments,
  after being transformed. Will keep the same order.
  */
  template <typename TranslationFunction, typename ValidatorFunc,
            isInstantiation<ConstConfigOptionProxy>... ValidatorParameterTypes>
  requires(std::invocable<TranslationFunction, const ValidatorParameterTypes> &&
           ...) &&
          ValidatorFunction<ValidatorFunc,
                            std::invoke_result_t<TranslationFunction,
                                                 ValidatorParameterTypes>...> &&
          (sizeof...(ValidatorParameterTypes) > 0) ConfigOptionValidatorManager(
      ValidatorFunc validatorFunction, std::string errorMessage,
      TranslationFunction translationFunction,
      const ValidatorParameterTypes... configOptionsToBeChecked)
      : ConfigOptionValidatorManager(
            transformValidatorIntoExceptionValidator<std::invoke_result_t<
                TranslationFunction, ValidatorParameterTypes>...>(
                std::move(validatorFunction), std::move(errorMessage)),
            std::move(translationFunction), configOptionsToBeChecked...) {}

  /*
  @brief Call the saved validator function with the references given at
  construction and throws an exception, if the validator returns a value, that
  signals, that the given arguments weren't valid.
  */
  void checkValidator() const;
};

}  // namespace ad_utility
