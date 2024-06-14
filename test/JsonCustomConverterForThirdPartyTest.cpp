// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (April of 2023,
// schlegea@informatik.uni-freiburg.de)

#include <gtest/gtest.h>

#include <memory>
#include <optional>
#include <variant>

#include "util/json.h"

/*
These are tests for all the custom converter for third party classes in
`json.h`.
*/

// `std::optional`
TEST(JsonCustomConverterForThirdParty, StdOptional) {
  nlohmann::json j;

  // `std::optional` without a value.
  j = std::optional<int>{};
  ASSERT_TRUE(j.is_null());
  std::optional<int> testOptional = j.get<std::optional<int>>();
  ASSERT_FALSE(testOptional.has_value());

  // `std::optional` with a value.
  j = std::optional<int>{42};
  ASSERT_TRUE(j.is_number_integer());
  testOptional = j.get<std::optional<int>>();
  ASSERT_TRUE(testOptional.has_value());
  ASSERT_EQ(testOptional.value(), 42);
}

// `std::monostate` from `std::variant`.
TEST(JsonCustomConverterForThirdParty, StdMonostate) {
  nlohmann::json j;

  // Does it serialize?
  j = std::monostate{};
  ASSERT_TRUE(j.is_null());

  // Does it 'deserialize'? (`std::monostate` is just an empty placeholder,
  // so even the correct serialization doesn't do anything.)
  std::monostate empty = j.get<std::monostate>();

  // The deserializer of `std::monostate` has a custom exception, for when
  // somebody tries to interpret anything but `null` as an `std::monostate`,
  // because that can only be an error.
  j = 3;
  ASSERT_THROW(empty = j.get<std::monostate>(), nlohmann::detail::type_error);
}

// `std::variant`.
TEST(JsonCustomConverterForThirdParty, StdVariant) {
  nlohmann::json j;

  using VariantType = std::variant<std::monostate, int, float>;
  VariantType variant;

  // Helper function, that translates the given type `T` to its index number in
  // the given variant.
  // Most of this code is from: `https://stackoverflow.com/a/52305530`
  constexpr auto typeToVariantIndex =
      []<typename T, typename... Ts>(std::variant<Ts...>) {
        size_t i = 0;
        ((!std::is_same_v<T, Ts> && ++i) && ...);
        return i;
      };

  /*
  @brief Quick check, if the the given json object has the values for `index`
  and `value`, that are wanted.
  A `std::variant` gets saved as a json object with those two fields, so this
  is just a plain json check.
  */
  auto checkJson = [&j, &variant, &typeToVariantIndex]<typename ValueType>(
                       const ValueType& wantedValue) {
    ASSERT_EQ(typeToVariantIndex.template operator()<ValueType>(variant),
              j["index"].get<size_t>());
    ASSERT_EQ(wantedValue, j["value"].get<ValueType>());
  };

  /*
  @brief Sets `variant` to value, serializes `variant`, sets `variant` to an
  indermediate value and finally deserializes it. While doing this, it also
  checks, if the serialized and deserialized versions of variant are correct.

  @param newValue The value, that `variant` will be set to.
  @param intermediateValue The value, that `variant` will be set to before
  deserialization. Needs to be different from newValue, so that we can make
  sure, that the variant is actually changed by the deserialization.
  */
  auto doSimpleTest =
      [&j, &variant, &checkJson, &typeToVariantIndex]<typename NewValueType>(
          const NewValueType& newValue, const auto& intermediateValue) {
        // Serialize `variant` after setting it to the new value.
        variant = newValue;
        j = variant;

        // Was it serialized, as it should?
        checkJson(newValue);

        /*
        Set `variant` to a intermediate value, before checking, if it
        deserializes correctly.
        */
        variant = intermediateValue;
        variant = j.get<VariantType>();

        ASSERT_EQ(typeToVariantIndex.template operator()<NewValueType>(variant),
                  variant.index());
        ASSERT_EQ(newValue, std::get<NewValueType>(variant));
      };

  // Do simple tests for monostate, float and int.
  doSimpleTest((int)42, (float)6.5);
  doSimpleTest((float)13.702, (int)10);
  doSimpleTest(std::monostate{}, (int)42);
  doSimpleTest((float)4.2777422, std::monostate{});

  // There is a custom exception, should the index for a value type be not
  // valid. That is, to big, or to small.
  j["index"] = -1;
  ASSERT_THROW(variant = j.get<VariantType>(), nlohmann::detail::out_of_range);
  j["index"] = std::variant_size_v<VariantType>;
  ASSERT_THROW(variant = j.get<VariantType>(), nlohmann::detail::out_of_range);
}

// `std::unique_ptr<T>` for copy constructible `T`.
TEST(JsonCustomConverterForThirdParty,
     StdUnique_PtrForCopyConstructibleObjects) {
  nlohmann::json j;
  using PointerObjectType = int;
  using PointerType = std::unique_ptr<PointerObjectType>;
  PointerType pointer{};

  /*
  @brief Set, serialize and deserialize `pointer`.

  @param newValue The value, that pointer will be set to and which it
  should have after deserialization.
  @param intermediateValue Between serialization and deserialization `pointer`
  should be set to a different value, so that the json converter can't get
  away with doing nothing.
  */
  auto doCheckPreparation = [&j, &pointer](auto&& newValue,
                                           auto&& intermediateValue) {
    // Set and serialize.
    pointer = std::move(newValue);
    j = pointer;

    // Give the pointer a different object to manage, so that the converter
    // can't get away with doing nothing.
    pointer = std::move(intermediateValue);

    // Deserialize.
    pointer = j.get<PointerType>();
  };

  // Unique pointer doesn't own an object.
  doCheckPreparation(nullptr, std::make_unique<PointerObjectType>(42));
  ASSERT_EQ(pointer.get(), nullptr);

  // Unique pointer owns an object.
  doCheckPreparation(std::make_unique<int>(42), nullptr);
  ASSERT_NE(pointer.get(), nullptr);
  ASSERT_EQ(*pointer, 42);
}
