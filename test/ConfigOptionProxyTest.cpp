// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (August of 2023, schlegea@informatik.uni-freiburg.de)
//
// Copyright 2025, Bayerische Motoren Werke Aktiengesellschaft (BMW AG)

#include <gtest/gtest.h>

#include <concepts>

#include "../test/util/ConfigOptionHelpers.h"
#include "../test/util/GTestHelpers.h"
#include "backports/type_traits.h"
#include "util/ConfigManager/ConfigOption.h"
#include "util/ConfigManager/ConfigOptionProxy.h"
#include "util/TypeTraits.h"

namespace ad_utility {

// Test construction for a given type.
template <template <typename> typename ProxyType, typename OptionType>
struct DoTest {
  template <typename T>
  struct TestWrongType {
    OptionType& opt;

    template <typename WrongT>
    void operator()() const {
      if constexpr (!std::is_same_v<T, WrongT>) {
        ASSERT_ANY_THROW(ProxyType<WrongT> someProxy(opt));
      }
    }
  };

  template <typename T>
  void operator()() const {
    // Does the normal constructor work?
    T varForConfigOption;
    auto opt = OptionType("testOption", "", &varForConfigOption);
    ASSERT_EQ(&opt, &ProxyType<T>(opt).getConfigOption());
    static_assert(std::same_as<T, typename ProxyType<T>::value_type>);

    // Is there an exception, if we give a config option of the wrong type?
    doForTypeInConfigOptionValueType(TestWrongType<T>{opt});
  }
};

/*
@brief Test, if the constructor for `ConfigOption` works as wanted.

@tparam ProxyType The type of the proxy class.
@tparam OptionType Exists to define, if the test should be done with
`ConfigOption`, or `const ConfigOption`.
*/
CPP_template(template <typename> typename ProxyType, typename OptionType)(
    requires SameAsAny<OptionType, ConfigOption,
                       const ConfigOption>) void basicConstructorTest() {
  // Do the test for all possible types.
  doForTypeInConfigOptionValueType(DoTest<ProxyType, OptionType>{});
}

TEST(ConfigOptionProxy, NonConstConfigOptionProxyConstructor) {
  basicConstructorTest<ConfigOptionProxy, ConfigOption>();
}

struct ConstructNonConstConfigOptionProxy {
  template <typename T>
  void operator()() const {
    T varForConfigOption;
    ConfigOption opt("testOption", "", &varForConfigOption);
    ConfigOptionProxy<T> nonConstProxy(opt);
    ConstConfigOptionProxy<T> constProxy(nonConstProxy);
    ASSERT_EQ(&nonConstProxy.getConfigOption(), &constProxy.getConfigOption());
  }
};

TEST(ConfigOptionProxy, ConstConfigOptionProxyConstructor) {
  basicConstructorTest<ConstConfigOptionProxy, ConfigOption>();

  // Does it work with const `configOption`?
  basicConstructorTest<ConstConfigOptionProxy, const ConfigOption>();

  // Does construction with a `NonConstConfigOptionProxy` work?
  doForTypeInConfigOptionValueType(ConstructNonConstConfigOptionProxy{});
}

struct CheckConfigOptionConstness {
  template <typename T>
  void operator()() const {
    T varForConfigOption;
    ConfigOption opt("testOption", "", &varForConfigOption);
    ConfigOptionProxy<T> nonConstProxy(opt);
    ConstConfigOptionProxy<T> constProxy(opt);
    static_assert(!std::is_const_v<std::remove_reference_t<
                      decltype(nonConstProxy.getConfigOption())>>);
    static_assert(
        std::is_const_v<
            std::remove_reference_t<decltype(constProxy.getConfigOption())>>);
  }
};

TEST(ConfigOptionProxy, GetConfigOption) {
  // Simple test, if the returned `ConfigOption` are(n't) const.
  doForTypeInConfigOptionValueType(CheckConfigOptionConstness{});
}

template <typename F>
struct CheckConfigOptionConversion {
  F& identity;

  template <typename T>
  void operator()() const {
    T varForConfigOption;
    ConfigOption opt("testOption", "", &varForConfigOption);
    ConfigOptionProxy<T> nonConstProxy(opt);
    ConstConfigOptionProxy<T> constProxy(opt);

    // Does the explicit conversion work and return the same object, as the
    // one given?
    ASSERT_EQ(&opt, &static_cast<ConfigOption&>(nonConstProxy));
    ASSERT_EQ(&opt, &static_cast<const ConfigOption&>(nonConstProxy));
    ASSERT_EQ(&opt, &static_cast<const ConfigOption&>(constProxy));

    // Does the implicit conversion work and return the same object, as the
    // one given?
    ASSERT_EQ(&opt,
              &identity.template operator()<ConfigOption&>(nonConstProxy));
    ASSERT_EQ(&opt, &identity.template operator()<const ConfigOption&>(
                        nonConstProxy));
    ASSERT_EQ(&opt,
              &identity.template operator()<const ConfigOption&>(constProxy));

    // Does the cast to `ConfigOption` create a different instance?
    const ConfigOption& opt2 = static_cast<ConfigOption>(nonConstProxy);
    ASSERT_NE(&opt, &opt2);
    const ConfigOption& opt3 = static_cast<ConfigOption>(constProxy);
    ASSERT_NE(&opt, &opt3);
  }
};

TEST(ConfigOptionProxy, ConversionToConfigOption) {
  // A function variant of `ql::identity`, where you can give the type.
  auto identity = [](auto t) -> decltype(t) { return t; };

  doForTypeInConfigOptionValueType(
      CheckConfigOptionConversion<decltype(identity)>{identity});
}

}  // namespace ad_utility
