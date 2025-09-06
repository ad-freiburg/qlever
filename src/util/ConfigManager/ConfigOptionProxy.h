// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (March of 2023, schlegea@informatik.uni-freiburg.de)

#ifndef QLEVER_SRC_UTIL_CONFIGMANAGER_CONFIGOPTIONPROXY_H
#define QLEVER_SRC_UTIL_CONFIGMANAGER_CONFIGOPTIONPROXY_H

#include <absl/strings/str_cat.h>

#include <concepts>
#include <stdexcept>

#include "backports/type_traits.h"
#include "util/ConfigManager/ConfigOption.h"
#include "util/Exception.h"
#include "util/TypeTraits.h"

namespace ad_utility {
namespace ConfigManagerImpl {
/*
@brief Implementation of proxy/reference to a `ConfigOption`. Also saves the
type of value, that the configuration option holds. Needed, because without a
`ConfigOptionProxy` and `ConstConfigOptionProxy`, people could easily discard
the `const` from a `const ConfigOptionProxy` by copying it, to have non const
access to the referenced config option.

@tparam T The type of value, the later given config option will hold.
@tparam ConfigOptionType The kind of config option, this proxy will reference
to. Must be `ConfigOption`, or `const ConfigOption`.
*/
CPP_template(typename T, typename ConfigOptionType)(
    requires SupportedConfigOptionType<T> CPP_and ad_utility::SameAsAny<
        ConfigOptionType, ConfigOption,
        const ConfigOption>) class ConfigOptionProxyImplementation {
  ConfigOptionType* option_;

 public:
  // Custom constructor.
  explicit ConfigOptionProxyImplementation(ConfigOptionType& opt)
      : option_(&opt) {
    // Make sure, that the given `ConfigOption` holds values of the right type.
    AD_CONTRACT_CHECK(opt.template holdsType<T>());
  }

  // Get access to the configuration option, this is a proxy for. Const access
  // only for the const version, to make the usage clearer.
  const ConfigOptionType& getConfigOption() const
      QL_CONCEPT_OR_NOTHING(requires(std::is_const_v<ConfigOptionType>)) {
    AD_CORRECTNESS_CHECK(option_ != nullptr);
    return *option_;
  }

  ConfigOptionType& getConfigOption()
      QL_CONCEPT_OR_NOTHING(requires(!std::is_const_v<ConfigOptionType>)) {
    AD_CORRECTNESS_CHECK(option_ != nullptr);
    return *option_;
  }

  // (Implicit) conversion to `ConfigOptionType&`.
  explicit(false) operator ConfigOptionType&() const
      QL_CONCEPT_OR_NOTHING(requires(std::is_const_v<ConfigOptionType>)) {
    return getConfigOption();
  }

  explicit(false) operator ConfigOptionType&()
      QL_CONCEPT_OR_NOTHING(requires(!std::is_const_v<ConfigOptionType>)) {
    return getConfigOption();
  }
};

}  // namespace ConfigManagerImpl

// A const proxy/reference to a `ConfigOption`. Also saves the type of
// value, that the configuration option holds.
template <typename T>
class ConstConfigOptionProxy
    : public ConfigManagerImpl::ConfigOptionProxyImplementation<
          T, const ConfigOption> {
  using Base =
      ConfigManagerImpl::ConfigOptionProxyImplementation<T, const ConfigOption>;

 public:
  using value_type = T;

  // Construct proxy for the given option.
  explicit ConstConfigOptionProxy(const ConfigOption& opt) : Base(opt) {}
};

// A non const proxy/reference to a `ConfigOption`. Also saves the type of
// value, that the configuration option holds.
template <typename T>
class ConfigOptionProxy
    : public ConfigManagerImpl::ConfigOptionProxyImplementation<T,
                                                                ConfigOption> {
  using Base =
      ConfigManagerImpl::ConfigOptionProxyImplementation<T, ConfigOption>;

 public:
  using value_type = T;

  // Construct proxy for the given option.
  explicit ConfigOptionProxy(ConfigOption& opt) : Base(opt) {}

  // Implicit conversion from not const to const is allowed.
  explicit(false) operator ConstConfigOptionProxy<T>() {
    return ConstConfigOptionProxy<T>(Base::getConfigOption());
  }
};

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_CONFIGMANAGER_CONFIGOPTIONPROXY_H
