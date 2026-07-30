//  Copyright 2026 The QLever Authors, in particular:
//
//  2026 Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>, UFR
//
//  UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
#include "util/http/HttpProxyConfig.h"

#include <absl/strings/ascii.h>
#include <absl/strings/escaping.h>
#include <absl/strings/match.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_split.h>

#include <boost/asio/ip/address.hpp>
#include <boost/url/parse.hpp>
#include <boost/url/url_view.hpp>
#include <cstdlib>
#include <stdexcept>

#include "backports/algorithm.h"
#include "util/Exception.h"

namespace ad_utility::httpProxy {

namespace {

// Read an environment variable, returning `std::nullopt` if it is not set. Note
// that we deliberately treat a variable that is set but empty as "set to the
// empty string", which `parseProxyUrl` then maps to "no proxy". This makes
// `https_proxy=` a working way to disable a proxy inherited from a parent
// process, as it is with other tools.
std::optional<std::string_view> getEnv(const char* name) {
  const char* value = std::getenv(name);
  if (value == nullptr) {
    return std::nullopt;
  }
  return std::string_view{value};
}

// Return the value of the first of the given environment variables that is set,
// or `std::nullopt` if none of them is. Note that "set to the empty string" and
// "not set" must be kept apart here, because only the latter may fall back to
// `all_proxy`, see `ProxyConfiguration::fromEnvironment`.
std::optional<std::string_view> firstEnvThatIsSet(
    std::initializer_list<const char*> variableNames) {
  for (const char* name : variableNames) {
    if (auto value = getEnv(name); value.has_value()) {
      return value;
    }
  }
  return std::nullopt;
}

// Strip an optional `:port` suffix from a `no_proxy` list entry. We only strip
// what is unambiguously a port, so that an unbracketed IPv6 address (which
// contains multiple colons) is left alone rather than mangled.
std::string_view stripPortFromNoProxyEntry(std::string_view entry) {
  size_t colon = entry.rfind(':');
  if (colon == std::string_view::npos || entry.find(':') != colon) {
    return entry;
  }
  std::string_view port = entry.substr(colon + 1);
  bool isPort = !port.empty() && ql::ranges::all_of(port, absl::ascii_isdigit);
  return isPort ? entry.substr(0, colon) : entry;
}

}  // namespace

// ____________________________________________________________________________
std::optional<Proxy> parseProxyUrl(std::string_view proxyUrl) {
  std::string_view trimmed = absl::StripAsciiWhitespace(proxyUrl);
  if (trimmed.empty()) {
    return std::nullopt;
  }

  // Both `host:port` and `http://host:port` are accepted, as with other tools.
  // Boost.URL requires a scheme, so add the implied one. We must not do this
  // when a scheme is already present, because we want to report an unsupported
  // scheme (e.g. `socks5://`) as such.
  std::string withScheme =
      absl::StrContains(trimmed, "://") ? std::string{trimmed}
                                        : absl::StrCat("http://", trimmed);
  boost::system::result<boost::urls::url_view> parsed =
      boost::urls::parse_uri(withScheme);
  if (parsed.has_error()) {
    throw std::runtime_error(absl::StrCat(
        "The proxy URL \"", proxyUrl,
        "\" could not be parsed: ", parsed.error().message(),
        ". Expected a URL of the form [http://][user[:password]@]host[:port]"));
  }
  const boost::urls::url_view& url = parsed.value();

  if (url.scheme() != "http") {
    throw std::runtime_error(absl::StrCat(
        "The proxy URL \"", proxyUrl, "\" uses the scheme \"",
        std::string_view{url.scheme()},
        "\", but QLever only supports plain HTTP proxies (scheme \"http\"). "
        "Note that a plain HTTP proxy can still relay HTTPS requests; it is "
        "only the connection to the proxy itself that must not use TLS."));
  }
  if (url.host().empty()) {
    throw std::runtime_error(absl::StrCat("The proxy URL \"", proxyUrl,
                                          "\" does not specify a host"));
  }
  // A proxy URL is an authority, not a resource; a path would silently be
  // ignored, so reject it instead of pretending to honor it.
  if (!url.path().empty() && url.path() != "/") {
    throw std::runtime_error(absl::StrCat(
        "The proxy URL \"", proxyUrl, "\" must not have a path, but has \"",
        url.path(), "\""));
  }

  Proxy proxy;
  proxy.host_ = url.host();
  // Note the two-step conversion: `url.port()` is a `boost::core::string_view`,
  // which converts to `std::string_view` but not directly to `std::string`.
  proxy.port_ =
      url.has_port() ? std::string{std::string_view{url.port()}} : "80";
  if (url.has_userinfo()) {
    proxy.authorization_ = absl::StrCat(
        "Basic ",
        absl::Base64Escape(absl::StrCat(url.user(), ":", url.password())));
  }
  return proxy;
}

// ____________________________________________________________________________
bool isExcludedByNoProxy(std::string_view host, std::string_view noProxy) {
  std::string lowerHost = absl::AsciiStrToLower(host);
  for (std::string_view rawEntry : absl::StrSplit(noProxy, ',')) {
    std::string_view entry = absl::StripAsciiWhitespace(rawEntry);
    if (entry.empty()) {
      continue;
    }
    if (entry == "*") {
      return true;
    }
    entry = stripPortFromNoProxyEntry(entry);
    // A leading dot is conventional but carries no meaning: both `.example.org`
    // and `example.org` match the domain and all of its subdomains.
    while (absl::StartsWith(entry, ".")) {
      entry.remove_prefix(1);
    }
    if (entry.empty()) {
      continue;
    }
    std::string lowerEntry = absl::AsciiStrToLower(entry);
    if (lowerHost == lowerEntry ||
        absl::EndsWith(lowerHost, absl::StrCat(".", lowerEntry))) {
      return true;
    }
  }
  return false;
}

// ____________________________________________________________________________
bool isLoopbackHost(std::string_view host) {
  std::string lowerHost = absl::AsciiStrToLower(host);
  if (lowerHost == "localhost" || absl::EndsWith(lowerHost, ".localhost")) {
    return true;
  }
  // An IPv6 address may appear in brackets, which `make_address` does not
  // accept.
  std::string_view address = lowerHost;
  if (absl::StartsWith(address, "[") && absl::EndsWith(address, "]")) {
    address = address.substr(1, address.size() - 2);
  }
  boost::system::error_code ec;
  auto parsedAddress = boost::asio::ip::make_address(address, ec);
  // A host name that is not an IP address at all is not loopback (we
  // deliberately do not resolve it, which would be a DNS lookup per request).
  return !ec && parsedAddress.is_loopback();
}

// ____________________________________________________________________________
ProxyConfiguration::ProxyConfiguration(std::string_view httpProxy,
                                       std::string_view httpsProxy,
                                       std::string_view noProxy)
    : httpProxy_{parseProxyUrl(httpProxy)},
      httpsProxy_{parseProxyUrl(httpsProxy)},
      noProxy_{noProxy} {}

// ____________________________________________________________________________
ProxyConfiguration ProxyConfiguration::fromEnvironment() {
  auto allProxy = firstEnvThatIsSet({"all_proxy", "ALL_PROXY"});
  // `all_proxy` only applies if no scheme-specific variable is set at all. In
  // particular, `https_proxy=` (set but empty) means "no proxy for HTTPS" and
  // must not fall back to `all_proxy`.
  auto orElseAllProxy = [&allProxy](std::optional<std::string_view> value) {
    return value.has_value() ? value.value()
                             : allProxy.value_or(std::string_view{});
  };
  return ProxyConfiguration{
      orElseAllProxy(firstEnvThatIsSet({"http_proxy"})),
      orElseAllProxy(firstEnvThatIsSet({"https_proxy", "HTTPS_PROXY"})),
      firstEnvThatIsSet({"no_proxy", "NO_PROXY"})
          .value_or(std::string_view{})};
}

// ____________________________________________________________________________
std::optional<Proxy> ProxyConfiguration::proxyFor(std::string_view scheme,
                                                  std::string_view host) const {
  AD_CONTRACT_CHECK(scheme == "http" || scheme == "https");
  const std::optional<Proxy>& proxy =
      scheme == "http" ? httpProxy_ : httpsProxy_;
  if (!proxy.has_value() || isLoopbackHost(host) ||
      isExcludedByNoProxy(host, noProxy_)) {
    return std::nullopt;
  }
  return proxy;
}

// ____________________________________________________________________________
std::string ProxyConfiguration::asStringForLogging() const {
  if (empty()) {
    return "none";
  }
  // Deliberately without `authorization_`, which holds the proxy credentials.
  auto describe = [](const std::optional<Proxy>& proxy) -> std::string {
    if (!proxy.has_value()) {
      return "none";
    }
    return absl::StrCat(proxy->host_, ":", proxy->port_,
                        proxy->authorization_.empty() ? "" : " (with auth)");
  };
  return absl::StrCat("HTTP -> ", describe(httpProxy_), ", HTTPS -> ",
                      describe(httpsProxy_), ", excluded hosts: ",
                      noProxy_.empty() ? "none (except loopback)" : noProxy_);
}

// ____________________________________________________________________________
const ProxyConfiguration& globalProxyConfiguration() {
  static const ProxyConfiguration configuration =
      ProxyConfiguration::fromEnvironment();
  return configuration;
}

}  // namespace ad_utility::httpProxy
#endif
