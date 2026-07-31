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

#include <boost/url/parse.hpp>
#include <boost/url/url_view.hpp>
#include <cstdlib>
#include <stdexcept>

#include "util/Exception.h"

namespace ad_utility::httpProxy {

// ____________________________________________________________________________
std::string Proxy::asStringForLogging() const {
  return absl::StrCat(host_, ":", port_,
                      authorization_.empty() ? "" : " (with authentication)");
}

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
  std::string withScheme = absl::StrContains(trimmed, "://")
                               ? std::string{trimmed}
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
  // A proxy URL is an authority, not a resource; a path, query or fragment
  // would silently be ignored, so reject it instead of pretending to honor it.
  if (!url.path().empty() && url.path() != "/") {
    throw std::runtime_error(absl::StrCat("The proxy URL \"", proxyUrl,
                                          "\" must not have a path, but has \"",
                                          url.path(), "\""));
  }
  if (url.has_query() || url.has_fragment()) {
    throw std::runtime_error(absl::StrCat(
        "The proxy URL \"", proxyUrl, "\" must not have a query or fragment"));
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
std::string absoluteFormTarget(std::string_view host, std::string_view port,
                               std::string_view target) {
  AD_CONTRACT_CHECK(absl::StartsWith(target, "/"),
                    "The request target must be in origin form");
  std::string portSuffix = port == "80" ? "" : absl::StrCat(":", port);
  return absl::StrCat("http://", host, portSuffix, target);
}

// ____________________________________________________________________________
std::optional<Proxy> proxyFromEnvironment() {
  const char* value = std::getenv("http_proxy");
  // Note that `parseProxyUrl` maps the empty string to "no proxy", so a
  // variable that is set but empty needs no special handling here.
  return parseProxyUrl(value == nullptr ? std::string_view{}
                                        : std::string_view{value});
}

// ____________________________________________________________________________
const std::optional<Proxy>& globalProxy() {
  static const std::optional<Proxy> proxy = proxyFromEnvironment();
  return proxy;
}

}  // namespace ad_utility::httpProxy
#endif
