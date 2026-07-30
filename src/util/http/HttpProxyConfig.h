//  Copyright 2026 The QLever Authors, in particular:
//
//  2026 Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>, UFR
//
//  UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
#ifndef QLEVER_SRC_UTIL_HTTP_HTTPPROXYCONFIG_H
#define QLEVER_SRC_UTIL_HTTP_HTTPPROXYCONFIG_H

#include <optional>
#include <string>
#include <string_view>

#include "backports/three_way_comparison.h"

// Support for routing QLever's outgoing HTTP requests (`SERVICE` and `LOAD`)
// through an HTTP proxy, configured via the de-facto standard environment
// variables `http_proxy`, `https_proxy`, `all_proxy` and `no_proxy`. See
// `ProxyConfiguration::fromEnvironment` for the exact rules.
namespace ad_utility::httpProxy {

// An HTTP proxy that a single connection should be routed through.
struct Proxy {
  // Host and port of the proxy itself (not of the request's target).
  std::string host_;
  std::string port_;
  // The value for the `Proxy-Authorization` header, or empty if the proxy
  // requires no authentication.
  std::string authorization_;

  QL_DEFINE_DEFAULTED_EQUALITY_OPERATOR_LOCAL(Proxy, host_, port_,
                                              authorization_)
};

// Parse a proxy URL of the form `[http://][user[:password]@]host[:port]`. The
// port defaults to 80 (unlike `curl`, which defaults to 1080; QLever only
// supports HTTP proxies, for which 80 is the natural default). Percent-encoded
// credentials are decoded, and turned into `Proxy::authorization_` as a `Basic`
// credential.
//
// Returns `std::nullopt` if `proxyUrl` is empty or consists only of
// whitespace, which is the conventional way of saying "use no proxy".
//
// Throws `std::runtime_error` if the URL is malformed, has no host, or names a
// scheme other than `http`. In particular, `https://` (TLS to the proxy
// itself) and `socks5://` are rejected explicitly, rather than silently
// treated as a plain HTTP proxy.
std::optional<Proxy> parseProxyUrl(std::string_view proxyUrl);

// Return true if `host` is matched by `noProxy`, which is a comma-separated
// list of host names in the format of the `no_proxy` environment variable.
// Matching is case-insensitive and ignores a leading dot on list entries. An
// entry matches if it equals `host`, or if `host` ends with `.` followed by the
// entry (so `example.org` matches `sparql.example.org`, but not
// `notexample.org`). The single entry `*` matches every host.
//
// Note: an optional `:port` suffix on a list entry is stripped and ignored, so
// `example.org:80` excludes `example.org` on all ports. CIDR notation is not
// supported; IP addresses only match literally.
bool isExcludedByNoProxy(std::string_view host, std::string_view noProxy);

// Return true if `host` names the loopback interface, i.e. it is `localhost`,
// a subdomain of `localhost`, or a loopback IP address (`127.0.0.0/8` or
// `::1`, optionally in brackets). Such hosts always bypass the proxy, see
// `ProxyConfiguration::proxyFor`.
bool isLoopbackHost(std::string_view host);

// The proxy settings for this process. Immutable after construction; obtain the
// process-wide instance via `globalProxyConfiguration()` below.
class ProxyConfiguration {
 private:
  std::optional<Proxy> httpProxy_;
  std::optional<Proxy> httpsProxy_;
  std::string noProxy_;

 public:
  // Construct from the raw values of the corresponding environment variables.
  // Primarily for testing; production code uses `fromEnvironment()`. Throws if
  // one of the proxy URLs is malformed, see `parseProxyUrl`.
  ProxyConfiguration(std::string_view httpProxy, std::string_view httpsProxy,
                     std::string_view noProxy);

  // Read the settings from the environment. For HTTPS targets we consult
  // `https_proxy`, `HTTPS_PROXY`, `all_proxy`, `ALL_PROXY` in that order; for
  // HTTP targets `http_proxy`, `all_proxy`, `ALL_PROXY`. The uppercase
  // `HTTP_PROXY` is deliberately *not* honored, following `curl`: in a CGI
  // environment it would be settable by a remote client via the `Proxy:`
  // request header. The exclusion list is taken from `no_proxy`, or else
  // `NO_PROXY`. Throws if a proxy URL is malformed, see `parseProxyUrl`.
  static ProxyConfiguration fromEnvironment();

  // The proxy to use for a request to `host` via `scheme` (which must be
  // `"http"` or `"https"`), or `std::nullopt` to connect directly. Loopback
  // hosts always connect directly, so that a proxy meant for external traffic
  // does not break requests to endpoints on the same machine.
  std::optional<Proxy> proxyFor(std::string_view scheme,
                                std::string_view host) const;

  // A single-line, human-readable summary for the startup log. Never contains
  // the proxy credentials.
  std::string asStringForLogging() const;

  // True if no proxy is configured at all, i.e. `proxyFor` always returns
  // `std::nullopt`.
  bool empty() const {
    return !httpProxy_.has_value() && !httpsProxy_.has_value();
  }
};

// The process-wide proxy configuration, read from the environment on first use.
// Throws if the environment holds a malformed proxy URL; `qlever-server` calls
// this early during startup so that such a misconfiguration is reported before
// the index is loaded, rather than on the first federated query.
const ProxyConfiguration& globalProxyConfiguration();

}  // namespace ad_utility::httpProxy

#endif  // QLEVER_SRC_UTIL_HTTP_HTTPPROXYCONFIG_H
#endif
