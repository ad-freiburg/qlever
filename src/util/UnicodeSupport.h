// Copyright 2025, Bayerische Motoren Werke Aktiengesellschaft (BMW AG)

#ifndef QLEVER_SRC_UTIL_UNICODESUPPORT_H
#define QLEVER_SRC_UTIL_UNICODESUPPORT_H

namespace ad_utility {
// Whether QLever was built with ICU (proper Unicode) support. This is the
// default value for the `useICU` template parameter of the various string
// functions that have both an ICU-based and an ICU-free implementation (for
// example `ad_utility::utf8ToLower`). It is `false` exactly if the `NO_UNICODE`
// CMake option was set (which defines the `QLEVER_NO_UNICODE` macro). That way
// the ICU-free implementations are selected automatically in an ICU-free build,
// while both implementations remain instantiable (and thus testable) in a
// regular build.
#ifdef QLEVER_NO_UNICODE
inline constexpr bool useICUDefault = false;
#else
inline constexpr bool useICUDefault = true;
#endif
}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_UNICODESUPPORT_H
