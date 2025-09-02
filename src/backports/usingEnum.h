//
// Created by kalmbacj on 9/2/25.
//

#ifndef USINGENUM_H
#define USINGENUM_H

// TODO<BMW>
// 1. Document, including the manual definition with explicit values.
// 2. Document the current limitations (up to 10 entries, can be extended
// manually below).
// 3. apply this for all enums that occur with `using enum`.
// 4. This doesn't work if the enum is declared/defined inside a class. In this
// case it should work
//    to define the enum before the class in a suitable namespace.

// Manual implementation for enums with explicit values
#define QL_DEFINE_ENUM_MANUAL(EnumName, ...) \
  enum class EnumName { __VA_ARGS__ };       \
  namespace ql_using_enum_namespace_##EnumName {

// Add this after QL_DEFINE_ENUM_MANUAL for each enum value
#define QL_ENUM_ALIAS(EnumName, name) \
  static constexpr EnumName name = EnumName::name;

// Close the namespace
#define QL_DEFINE_ENUM_END() }

// Automatic version for simple enum values (identifiers only, no assignments)
#define QL_DEFINE_ENUM(EnumName, ...)             \
  enum class EnumName { __VA_ARGS__ };            \
  namespace ql_using_enum_namespace_##EnumName {  \
    QL_DEFINE_ENUM_ALIASES(EnumName, __VA_ARGS__) \
  }

// Alias generation for simple cases
#define QL_DEFINE_ENUM_ALIASES(EnumName, ...) \
  QL_DEFINE_ENUM_DISPATCH(__VA_ARGS__)(EnumName, __VA_ARGS__)

#define QL_DEFINE_ENUM_DISPATCH(...) \
  QL_DEFINE_ENUM_COUNT(__VA_ARGS__, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1)

#define QL_DEFINE_ENUM_COUNT(_1, _2, _3, _4, _5, _6, _7, _8, _9, _10, N, ...) \
  QL_DEFINE_ENUM_##N

#define QL_DEFINE_ENUM_1(EnumName, a) static constexpr EnumName a = EnumName::a;
#define QL_DEFINE_ENUM_2(EnumName, a, b)     \
  static constexpr EnumName a = EnumName::a; \
  static constexpr EnumName b = EnumName::b;
#define QL_DEFINE_ENUM_3(EnumName, a, b, c)  \
  static constexpr EnumName a = EnumName::a; \
  static constexpr EnumName b = EnumName::b; \
  static constexpr EnumName c = EnumName::c;
#define QL_DEFINE_ENUM_4(EnumName, a, b, c, d) \
  QL_DEFINE_ENUM_2(EnumName, a, b);            \
  QL_DEFINE_ENUM_2(EnumName, c, d);
#define QL_DEFINE_ENUM_5(EnumName, a, b, c, d, e) \
  QL_DEFINE_ENUM_3(EnumName, a, b, c);            \
  QL_DEFINE_ENUM_2(EnumName, d, e);
#define QL_DEFINE_ENUM_6(EnumName, a, b, c, d, e, f) \
  QL_DEFINE_ENUM_3(EnumName, a, b, c);               \
  QL_DEFINE_ENUM_3(EnumName, d, e, f);
#define QL_DEFINE_ENUM_7(EnumName, a, b, c, d, e, f, g) \
  QL_DEFINE_ENUM_4(EnumName, a, b, c, d);               \
  QL_DEFINE_ENUM_3(EnumName, e, f, g);
#define QL_DEFINE_ENUM_8(EnumName, a, b, c, d, e, f, g, h) \
  QL_DEFINE_ENUM_4(EnumName, a, b, c, d);                  \
  QL_DEFINE_ENUM_4(EnumName, e, f, g, h);
#define QL_DEFINE_ENUM_9(EnumName, a, b, c, d, e, f, g, h, i) \
  QL_DEFINE_ENUM_4(EnumName, a, b, c, d);                     \
  QL_DEFINE_ENUM_5(EnumName, e, f, g, h, i);
#define QL_DEFINE_ENUM_10(EnumName, a, b, c, d, e, f, g, h, i, j) \
  QL_DEFINE_ENUM_4(EnumName, a, b, c, d);                         \
  QL_DEFINE_ENUM_6(EnumName, e, f, g, h, i, j);

#define QL_USING_ENUM(EnumName) \
  using namespace ql_using_enum_namespace_##EnumName;

// Replacement for `QL_USING_ENUM` when using an enum for a different namespace.
#define QL_USING_ENUM_NAMESPACE(Namespace, EnumName) \
  using namespace Namespace::ql_using_enum_namespace_##EnumName;

#endif  // USINGENUM_H
