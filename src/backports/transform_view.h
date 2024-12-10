//
// Created by kalmbacj on 12/6/24.
//

#ifndef TRANSFORM_VIEW_H
#define TRANSFORM_VIEW_H
#include <iterator>
#include <type_traits>

namespace ql {
namespace __detail {
template <bool _Const, typename T>
using __maybe_const_t = std::conditional_t<_Const, const T, T>;
}
template <typename Range>
using iterator_t = std::decay_t<decltype(std::begin(std::declval<Range&>()))>;
template <typename Range>
using const_iterator_t =
    std::decay_t<decltype(std::cbegin(std::declval<Range&>()))>;
template <typename Range>
using sentinel_t = std::decay_t<decltype(std::end(std::declval<Range&>()))>;
template <typename Range>
using const_sentinel_t =
    std::decay_t<decltype(std::begin(std::declval<Range&>()))>;

template <typename Range>
using range_reference_t = std::iterator_traits<iterator_t<Range>>::reference;
template <typename Range>
using range_difference_t =
    std::iterator_traits<iterator_t<Range>>::difference_type;

template <typename Range>
constexpr bool forward_range = std::is_base_of_v<
    std::forward_iterator_tag,
    typename std::iterator_traits<iterator_t<Range>>::iterator_category>;
template <typename Range>
constexpr bool random_access_range = std::is_base_of_v<
    std::random_access_iterator_tag,
    typename std::iterator_traits<iterator_t<Range>>::iterator_category>;
template <typename Range>
constexpr bool bidirectional_range = std::is_base_of_v<
    std::bidirectional_iterator_tag,
    typename std::iterator_traits<iterator_t<Range>>::iterator_category>;
template <typename _Vp, typename _Fp>
class transform_view {
 private:
  template <bool _Const>
  using _Base = std::conditional_t<_Const, const _Vp, _Vp>;

  template <bool _Const, typename B = _Base<_Const>,
            typename = std::enable_if_t<forward_range<B>>>
  struct __iter_cat {
   private:
    static auto _S_iter_cat() {
      using _Base = transform_view::_Base<_Const>;
      using _Res = std::invoke_result_t<_Fp&, range_reference_t<_Base>>;
      if constexpr (std::is_lvalue_reference_v<_Res>) {
        using _Cat =
            typename std::iterator_traits<iterator_t<_Base>>::iterator_category;
        if constexpr (std::is_base_of_v<std::contiguous_iterator_tag, _Cat>)
          return std::random_access_iterator_tag{};
        else
          return _Cat{};
      } else
        return std::input_iterator_tag{};
    }

   public:
    using iterator_category = decltype(_S_iter_cat());
  };

  template <bool _Const>
  struct _Sentinel;

  template <bool _Const>
  struct _Iterator : __iter_cat<_Const> {
   private:
    using _Parent = __detail::__maybe_const_t<_Const, transform_view>;
    using _Base = transform_view::_Base<_Const>;

    static auto _S_iter_concept() {
      if constexpr (random_access_range<_Base>)
        return std::random_access_iterator_tag{};
      else if constexpr (bidirectional_range<_Base>)
        return std::bidirectional_iterator_tag{};
      else if constexpr (forward_range<_Base>)
        return std::forward_iterator_tag{};
      else
        return std::input_iterator_tag{};
    }

    using _Base_iter = iterator_t<_Base>;

    _Base_iter _M_current = _Base_iter();
    _Parent* _M_parent = nullptr;

   public:
    using iterator_concept = decltype(_S_iter_concept());
    // iterator_category defined in __transform_view_iter_cat
    using value_type = std::remove_cvref_t<
        std::invoke_result_t<_Fp&, range_reference_t<_Base>>>;
    using difference_type = range_difference_t<_Base>;

    _Iterator() = default;

    constexpr _Iterator(_Parent* __parent, _Base_iter __current)
        : _M_current(std::move(__current)), _M_parent(__parent) {}

    constexpr _Iterator(_Iterator<!_Const> __i)
        : _M_current(std::move(__i._M_current)), _M_parent(__i._M_parent) {}

    constexpr const _Base_iter& base() const& noexcept { return _M_current; }

    constexpr _Base_iter base() && { return std::move(_M_current); }

    constexpr decltype(auto) operator*() const
        noexcept(noexcept(std::__invoke(*_M_parent->_M_fun, *_M_current))) {
      return std::__invoke(*_M_parent->_M_fun, *_M_current);
    }

    constexpr _Iterator& operator++() {
      ++_M_current;
      return *this;
    }

    constexpr void operator++(int) { ++_M_current; }

    constexpr _Iterator operator++(int) requires forward_range<_Base> {
      auto __tmp = *this;
      ++*this;
      return __tmp;
    }

    constexpr _Iterator& operator--() requires bidirectional_range<_Base> {
      --_M_current;
      return *this;
    }

    constexpr _Iterator operator--(int) requires bidirectional_range<_Base> {
      auto __tmp = *this;
      --*this;
      return __tmp;
    }

    constexpr _Iterator& operator+=(difference_type __n)
        requires random_access_range<_Base> {
      _M_current += __n;
      return *this;
    }

    constexpr _Iterator& operator-=(difference_type __n)
        requires random_access_range<_Base> {
      _M_current -= __n;
      return *this;
    }

    constexpr decltype(auto) operator[](difference_type __n) const
    // requires random_access_range<_Base>
    {
      return std::__invoke(*_M_parent->_M_fun, _M_current[__n]);
    }

    friend constexpr bool operator==(const _Iterator& __x, const _Iterator& __y)
    // requires equality_comparable<_Base_iter>
    {
      return __x._M_current == __y._M_current;
    }

    friend constexpr bool operator<(const _Iterator& __x, const _Iterator& __y)
    // requires random_access_range<_Base>
    {
      return __x._M_current < __y._M_current;
    }

    friend constexpr bool operator>(const _Iterator& __x, const _Iterator& __y)
    // requires random_access_range<_Base>
    {
      return __y < __x;
    }

    friend constexpr bool operator<=(const _Iterator& __x, const _Iterator& __y)
    // requires random_access_range<_Base>
    {
      return !(__y < __x);
    }

    friend constexpr bool operator>=(const _Iterator& __x, const _Iterator& __y)
    // requires random_access_range<_Base>
    {
      return !(__x < __y);
    }

    friend constexpr _Iterator operator+(_Iterator __i, difference_type __n)
    // requires random_access_range<_Base>
    {
      return {__i._M_parent, __i._M_current + __n};
    }

    friend constexpr _Iterator operator+(difference_type __n, _Iterator __i)
    // requires random_access_range<_Base>
    {
      return {__i._M_parent, __i._M_current + __n};
    }

    friend constexpr _Iterator operator-(_Iterator __i, difference_type __n)
    // requires random_access_range<_Base>
    {
      return {__i._M_parent, __i._M_current - __n};
    }

    // _GLIBCXX_RESOLVE_LIB_DEFECTS
    // 3483. transform_view::iterator's difference is overconstrained
    friend constexpr difference_type operator-(const _Iterator& __x,
                                               const _Iterator& __y)
    // requires sized_sentinel_for<iterator_t<_Base>, iterator_t<_Base>>
    {
      return __x._M_current - __y._M_current;
    }

    friend constexpr decltype(auto) iter_move(const _Iterator& __i) noexcept(
        noexcept(*__i)) {
      if constexpr (std::is_lvalue_reference_v<decltype(*__i)>)
        return std::move(*__i);
      else
        return *__i;
    }

    friend _Iterator<!_Const>;
    template <bool>
    friend struct _Sentinel;
  };

  template <bool _Const>
  struct _Sentinel {
   private:
    using _Parent = __detail::__maybe_const_t<_Const, transform_view>;
    using _Base = transform_view::_Base<_Const>;

    template <bool _Const2>
    constexpr auto __distance_from(const _Iterator<_Const2>& __i) const {
      return _M_end - __i._M_current;
    }

    template <bool _Const2>
    constexpr bool __equal(const _Iterator<_Const2>& __i) const {
      return __i._M_current == _M_end;
    }

    sentinel_t<_Base> _M_end = sentinel_t<_Base>();

   public:
    _Sentinel() = default;

    constexpr explicit _Sentinel(sentinel_t<_Base> __end) : _M_end(__end) {}

    constexpr _Sentinel(_Sentinel<!_Const> __i)
        /*
      requires _Const
        && convertible_to<sentinel_t<_Vp>, sentinel_t<_Base>>
*/
        : _M_end(std::move(__i._M_end)) {}

    constexpr sentinel_t<_Base> base() const { return _M_end; }

    template <bool _Const2>
    /*
      requires sentinel_for<sentinel_t<_Base>,
                 iterator_t<__detail::__maybe_const_t<_Const2, _Vp>>>
     */
    friend constexpr bool operator==(const _Iterator<_Const2>& __x,
                                     const _Sentinel& __y) {
      return __y.__equal(__x);
    }

    template <bool _Const2,
              typename _Base2 = __detail::__maybe_const_t<_Const2, _Vp>>
    /*
      requires sized_sentinel_for<sentinel_t<_Base>, iterator_t<_Base2>>
     */
    friend constexpr range_difference_t<_Base2> operator-(
        const _Iterator<_Const2>& __x, const _Sentinel& __y) {
      return -__y.__distance_from(__x);
    }

    template <bool _Const2,
              typename _Base2 = __detail::__maybe_const_t<_Const2, _Vp>>
    /*
      requires sized_sentinel_for<sentinel_t<_Base>, iterator_t<_Base2>>
*/
    friend constexpr range_difference_t<_Base2> operator-(
        const _Sentinel& __y, const _Iterator<_Const2>& __x) {
      return __y.__distance_from(__x);
    }

    friend _Sentinel<!_Const>;
  };

  _Vp _M_base = _Vp();
  [[no_unique_address]] _Fp _M_fun;

 public:
  transform_view() = default;

  constexpr transform_view(_Vp __base, _Fp __fun)
      : _M_base(std::move(__base)), _M_fun(std::move(__fun)) {}

  constexpr _Vp base() const&  // requires copy_constructible<_Vp>
  {
    return _M_base;
  }

  constexpr _Vp base() && { return std::move(_M_base); }

  constexpr _Iterator<false> begin() {
    return _Iterator<false>{this, std::begin(_M_base)};
  }

  constexpr _Iterator<true> begin() const
  /*requires range<const _Vp>
    && regular_invocable<const _Fp&, range_reference_t<const _Vp>> */
  {
    return _Iterator<true>{this, std::begin(_M_base)};
  }

  // TODO<joka921> We probably actually need the sentinel case, so we need
  // to handle the overload resolution via `std::enable_if`
  /*
  constexpr _Sentinel<false>
  end()
  { return _Sentinel<false>{std::end(_M_base)}; }
   */

  constexpr _Iterator<false> end()  // requires common_range<_Vp>
  {
    return _Iterator<false>{this, std::end(_M_base)};
  }

  // TODO<joka921> Same here (see above)
  /*
  constexpr _Sentinel<true>
  end() const

    requires range<const _Vp>
      && regular_invocable<const _Fp&, range_reference_t<const _Vp>>
  { return _Sentinel<true>{ranges::end(_M_base)}; }
   */

  constexpr _Iterator<true> end() const
  /*
requires common_range<const _Vp>
  && regular_invocable<const _Fp&, range_reference_t<const _Vp>>
   */
  {
    return _Iterator<true>{this, std::end(_M_base)};
  }

  constexpr auto size()  // requires sized_range<_Vp>
  {
    return std::size(_M_base);
  }

  constexpr auto size() const  // requires sized_range<const _Vp>
  {
    return std::size(_M_base);
  }
};
}  // namespace ql

#endif  // TRANSFORM_VIEW_H
