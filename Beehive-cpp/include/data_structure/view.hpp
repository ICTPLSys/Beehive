#pragma once
#include <algorithm>
#include <cstddef>
#include <cstdlib>
#include <functional>
#include <utility>

#include "utils/cpu_cycles.hpp"
#include "utils/debug.hpp"

namespace FarLib {

namespace FarView {

template <typename T, bool Mut, typename LastView>
class DropView;

template <typename T, bool Mut, typename LastView>
class TakeView;

template <typename T, bool Mut, typename LastView, typename F>
class FilterView;

template <typename T, bool Mut, typename LastView, typename Visitor>
class VisitView;

template <typename SrcType, bool Mut, typename LastView, typename TargetType, typename Map>
class MapView;

template <typename T, bool Mut, typename Derived>
class View {
    using Element = T;
    using SrcType = T;
    using Self = View<T, Mut, Derived>;
    using GeneratedDropView = DropView<T, Mut, Derived>;
    using GeneratedTakeView = TakeView<T, Mut, Derived>;
    template <typename F>
    using GeneratedFilterView = FilterView<T, Mut, Derived, F>;
    template <typename Visitor>
    using GeneratedVisitView = VisitView<T, Mut, Derived, Visitor>;
    template <typename TargetType, typename Map>
    using GeneratedMapView = MapView<SrcType, Mut, Derived, TargetType, Map>;
public:
    template <typename Func>
    void for_each(Func &&func, DereferenceScope &scope) {
        static_cast<Derived *>(this)->template for_each_impl<Func>(std::forward<Func>(func), scope);
    }

    GeneratedDropView drop(size_t n) {
        return GeneratedDropView(*static_cast<Derived *>(this), n);
    }

    GeneratedTakeView take(size_t n) {
        return GeneratedTakeView(*static_cast<Derived *>(this), n);
    }

    template <typename F>
    GeneratedFilterView<F> filter(F &&f) {
        return GeneratedFilterView<F>(*static_cast<Derived *>(this), f);
    }

    template <typename Visitor>
    GeneratedVisitView<Visitor> visit(Visitor &&visitor) {
        return GeneratedVisitView<Visitor>(*static_cast<Derived *>(this), visitor);
    }

    template <std::invocable<SrcType> Map>
    auto map(Map &&map) {
        using TargetType = std::invoke_result_t<Map, SrcType>;
        return GeneratedMapView<TargetType, Map>(*static_cast<Derived *>(this), map);
    }
};

template <typename T, bool Mut, typename LastView, typename F>
class FilterView : public View<T, Mut, FilterView<T, Mut, LastView, F>> {
    using Element = T;
    using ElementRef = std::conditional_t<Mut, Element &, const Element &>;
private:
    LastView view;
    F filter_;

public:
    FilterView(LastView view, F &&f)
        : view(view), filter_(std::forward<F>(f)) {}

    template <typename Func>
    void for_each_impl(Func &&func, DereferenceScope &scope) {
        view.template for_each([&](ElementRef e) {
            if (filter_(e)) {
                func(e);
            }
        }, scope);
    }
};

template <typename T, bool Mut, typename LastView>
class DropView : public View<T, Mut, DropView<T, Mut, LastView>> {
    using Element = T;
    using ElementRef = std::conditional_t<Mut, Element &, const Element &>;
private:
    LastView view;
    size_t skip_size;

public:
    DropView(LastView view, size_t skip_size)
        : view(view), skip_size(skip_size) {}

    template <typename Func>
    void for_each_impl(Func &&func, DereferenceScope &scope) {
        size_t count = 0;
        view.template for_each([&](ElementRef e) {
            if (count < skip_size) {
                count++;
            } else {
                func(e);
            }
        }, scope);
    }
};

template <typename T, bool Mut, typename LastView>
class TakeView : public View<T, Mut, TakeView<T, Mut, LastView>> {
    using Element = T;
    using ElementRef = std::conditional_t<Mut, Element &, const Element &>;
private:
    LastView view;
    size_t take_num;

public:
    TakeView(LastView &view, size_t take_num)
        : view(view), take_num(take_num) {}

    template <typename Func>
    void for_each_impl(Func &&func, DereferenceScope &scope) {
        size_t count = 0;
        view.template for_each([&](ElementRef e) {
            if (count < take_num) {
                count++;
                func(e);
            } else {
                return;
            }
        }, scope);
    }
};

template <typename T, bool Mut, typename LastView, typename Visitor>
class VisitView : public View<T, Mut, VisitView<T, Mut, LastView, Visitor>> {
    using Element = T;
    using ElementRef = std::conditional_t<Mut, Element &, const Element &>;

private:
    LastView view;
    Visitor visitor;

public:
    VisitView(LastView view, Visitor &&visitor)
        : view(view),
          visitor(std::forward<Visitor>(visitor)) {}

    template <typename Func>
    void for_each_impl(Func &&func, DereferenceScope &scope) {
        view.template for_each([&](ElementRef e) {
            visitor(e);
            func(e);
        }, scope);
    }
};

template <typename SrcType, bool Mut, typename LastView, typename TargetType, typename Map>
class MapView : public View<TargetType, Mut, MapView<SrcType, Mut, LastView, TargetType, Map>> {
    using SrcElement = SrcType;
    using SrcReference = std::conditional_t<Mut, SrcType &, const SrcType &>;
    using TargetElement = TargetType;
private:
    LastView view;
    Map map;

public:
    MapView(LastView view, Map &&map) : view(view), map(std::forward<Map>(map)) {}

    template <typename Func>
    void for_each_impl(Func &&func, DereferenceScope &scope) {
        view.template for_each([&] (SrcReference e) {
            func(map(e));
        }, scope);
    }
};
}  // namespace FarView
}  // namespace FarLib
