
#pragma once
#include "data_structure/far_vector.hpp"
#include "data_structure/view.hpp"

namespace Beehive {
template <VecElementType T, size_t GroupSize>
template <bool Mut>
class FarVector<T, GroupSize>::VectorView
    : public Beehive::FarView::View<T, Mut,
                                    FarVector<T, GroupSize>::VectorView<Mut>> {
    using Pointer = FarVector<T, GroupSize>::Pointer;

private:
    Pointer start;
    Pointer end;

public:
    VectorView(Pointer &&start, Pointer &&end)
        : start(std::forward<Pointer>(start)),
          end(std::forward<Pointer>(end)) {}
    template <typename Func>
    void for_each_impl(Func &&func) {
        RootDereferenceScope scope;
        auto ed = this->end.template get_iterator<Mut>(scope);
        for (auto it = this->start.template get_iterator<Mut>(scope); it < ed;
             it.next(scope)) {
            func(*it);
        }
    }
};
}  // namespace Beehive