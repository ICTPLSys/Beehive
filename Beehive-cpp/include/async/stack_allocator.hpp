#include <cstddef>
#include <stack>

namespace Beehive {
namespace async {

template <size_t StackSize>
class StackAllocator {
public:
    static char *allocate_stack() {
        char *stack;
        if (stacks.empty()) [[unlikely]] {
            stack =
                static_cast<char *>(std::aligned_alloc(StackSize, StackSize));
        } else {
            stack = stacks.top();
            stacks.pop();
        }
        return stack;
    }

    static void deallocate_stack(char *stack) { stacks.push(stack); }

    ~StackAllocator() {
        while (!stacks.empty()) {
            std::free(stacks.top());
            stacks.pop();
        }
    }

private:
    static thread_local std::stack<char *> stacks;
};

template <size_t StackSize>
thread_local std::stack<char *> StackAllocator<StackSize>::stacks;

}  // namespace async
}  // namespace Beehive