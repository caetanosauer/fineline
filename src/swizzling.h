/*
 * MIT License
 *
 * Copyright (c) 2016 Caetano Sauer
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and
 * associated documentation files (the "Software"), to deal in the Software without restriction,
 * including without limitation the rights to use, copy, modify, merge, publish, distribute,
 * sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all copies or
 * substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT
 * NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
 * DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

#ifndef FINELINE_SWIZZLING_H
#define FINELINE_SWIZZLING_H

#include <limits>

#include "assertions.h"
#include "metaprog.h"

namespace fineline {

using foster::assert;

template <
    typename T,
    typename Fetcher
>
class SwizzablePtr
{
public:

    using PointeeType = T;
    using NodeId = typename Fetcher::IdType;

    explicit SwizzablePtr(T* p = nullptr) : ptr_(p) {}

    template <typename OtherT, typename = typename
        std::enable_if<std::is_convertible<OtherT*, T*>::value>::type>
    SwizzablePtr(const SwizzablePtr<OtherT, Fetcher>& other)
        : SwizzablePtr<T, Fetcher>(other)
    {}

    template <typename OtherT, typename = typename
        std::enable_if<std::is_convertible<OtherT*, T*>::value>::type>
    SwizzablePtr(const SwizzablePtr<OtherT, Fetcher>&& other)
        : SwizzablePtr<T, Fetcher>(std::move(other))
    {}

    template <typename OtherT>
    SwizzablePtr& operator=(const SwizzablePtr<OtherT, Fetcher>& other)
    {
        this->ptr_ = other.ptr_;
        return *this;
    }

    template <typename OtherT>
    SwizzablePtr& operator=(const SwizzablePtr<OtherT, Fetcher>&& other)
    {
        this->ptr_ = std::move(other.ptr_);
        return *this;
    }

    SwizzablePtr& operator=(void* raw_ptr)
    {
        this->ptr_ = reinterpret_cast<T*>(raw_ptr);
        return *this;
    }

    operator bool() const
    {
        return unset_unswizzled_bit(ptr_);
    }

    T* operator->()
    {
        swizzle();
        return ptr_;
    }

    T& operator*()
    {
        swizzle();
        return *ptr_;
    }

    bool operator!() const { return !ptr_; }
    bool operator==(const SwizzablePtr& other) const { return other.ptr_ == ptr_; }

    operator void*() { return ptr_; }
    T* get() { return ptr_; }

    friend std::ostream& operator<< (std::ostream& out, const SwizzablePtr<T, Fetcher>& ptr)
    {
        return out << ptr.ptr_;
    }

    template <class OtherT>
    static SwizzablePtr<T, Fetcher> static_pointer_cast(SwizzablePtr<OtherT, Fetcher> other)
    {
        return SwizzablePtr<T, Fetcher>(static_cast<T*>(other.operator void*()));
    }

    bool is_swizzled() const
    {
        return !(cast_to_int(ptr_) & UNSWIZZLED_BIT_MASK);
    }

    void swizzle()
    {
        if (is_swizzled()) { return; }

        auto p = unset_unswizzled_bit(ptr_);
        NodeId node_id = static_cast<NodeId>(cast_to_int(p));
        ptr_ = Fetcher::fetch(node_id);

        assert<1>(is_swizzled());
    }

    void unswizzle(NodeId node_id)
    {
        if (!is_swizzled()) { return; }

        Fetcher::free(ptr_);
        ptr_ = set_unswizzled_bit(reinterpret_cast<T*>(node_id));

        assert<1>(!is_swizzled());
    }

protected:

    using PointerMask = foster::meta::UnsignedInteger<sizeof(T*)>;

    static T* set_unswizzled_bit(T* p)
    {
        return cast_from_int(cast_to_int(p) ^ UNSWIZZLED_BIT_MASK);
    }

    static T* unset_unswizzled_bit(T* p)
    {
        return cast_from_int(cast_to_int(p) & ~UNSWIZZLED_BIT_MASK);
    }

    static PointerMask cast_to_int(T* p)
    {
        return reinterpret_cast<PointerMask>(p);
    }

    static T* cast_from_int(PointerMask i)
    {
        return reinterpret_cast<T*>(i);
    }

private:

    /*
     * Use a tagged pointer approach to indicate swizzling state, i.e., some bits of the actual
     * pointer are assumed to be unused by the virtual addressing scheme and thus can be used
     * for our own purposes. The assumption here is that the most significant bit can be used,
     * i.e., that a virtual address is at most 63 bits. Alternatively, if all allocations are
     * properly aligned, the least significant bit could also be used.
     */
    static constexpr PointerMask UNSWIZZLED_BIT_MASK =
        static_cast<PointerMask>(1) << (8*sizeof(T*) - 1);

    T* ptr_;
};

} // namespace fineline

#endif

