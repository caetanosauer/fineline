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

#ifndef FINELINE_LOGPAGE_H
#define FINELINE_LOGPAGE_H

/**
 * \file kv_array.h
 *
 * A fixed-length array that encodes arbitrary key-value pairs and stores them in a slot array in
 * the appropriate position.
 */

#include "assertions.h"
#include "encoding.h"
#include "lrtype.h"
#include "slot_array.h"

namespace fineline {

using foster::LRType;
using foster::assert;

template <typename... T>
using LogEncoder = typename foster::VariadicEncoder<foster::InlineEncoder, T...>;

constexpr size_t LogPageSize = 1048576;
constexpr size_t LogrecAlignment = 8;

// TODO we prob dont want htese to be templated like this -- because
// we have to deserialize them at runtime
template <class NodeId = uint32_t, unsigned SeqNumBits = 24>
struct alignas(LogrecAlignment) LogrecHeader
{
    LogrecHeader() {}

    LogrecHeader(NodeId id, uint32_t seq, LRType type)
        : node_id(id), seq_num(seq), type(type)
    {}

    NodeId node_id;
    struct {
        uint32_t seq_num : SeqNumBits;
        LRType type;
    };
};

template <size_t PageSize, class IdType = uint32_t>
class LogPage : protected foster::SlotArray<LogrecHeader<IdType>, PageSize>
{
public:
    using Key = LogrecHeader<IdType>;
    using PayloadPtr = typename foster::SlotArray<Key, PageSize>::PayloadPtr;
    using SlotNumber = typename foster::SlotArray<Key, PageSize>::SlotNumber;
    using ThisType = LogPage<PageSize, IdType>;

    template <typename... T>
    ThisType* log(const Key& hdr, T... args)
    {
        bool success = try_insert(hdr, args...);
        if (!success) {
            next_page_ = new ThisType;
            success = next_page_->try_insert(hdr, args...);
            assert<0>(success, "Record does not fit in log page");
            return next_page_;
        }

        return this;
    }

    class Iterator
    {
    public:
        Iterator(ThisType* lp) :
            current_slot_{0}, lp_{lp}
        {}

        bool next(Key& key, char*& payload)
        {
            if (current_slot_ >= lp_->slot_count()) { return false; }

            auto& slot = lp_->get_slot(current_slot_);
            payload = reinterpret_cast<char*>(lp_->get_payload(slot.ptr));
            key = slot.key;
            current_slot_++;

            return true;
        }

    private:
        SlotNumber current_slot_;
        LogPage<PageSize>* lp_;
    };

    Iterator iterate()
    {
        return Iterator{this};
    }

protected:

    template <typename... T>
    bool try_insert(const LogrecHeader<uint32_t>& hdr, T... args)
    {
        size_t length = LogEncoder<T...>::get_payload_length(args...);

        typename LogPage::PayloadPtr ptr;
        if (!this->allocate_payload(ptr, length)) {
            return false;
        }

        auto slot = this->slot_count();
        if (!this->insert_slot(slot)) {
            // No space left -- free previously allocated payload.
            this->free_payload(ptr, length);
            return false;
        }

        char* addr = static_cast<char*>(this->get_payload(ptr));
        LogEncoder<T...>::encode(addr, args...);
        this->get_slot(slot).key = hdr;
        this->get_slot(slot).ptr = ptr;
        this->get_slot(slot).ghost = false;

        return true;
    }

private:
    LogPage<PageSize>* next_page_;
};

namespace tls {

thread_local LogPage<LogPageSize>* plog = new LogPage<LogPageSize>;

void reset_plog()
{
    plog = new LogPage<LogPageSize>;
}

}

} // namespace fineline

#endif
