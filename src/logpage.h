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

#include <memory>

namespace fineline {

using foster::LRType;
using foster::assert;

template <typename... T>
using LogEncoder = typename foster::VariadicEncoder<foster::InlineEncoder, T...>;

// Default alignment = 1/2 of typical cache line (64B)
constexpr size_t LogrecAlignment = 32;

template <
    class NodeId = uint32_t,
    class SeqNum = uint32_t,
    class LogrecLength = uint16_t
>
struct alignas(LogrecAlignment) LogrecHeader
{
    using IdType = NodeId;
    using SeqNumType = SeqNum;

    LogrecHeader() {}

    LogrecHeader(NodeId id, SeqNum seq, LRType type)
        : node_id(id), seq_num(seq), type(type)
        // length is set by LogPage
    {}

    NodeId node_id;
    SeqNum seq_num;
    LogrecLength length;
    LRType type;

    friend bool operator<(
            const LogrecHeader<NodeId, SeqNum, LogrecLength>& a,
            const LogrecHeader<NodeId, SeqNum, LogrecLength>& b)
    {
        /*
         * "raw" comparison operator that uses the binary contents for comparison in order to
         * speed up sort and select operarions. This is also known as a "normalized key".
         * Because I am not sure about the implications of different struct alignments, I am
         * asserting that the two key fields are of the same size.
         */
        static_assert(sizeof(NodeId) == sizeof(SeqNum),
                "LogrecHeader comparison currently requires the same types for NodeId and SeqNum");

        return memcmp(&a, &b, sizeof(NodeId) + sizeof(SeqNum)) < 0;
    }
};

template <class Key>
class AbstractLogIterator
{
public:
    virtual bool next(Key&, char*&) { return false; }
};

template <size_t PageSize, class LogrecHeader>
class LogPage : public foster::SlotArray<LogrecHeader, PageSize>
{
public:
    using Key = LogrecHeader;
    using PayloadPtr = typename foster::SlotArray<Key, PageSize>::PayloadPtr;
    using SlotNumber = typename foster::SlotArray<Key, PageSize>::SlotNumber;
    using ThisType = LogPage<PageSize, LogrecHeader>;

    template <typename... T>
    bool try_insert(const LogrecHeader& hdr, T... args)
    {
        size_t length = LogEncoder<T...>::get_payload_length(args...);

        typename LogPage::PayloadPtr ptr;
        if (!this->allocate_payload(ptr, length)) {
            return false;
        }

        auto slot = this->slot_count();
        if (!this->insert_slot(slot)) {
            this->free_payload(ptr, length);
            return false;
        }

        char* addr = static_cast<char*>(this->get_payload(ptr));
        LogEncoder<T...>::encode(addr, args...);
        this->get_slot(slot).key = hdr;
        this->get_slot(slot).key.length = length;
        this->get_slot(slot).ptr = ptr;
        this->get_slot(slot).ghost = false;

        return true;
    }

    bool try_insert_raw(const LogrecHeader& hdr, const char* payload)
    {
        typename LogPage::PayloadPtr ptr;
        if (!this->allocate_payload(ptr, hdr.length)) {
            return false;
        }

        auto slot = this->slot_count();
        if (!this->insert_slot(slot)) {
            this->free_payload(ptr, hdr.length);
            return false;
        }

        char* addr = static_cast<char*>(this->get_payload(ptr));
        ::memcpy(addr, payload, hdr.length);
        this->get_slot(slot).key = hdr;
        this->get_slot(slot).ptr = ptr;
        this->get_slot(slot).ghost = false;

        return true;
    }

    class Iterator : public AbstractLogIterator<Key>
    {
    public:
        Iterator(ThisType* lp, bool forward = true) :
            current_slot_{0}, forward_(forward), lp_{lp}
        {
            if (!forward && lp) { current_slot_ = lp_->slot_count() - 1; }
        }

        bool next(Key& key, char*& payload)
        {
            if (!lp_ || current_slot_ >= lp_->slot_count() || current_slot_ < 0) {
                return false;
            }

            auto& slot = lp_->get_slot(current_slot_);
            payload = reinterpret_cast<char*>(lp_->get_payload(slot.ptr));
            key = slot.key;
            current_slot_ += forward_ ? 1 : -1;

            return true;
        }

    private:
        SlotNumber current_slot_;
        bool forward_;
        ThisType* lp_;
    };

    template <class... Args>
    std::unique_ptr<Iterator> iterate(Args... args)
    {
        return std::unique_ptr<Iterator>{new Iterator{this, args...}};
    }

};

} // namespace fineline

#endif
