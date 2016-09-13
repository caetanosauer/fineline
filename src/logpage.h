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

#include "assertions.h"
#include "encoding.h"
#include "slot_array.h"
#include "logheader.h"

#include <memory>

namespace fineline {

using foster::LRType;
using foster::assert;

template <typename... T>
using LogEncoder = typename foster::VariadicEncoder<foster::InlineEncoder, T...>;

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
    bool try_insert(LogrecHeader& hdr, T... args)
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
        hdr.set_length(length);
        this->get_slot(slot) = { hdr, ptr, false };

        return true;
    }

    bool try_insert_raw(const LogrecHeader& hdr, const char* payload)
    {
        typename LogPage::PayloadPtr ptr;
        if (!this->allocate_payload(ptr, hdr.length())) {
            return false;
        }

        auto slot = this->slot_count();
        if (!this->insert_slot(slot)) {
            this->free_payload(ptr, hdr.length());
            return false;
        }

        char* addr = static_cast<char*>(this->get_payload(ptr));
        ::memcpy(addr, payload, hdr.length());
        this->get_slot(slot).key = hdr;
        this->get_slot(slot).ptr = ptr;
        this->get_slot(slot).ghost = false;

        return true;
    }

    size_t get_payload_length(SlotNumber s) const
    {
        return this->get_slot(s).key.length();
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
