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

#ifndef FINELINE_AETHER_H
#define FINELINE_AETHER_H

#include "assertions.h"
#include "legacy/carray_slot.h"

namespace fineline {

using foster::assert;

template <
    class LogPage,
    class Latch,
    template<class> class Buffer,
    template<class> class CArray
>
class AetherInsertBuffer
{
public:

    using SlotNumber = typename LogPage::SlotNumber;
    using PayloadPtr = typename LogPage::PayloadPtr;
    using EpochNumber = typename Buffer<LogPage>::EpochNumber;
    using Reservation = typename legacy::BaseCArraySlot::StatusType;

    struct CArraySlot : public legacy::BaseCArraySlot
    {
        SlotNumber first_slot;
        PayloadPtr first_payload;
        std::shared_ptr<LogPage> log_page;
        EpochNumber epoch;
    };

    static constexpr unsigned PayloadBits = 32;
    static constexpr unsigned PayloadBlockSize = LogPage::AlignmentSize;

    AetherInsertBuffer(std::shared_ptr<Buffer<LogPage>> buffer)
        : buffer_(buffer)
    {}

    template <class PrivateLogPage>
    EpochNumber insert(const PrivateLogPage& plog)
    {
        // TODO: this might not be needed if we abstract the copy
        static_assert(LogPage::AlignmentSize == PrivateLogPage::AlignmentSize,
            "Private log page and log page must have the same alignment");

        auto first_payload = plog.get_first_payload();
        auto payload_count = plog.get_payload_end() - first_payload;
        auto to_reserve = encode_reservation(plog.slot_count(), payload_count);

        CArraySlot* cslot {nullptr};
        Reservation target {0};

        join_carray(to_reserve, cslot, target);
        auto epoch = cslot->epoch;
        copy_to_target(plog, target, cslot);
        leave_carray(cslot, to_reserve);

        return epoch;
    }

    static Reservation encode_reservation(SlotNumber slot, size_t payload_count)
    {
        assert<1>(payload_count < (1ul << PayloadBits));
        // one bit must be left for sign
        assert<1>(slot < (1ul << (PayloadBits-1)));
        assert<1>(slot > 0 && payload_count > 0);

        return (slot << PayloadBits) + payload_count;
    }

    static std::pair<SlotNumber, size_t> decode_reservation(Reservation resv)
    {
        return std::make_pair(resv >> PayloadBits, (resv << PayloadBits) >> PayloadBits);
    }

    static size_t get_reservation_bytes(Reservation resv)
    {
        auto decoded = decode_reservation(resv);
        return decoded.first * sizeof(typename LogPage::Slot)
            + decoded.second * PayloadBlockSize;
    }

protected:

    void join_carray(Reservation to_reserve, CArraySlot*& cslot, Reservation& target)
    {
        Reservation old_status;
        cslot = carray_.join_slot(to_reserve, old_status);
        target = old_status;

        assert<1>(cslot);

        if (CArray<CArraySlot>::is_leader(old_status)) {
            // acquire latch on behalf of whole group
            latch_.acquire_write();
            assert<1>(target == 0);

            carray_.replace_active_slot(cslot);
            old_status = carray_.fetch_slot_status(cslot);
            reserve_space(cslot, old_status);
            carray_.finish_slot_reservation(cslot, old_status);
        }
        else {
            carray_.wait_for_leader(cslot);
        }
    }

    void reserve_space(CArraySlot* cslot, Reservation to_reserve)
    {
        // Step 1) make sure current page exists and has enough space
        auto space_needed = get_reservation_bytes(to_reserve);
        if (space_needed > curr_page_->free_space()) {
            // release current page and get new one
            curr_page_.reset();
        }
        if (!curr_page_) {
            curr_page_ = buffer_->produce(cslot->epoch);
        }
        assert<1>(curr_page_.get());
        assert<1>(space_needed <= curr_page_->free_space());
        cslot->log_page = curr_page_;

        // Step 2) allocate slots and payloads requested
        // TODO: log page class can abstract this reservation for us
        cslot->first_slot = curr_page_->slot_count();
        auto resv = decode_reservation(to_reserve);
        auto slot_count = resv.first;
        for (SlotNumber s = 0; s < slot_count; s++) {
            bool success = curr_page_->insert_slot(curr_page_->slot_count());
            assert<1>(success);
        }

        cslot->first_payload = curr_page_->get_first_payload();
        PayloadPtr dest_ptr;
        auto payload_count = resv.second;
        bool success = curr_page_->allocate_payload(dest_ptr, payload_count * PayloadBlockSize);
        assert<1>(success);

        // Step 3) release latch to allow copies and insertions by other threads
        latch_.release_write();
    }

    template <class PrivateLogPage>
    void copy_to_target(const PrivateLogPage& plog, Reservation target, CArraySlot* cslot)
    {
        auto payload_count = decode_reservation(target).second;
        auto target_payload = cslot->first_payload - payload_count;

        // TODO: this can also be abstracted, like we did for move_kv_records
        // slots cannot be copied with memcpy because slot size might be different in plog page
        for (SlotNumber s = 0; s < plog.slot_count(); s++) {
            auto& src = plog.get_slot(s);
            auto& dest = curr_page_->get_slot(cslot->first_slot + s);
            dest.key = src.key;
            dest.ghost = src.ghost;
            dest.ptr = cslot->first_payload - (plog.get_payload_end() - src.ptr);
        }

        // now copy payloads
        ::memcpy(cslot->log_page->get_payload(target_payload),
                plog.get_payload(plog.get_first_payload()),
                payload_count * PayloadBlockSize);

    }

    void leave_carray(CArraySlot* cslot, Reservation to_reserve)
    {
        auto end_status = carray_.leave_slot(cslot, to_reserve);
        if (CArray<CArraySlot>::is_last_to_leave(end_status, to_reserve)) {
            /*
             * All we have to do to release this consolidation group is set the log page's shared
             * pointer to null, which will decrease its reference counter. Once the counter drops
             * to zero, all inserts on that page have finished. Also, because the insert buffer
             * class keeps a shared pointer in curr_page_, we know that no other inserts may come
             * unexpectedly. Thus, a reference counter of zero indicates that the page is now done
             * with and can be flushed by the commit service.
             */
            cslot->log_page.reset();
            carray_.free_slot(cslot);
        }
    }

private:
    CArray<CArraySlot> carray_;
    Latch latch_;
    std::shared_ptr<Buffer<LogPage>> buffer_;
    std::shared_ptr<LogPage> curr_page_;
};

} // namespace fineline

#endif
