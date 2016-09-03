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

/*
 * (c) Copyright 2014, Hewlett-Packard Development Company, LP
 */

/*
     Shore-MT -- Multi-threaded port of the SHORE storage manager

                       Copyright (c) 2010-2014
      Data Intensive Applications and Systems Labaratory (DIAS)
               Ecole Polytechnique Federale de Lausanne

                         All Rights Reserved.

   Permission to use, copy, modify and distribute this software and
   its documentation is hereby granted, provided that both the
   copyright notice and this permission notice appear in all copies of
   the software, derivative works or modified versions, and any
   portions thereof, and that both notices appear in supporting
   documentation.

   This code is distributed in the hope that it will be useful, but
   WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. THE AUTHORS
   DISCLAIM ANY LIABILITY OF ANY KIND FOR ANY DAMAGES WHATSOEVER
   RESULTING FROM THE USE OF THIS SOFTWARE.
*/

#include "carray.h"

namespace fineline {
namespace legacy {

template <class CArraySlot>
void ConsolidationArray<CArraySlot>::wait_for_leader(CArraySlot* info) {
    long old_count;
    while( (old_count=info->status) >= SLOT_FINISHED);
    // TODO: probably not needed because info->status is atomic
    // std::atomic_thread_fence(std::memory_order_acquire);
}

template <class CArraySlot>
ConsolidationArray<CArraySlot>::ConsolidationArray(int active_slot_count)
    : _active_slot_count(active_slot_count)
{
    // Zero-out all slots
    ::memset(_all_slots, 0, sizeof(CArraySlot) * ALL_SLOT_COUNT);
    typedef CArraySlot* CArraySlotPtr;
    _active_slots = new CArraySlotPtr[_active_slot_count];
    for (int i = 0; i < ALL_SLOT_COUNT; ++i) {
        _all_slots[i].status = SLOT_UNUSED;
    }
    // Mark initially active slots
    for (int i = 0; i < _active_slot_count; ++i) {
        _active_slots[i] = _all_slots + i;
        _active_slots[i]->status = SLOT_AVAILABLE;
    }
}

template <class CArraySlot>
ConsolidationArray<CArraySlot>::~ConsolidationArray() {
    delete[] _active_slots;
    // Check all slots are freed
    for (int i = 0; i < ALL_SLOT_COUNT; ++i) {
        assert<0>(_all_slots[i].status == SLOT_UNUSED
            || _all_slots[i].status == SLOT_AVAILABLE);
    }
}


template <class CArraySlot>
CArraySlot* ConsolidationArray<CArraySlot>::join_slot(int32_t size, StatusType &old_count)
{
    assert<1>(size > 0);
    auto idx = std::hash<std::thread::id>{}(std::this_thread::get_id());

    while (true) {
        // probe phase
        CArraySlot* info = nullptr;
        while (true) {
            idx = (idx + 1) % _active_slot_count;
            info = _active_slots[idx];
            old_count = info->status;
            if (old_count >= SLOT_AVAILABLE) {
                // this slot is available for join!
                break;
            }
        }

        // join phase
        while (true) {
            // set to 'available' and add our size to the slot
            StatusType new_count = old_count + size;
            StatusType old_count_cas_tmp = old_count;
            if(std::atomic_compare_exchange_strong(
                &info->status, &old_count_cas_tmp, new_count))
            {
                // CAS succeeded. All done.
                // The assertion below doesn't necessarily hold because of the
                // ABA problem -- someone else might have grabbed the same slot
                // and gone through a whole join-release cycle, so that info is
                // now on a different array position. In general, this second
                // while loop must not use idx at all.
                // assert<1>(old_count != 0 || _active_slots[idx] == info);
                return info;
            }
            else {
                // the status has been changed.
                assert<1>(old_count != old_count_cas_tmp);
                old_count = old_count_cas_tmp;
                if (old_count < SLOT_AVAILABLE) {
                    // it's no longer available. retry from probe
                    break;
                } else {
                    // someone else has joined, but still able to join.
                    continue;
                }
            }
        }
    }
}

template <class CArraySlot>
void ConsolidationArray<CArraySlot>::replace_active_slot(CArraySlot* info)
{
    assert<1>(info->status > SLOT_AVAILABLE);
    while (SLOT_UNUSED != _all_slots[_slot_mark].status) {
        if(++_slot_mark == ALL_SLOT_COUNT) {
            _slot_mark = 0;
        }
    }
    _all_slots[_slot_mark].status = SLOT_AVAILABLE;

    // Look for pointer to the slot in the active array
    for (int i = 0; i < _active_slot_count; i++) {
        if (_active_slots[i] == info) {
            _active_slots[i] = _all_slots + _slot_mark;
        }
    }
}

} // namespace legacy
} // namespace fineline
