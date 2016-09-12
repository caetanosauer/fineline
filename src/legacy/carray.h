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

/**
 * \defgroup CARRAY Consolidation Array
 * \ingroup SSMLOG
 * \brief \b Consolidation \b Array (\b C-Array).
 * \details
 * Logging functions/members to implement \e Consolidation-Array with \e Decoupled-Buffer-Fill
 * and \e Delegated-Buffer-Release invented at CMU/EPFL.
 * This technique dramatically reduces the contention in log buffer
 * accesses. For more details, read the Aether paper and its extended version on VLDB Journal.
 *
 * \section ACK Acknowledgement
 * The ideas, performance evaluations, and the initial implementation are solely done by the
 * EPFL team. We just took the implementation and refactored, keeping the main logics.
 * @see https://bitbucket.org/shoremt/shore-mt/commits/66e5c0aa2f6528cfdbda0907ad338066f14ec066
 *
 * \section DIFF Differences from Original Implementation
 * A few minor details have been changed.
 *   \li Separated the core C-Array logics to the classes in this file rather than
 * further bloating log_core.
 *   \li CArraySlot (insert_info in original code) places me2 at first so that we can avoid
 * the very tricky (or dubious in portability) offset calculation by union-ing int and pointer.
 * It's simply a reinterpret_cast in our code.
 *   \li qnode itself has the status as union so that we don't need hacked_qnode thingy.
 *   \li We use Lintel's atomic operations, which have slightly different method signatures.
 * We made according changes to incorporate that.
 *   \li Lots of comments added.
 *
 * \section PERF Performance Improvements
 * As of 20140220, with and without this improvement, TPCC on 4-socket machine is as
 * follows (12 worker threads, 3 active slots, CARRAY_RELEASE_DELEGATION=false).
 *   \li LOCK-ON: BEFORE=13800 TPS, AFTER=15400 TPS.
 *   \li LOCK-OFF: BEFORE=17900 TPS, AFTER=30900 TPS.
 *   \li i.e. LOCK-OFF, LOG-OFF: 63000 TPS.
 * So, big improvements in log contention, but not completely removing it.
 * Now lock manager is bigger bottleneck.
 *
 * \section CON Considerations
 * Among the three techniques, \e Delegated-Buffer-Release was a bit dubious to be added
 * because, as shown in the Aether paper, it has little benefits in "usual" workload yet
 * adds 10% overheads because of a few more atomic operations.
 * It might be worth the extra 10% for stability in case of highly skewed log sizes, so we
 * should keep an eye. CARRAY_RELEASE_DELEGATION option turns on/off this feature.
 *
 * Also, consider adjusting the number of active slots depending on the number of worker
 * threads as suggested in the paper. The startup option \b sm_carray_slots does it.
 * The default value for this option is ConsolidationArray#DEFAULT_ACTIVE_SLOT_COUNT
 *
 * \section REF Reference
 * \li Ryan Johnson, Ippokratis Pandis, Radu Stoica, Manos Athanassoulis, and Anastasia
 * Ailamaki. "Aether: a scalable approach to logging."
 * Proceedings of the VLDB Endowment 3, no. 1-2 (2010): 681-692.
 * \li Ryan Johnson, Ippokratis Pandis, Radu Stoica, Manos Athanassoulis, and Anastasia
 * Ailamaki. "Scalability of write-ahead logging on multicore and multisocket hardware."
 * The VLDB Journal 21, no. 2 (2012): 239-263.
 */

#ifndef FINELINE_LEGACY_CARRAY_H
#define FINELINE_LEGACY_CARRAY_H

#include <atomic>
#include <thread>

#include "assertions.h"
#include "mcs_lock.h"

namespace fineline {
namespace legacy {

using foster::assert;

/**
 * \brief The implementation class of \b Consolidation \b Array.
 * \ingroup CARRAY
 * \details
 * See Section A.2 and A.3 of Aether paper.
 */
template <class CArraySlot>
class ConsolidationArray
{
public:
    using StatusType = typename CArraySlot::StatusType;

    /** @param[in] active_slot_count Max number of slots that can be active at the same time */
    ConsolidationArray(int active_slot_count = 3);
    ~ConsolidationArray();

    /** Total number of slots. */
    static constexpr StatusType ALL_SLOT_COUNT      = 256;

    /**
     * slots that are in active slots and up for grab have this StatusType.
     */
    static constexpr StatusType SLOT_AVAILABLE      = 0;
    /**
     * slots that are in the pool but not in active slots
     * have this StatusType.
     */
    static constexpr StatusType SLOT_UNUSED         = -1;
    /**
     * Once the first thread in the slot puts this as StatusType, other threads can no
     * longer join the slot.
     */
    static constexpr StatusType SLOT_PENDING        = -2;
    /**
     * Once the first thread acquires a buffer space and LSN, it puts this \b MINUS
     * the combined log size as the StatusType. All threads in the slot atomically add
     * its log size to this, making the last one notice that it is exactly SLOT_FINISHED.
     */
    static constexpr StatusType SLOT_FINISHED       = -4;

    static_assert(sizeof(CArraySlot) % CACHELINE_SIZE == 0,
       "size of CArraySlot must be aligned to CACHELINE_SIZE for better performance");

    /**
     * Grabs some active slot and \b atomically joins the slot.
     * @param[in] size log size to add
     * @param[out] status \b atomically obtained status of the joined slot
     * @return the slot we have just joined
     */
    CArraySlot*         join_slot(StatusType size, StatusType &status);

    /**
     * join the memcpy-complete queue but don't spin yet.
     * This sets the CArraySlot#me2 and CArraySlot#pred2.
     */
    void                join_expose(CArraySlot* slot);

    /**
     * Spins until the leader of the given slot acquires log buffer.
     * @pre current thread is not the leader of the slot
     */
    void                wait_for_leader(CArraySlot* slot);

    /**
     * Retire the given slot from active slot, upgrading an unused thread to an active slot.
     * @param[slot] slot to replace
     * @pre current thread is the leader of the slot
     * @pre slot->count > SLOT_AVAILABLE, in other words thte slot is
     * already owned and no other thread can disturb this change.
     */
    void                replace_active_slot(CArraySlot* slot);

    /**
     * Set the status of the given slot to UNUSED, making it eligible for new inserts.
     */
    void free_slot(CArraySlot* cslot)
    {
        cslot->status = SLOT_UNUSED;
    }

    StatusType fetch_slot_status(CArraySlot* cslot)
    {
        return std::atomic_exchange(&cslot->status, SLOT_PENDING);
    }

    void finish_slot_reservation(CArraySlot* cslot, StatusType reserved)
    {
        cslot->status = SLOT_FINISHED - reserved;
    }

    StatusType leave_slot(CArraySlot* cslot, StatusType reserved)
    {
        return std::atomic_fetch_add(&cslot->status, reserved);
    }

    static bool is_last_to_leave(StatusType prev_status, StatusType reserved)
    {
        return prev_status + reserved == SLOT_FINISHED;
    }

    static bool is_leader(StatusType prev_status)
    {
        return prev_status == SLOT_AVAILABLE;
    }

private:
    int _indexof(const CArraySlot* slot) const
    {
        return slot - _all_slots;
    }

    /**
     * Clockhand of active slots. We use this to evenly distribute accesses to slots.
     * This value is not protected at all because we don't care even if it's not
     * perfectly even. We anyway atomically obtain the slot.
     */
    int32_t _slot_mark {0};
    /** Max number of slots that can be active at the same time. */
    const int32_t _active_slot_count;
    /** All slots, including available, currently used, or retired slots. */
    CArraySlot _all_slots[ALL_SLOT_COUNT];
    /** Active slots that are (probably) up for grab or join. */
    CArraySlot** _active_slots;
};

} // namespace legacy
} // namespace fineline

#endif // LOG_CARRAY_H
