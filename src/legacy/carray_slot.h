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

#ifndef FINELINE_LEGACY_CARRAY_SLOT_H
#define FINELINE_LEGACY_CARRAY_SLOT_H

#include "mcs_lock.h"

namespace fineline {
namespace legacy {

/**
 * \brief One slot in ConsolidationArray.
 * \ingroup CARRAY
 * \details
 * Each slot belongs to two mcs_lock queues, one for buffer acquisition (me/_insert_lock)
 * and another for buffer release (me2/_expose_lock).
 */
struct BaseCArraySlot alignas (CACHELINE_SIZE)
{
    using StatusType = int64_t;

    /**
    * \brief The secondary queue lock used to delegate buffer-release.
    * Lock head is ConsolidationArray::_expose_lock.
    * This must be the first member as we reinterpret qnode as insert_info.
    * See Section A.3 of Aether paper.
    */
    mcs_lock::qnode me2;

    /**
     * \brief The current status of this slot.
     * \details
     * This is the key variable used for every atomic operation of C-array slot.
     * @see carray_status_t
     */
    StatusType status;

    /**
    * The main queue lock used to acquire log buffers.
    * Lock head is log_core::_insert_lock.
    * \NOTE This should not be in the same cache line as me2.
    */
    mcs_lock::qnode me;

    /**
    * Predecessor qnode of me2. Used to delegate buffer release.
    */
    mcs_lock::qnode* pred2;
};


} // namespace legacy
} // namespace fineline

#endif
