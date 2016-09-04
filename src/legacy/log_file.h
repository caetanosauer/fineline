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
 * (c) Copyright 2011-2013, Hewlett-Packard Development Company, LP
 */


/* -*- mode:C++; c-basic-offset:4 -*-
     Shore-MT -- Multi-threaded port of the SHORE storage manager

                       Copyright (c) 2007-2009
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

/*<std-header orig-src='shore' incl-file-exclusion='SRV_LOG_H'>

 $Id: partition.h,v 1.6 2010/08/23 14:28:18 nhall Exp $

SHORE -- Scalable Heterogeneous Object REpository

Copyright (c) 1994-99 Computer Sciences Department, University of
                      Wisconsin -- Madison
All Rights Reserved.

Permission to use, copy, modify and distribute this software and its
documentation is hereby granted, provided that both the copyright
notice and this permission notice appear in all copies of the
software, derivative works or modified versions, and any portions
thereof, and that both notices appear in supporting documentation.

THE AUTHORS AND THE COMPUTER SCIENCES DEPARTMENT OF THE UNIVERSITY
OF WISCONSIN - MADISON ALLOW FREE USE OF THIS SOFTWARE IN ITS
"AS IS" CONDITION, AND THEY DISCLAIM ANY LIABILITY OF ANY KIND
FOR ANY DAMAGES WHATSOEVER RESULTING FROM THE USE OF THIS SOFTWARE.

This software was developed with support by the Advanced Research
Project Agency, ARPA order number 018 (formerly 8230), monitored by
the U.S. Army Research Laboratory under contract DAAB07-91-C-Q518.
Further funding for this work was provided by DARPA through
Rome Research Laboratory Contract No. F30602-97-2-0247.

*/

#ifndef FINELINE_LEGACY_LOG_FILE_H
#define FINELINE_LEGACY_LOG_FILE_H

#include <memory>
#include <atomic>
#include <mutex>

#include "lsn.h"

namespace fineline {
namespace legacy {

template <size_t>
class log_storage; // forward

template<size_t PageSize>
class log_file {
public:
    using BlockOffset = uint32_t;

    /*
     * Log file number consists of 2 parts: the high bits represent the merge depth; it is zero for
     * files where log pages are appended directly, and N > 0 for files created by merging files of
     * depth N-1. This mechanism makes it easy to manage files for partitions in different levels.
     */
    static constexpr size_t FileHighNumBits = 8;
    static constexpr size_t FileLowNumBits = 24;
    using FileNumber = UnsignedNumberPair<FileHighNumBits, FileLowNumBits>;

    static constexpr int invalid_fhdl = -1;

    log_file(std::shared_ptr<log_storage<PageSize>>, FileNumber);
    virtual ~log_file() { }

    void open_for_append();
    void open_for_read();
    void close_for_append();
    void close_for_read();

    void read(BlockOffset, void* dest);
    void append(void* src);

    size_t get_size();

    void scan_for_size();

    void destroy();

    FileNumber num() const { return _num; }

    bool is_open_for_read() const { return (_fhdl_rd != invalid_fhdl); }

    bool is_open_for_append() const { return (_fhdl_app != invalid_fhdl); }

    void set_size(size_t s) { _size = s; }

private:
    FileNumber _num;
    std::shared_ptr<log_storage<PageSize>> _owner;
    std::atomic<size_t> _size;
    int _fhdl_rd;
    int _fhdl_app;
    std::mutex _mutex;
};

} // namespace legacy
} // namespace fineline

#endif
