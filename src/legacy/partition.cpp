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

/*<std-header orig-src='shore'>

 $Id: partition.cpp,v 1.11 2010/12/08 17:37:43 nhall Exp $

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

#include "partition.h"

#include <sys/stat.h>
#include <stdexcept>
#include <string>
#include <cerrno>
#include <fcntl.h>

#include "assertions.h"
#include "log_storage.h"

namespace fineline {
namespace legacy {

using foster::assert;

template<size_t PageSize>
log_file<PageSize>::log_file(std::shared_ptr<log_storage<PageSize>> owner, FileNumber num)
    : _num(num), _owner(owner), _size(-1),
      _fhdl_rd(invalid_fhdl), _fhdl_app(invalid_fhdl)
{
}

void check_error(int res)
{
    if (res != 0) {
        auto what = "I/O error! errno = " + std::to_string(errno);
        throw std::runtime_error(what);
    }
}

template<size_t PageSize>
void log_file<PageSize>::open_for_append()
{
    std::unique_lock<std::mutex> lck(_mutex);

    assert<1>(!is_open_for_append());

    int fd, flags = O_RDWR | O_CREAT;
    string fname = _owner->make_log_name(_num);
    fd = ::open(fname.c_str(), flags);
    if (fd < 0) { throw std::runtime_error("Error opening log file"); }
    _fhdl_app = fd;
}

template<size_t PageSize>
void log_file<PageSize>::open_for_read()
{
    std::unique_lock<std::mutex> lck(_mutex);

    if(_fhdl_rd == invalid_fhdl) {
        string fname = _owner->make_log_name(_num);
        int flags = O_RDONLY;
        int fd = ::open(fname.c_str(), flags, 0);
        if (fd < 0) { throw std::runtime_error("Error opening log file"); }

        assert<3>(_fhdl_rd == invalid_fhdl);
        _fhdl_rd = fd;
    }
    assert<3>(is_open_for_read());
}

template<size_t PageSize>
void log_file<PageSize>::close_for_append()
{
    std::unique_lock<std::mutex> lck(_mutex);

    if (_fhdl_app != invalid_fhdl)  {
        check_error(::close(_fhdl_app));
        _fhdl_app = invalid_fhdl;
    }
}

template<size_t PageSize>
void log_file<PageSize>::close_for_read()
{
    std::unique_lock<std::mutex> lck(_mutex);

    if (_fhdl_rd != invalid_fhdl)  {
        check_error(::close(_fhdl_app));
        _fhdl_rd = invalid_fhdl;
    }
}

template<size_t PageSize>
size_t log_file<PageSize>::get_size()
{
    if (_size < 0) { scan_for_size(); }
    assert<3>(_size >= 0);
    return _size;
}

template<size_t PageSize>
void log_file<PageSize>::scan_for_size()
{
    // start scanning backwards from end of file until first valid log page is found
    open_for_read();

    std::unique_lock<std::mutex> lck(_mutex);
    if (_size >= 0) { return; }

    struct stat statbuf;
    ::fstat(_fhdl_rd, &statbuf);
    size_t fsize = statbuf.st_size;

    if (statbuf.st_size == 0) {
        _size = 0;
        return;
    }

    /*
     * For now, we just assume that if the file size is a multiple of the page size, then the
     * file is consistent. Otherwise, we just trim out whatever leftover is at the end.
     * In the future, we would probably want to check a CRC of each page, or something like that.
     */
    _size -= fsize % PageSize;
}

template<size_t PageSize>
void log_file<PageSize>::append(void* src)
{
    assert<1>(is_open_for_append());

    check_error(::fsync(_fhdl_app));
}

template<size_t PageSize>
void log_file<PageSize>::read(BlockOffset offset, void* dest)
{
    assert<1>(is_open_for_read());

}

template<size_t PageSize>
void log_file<PageSize>::destroy()
{
    close_for_read();
    close_for_append();

    fs::path f = _owner->make_log_name(_num);
    fs::remove(f);
}

} // namespace legacy
} // namespace fineline
