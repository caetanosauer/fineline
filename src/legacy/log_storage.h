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
 * (c) Copyright 2011-2014, Hewlett-Packard Development Company, LP
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

 $Id: log_core.h,v 1.11 2010/09/21 14:26:19 nhall Exp $

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

#ifndef FINELINE_LEGACY_LOG_STORAGE_H
#define FINELINE_LEGACY_LOG_STORAGE_H

#include <map>
#include <vector>
#include <memory>
#include <mutex>
#include <condition_variable>

#define BOOST_FILESYSTEM_NO_DEPRECATED
#include <boost/filesystem.hpp>
namespace fs = boost::filesystem;

#include "log_file.h"
#include "latch_mutex.h"

namespace fineline {
namespace legacy {

template <class> class file_recycler_t;

template <size_t PageSize>
class log_storage {

    friend class file_recycler_t<log_storage<PageSize>>;

public:
    using LogFile = log_file<PageSize>;
    using FileNumber = typename LogFile::FileNumber;
    using FileHighNumber = typename FileNumber::HighType;
    using FileLowNumber = typename FileNumber::LowType;
    using FileMap = std::map<FileNumber, std::shared_ptr<LogFile>>;
    using CurrentFileMap = std::map<FileHighNumber, std::shared_ptr<LogFile>>;

    log_storage(std::string logdir, bool reformat, bool file_size, unsigned max_files,
        bool delete_old_files);
    virtual ~log_storage();

    std::shared_ptr<LogFile> get_file_for_flush(FileHighNumber);
    std::shared_ptr<LogFile> curr_file(FileHighNumber) const;
    std::shared_ptr<LogFile> get_file(FileNumber n) const;

    string make_log_name(FileNumber fnum) const;
    fs::path make_log_path(FileNumber fnum) const;

    void wakeup_recycler();
    unsigned delete_old_files();

    size_t get_file_size() const { return _file_size; }

private:
    std::shared_ptr<LogFile> create_file(FileNumber pnum);

    fs::path _logpath;
    size_t _file_size;

    FileMap _files;
    CurrentFileMap _current;

    unsigned _max_files;
    bool _delete_old_files;

    // forbid copy
    log_storage<PageSize>(const log_storage<PageSize>&);
    log_storage<PageSize>& operator=(const log_storage<PageSize>&);

    void try_delete();

    // Latch to protect access to partition map
    foster::MutexLatch _file_map_latch;

    std::unique_ptr<file_recycler_t<log_storage<PageSize>>> _recycler_thread;

public:
    static const string log_prefix;
    static const string log_regex;
};

} // namespace legacy
} // namespace fineline

#endif
