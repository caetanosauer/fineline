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

#include <boost/regex.hpp>
#include <cstdio>
#include <sys/types.h>
#include <sys/stat.h>
#include <atomic>
#include <thread>
#include <chrono>

#include "log_storage.h"

namespace fineline {
namespace legacy {

using std::string;
using foster::assert;
using foster::SharedLatchContext;
using foster::ExclusiveLatchContext;

template <class LogStorage>
class file_recycler_t
{
public:
    file_recycler_t(LogStorage* storage)
        : storage(storage), retire(false)
    {
    }

    ~file_recycler_t()
    {
        retire = true;
        if (_thread.get()) {
            wakeup();
            _thread->join();
        }
    }

    void run()
    {
        while (!retire) {
            std::unique_lock<std::mutex> lck(_mutex);
            _cond.wait(lck);
            if (retire.load()) { break; }
            storage->delete_old_files();
        }
    }

    void wakeup()
    {
        std::unique_lock<std::mutex> lck(_mutex);
        if (!_thread.get()) {
            _thread.reset(new std::thread {&file_recycler_t::run, this});
        }
        _cond.notify_one();
    }

    LogStorage* storage;
    std::atomic<bool> retire;
    std::condition_variable _cond;
    std::mutex _mutex;
    std::unique_ptr<std::thread> _thread;
};

/*
 * Opens log files in logdir and initializes partitions as well as the
 * given LSN's. The buffer given in prime_buf is primed with the contents
 * found in the last block of the last partition -- this logic was moved
 * from the various prime methods of the old log_core.
 */
template <size_t P>
log_storage<P>::log_storage(const Options& options)
    : _recycler(this)
{
    string logpath = options.get<string>("logpath");
    if (logpath.empty()) {
        throw std::runtime_error("ERROR: logpath must be set to enable logging.");
    }
    _logpath = logpath;
    _index_file_name = options.get<string>("log_index_path");

    bool reformat = options.get<bool>("format");
    if (!fs::exists(_logpath)) {
        if (reformat) {
            fs::create_directories(_logpath);
        } else {
            auto what = "Error: could not open the log directory " + logpath;
            throw std::runtime_error(what);
        }
    }

    _file_size = options.get<unsigned>("log_file_size");
    // option given in MB -> convert to B
    _file_size *= 1024 * 1024;
    // round to next multiple of the page size
    _file_size = (_file_size / P) * P;

    _max_files = options.get<unsigned>("log_max_files");
    _delete_old_files = options.get<bool>("log_recycle");

    std::map<FileHighNumber, FileLowNumber> last_files;
    fs::directory_iterator it(_logpath), eod;
    boost::regex log_rx(log_regex, boost::regex::basic);

    for (; it != eod; it++) {
        fs::path fpath = it->path();
        string fname = fpath.filename().string();

        if (boost::regex_match(fname, log_rx)) {
            if (reformat) {
                fs::remove(fpath);
                continue;
            }

            std::stringstream ss {fname.substr(string{log_prefix}.length())};
            FileNumber fnum;
            ss >> fnum;

            _files[fnum] = std::make_shared<LogFile>(_logpath, fnum);
            if (last_files.find(fnum.hi()) == last_files.end()
                    || fnum >= last_files[fnum.hi()])
            {
                last_files[fnum.hi()] = fnum.lo();
            }
        }
        else if (fname.substr(0, _index_file_name.length()) == _index_file_name) {
            if (reformat) {
                fs::remove(fpath);
                continue;
            }
            // ignore
        }
        else {
            auto what = "log_storage: cannot parse filename " + fname;
            throw std::runtime_error(what);
        }
    }

    for (auto elem : last_files) {
        auto fnum = FileNumber {elem.first, elem.second};
        auto p = get_file(fnum);
        if (!p) {
            create_file(fnum);
            p = get_file(fnum);
            assert<0>(p.get());
        }
        p->open_for_append();
        _current[elem.first] = p;
    }
}

template <size_t P>
log_storage<P>::~log_storage()
{
    ExclusiveLatchContext cs(&_file_map_latch);

    for (auto elem : _files) {
        auto p = elem.second;
        p->close_for_read();
        p->close_for_append();
    }
    _files.clear();
}

template <size_t PageSize>
std::shared_ptr<log_file<PageSize>> log_storage<PageSize>::get_file_for_flush(FileHighNumber level)
{
    auto p = curr_file(level);
    if (!p.get()) {
        p = create_file(FileNumber{level,1});
        p->open_for_append();
    }
    else if (p->get_size() + PageSize > _file_size) {
        auto n = p->num();
        p->close_for_append();
        p = create_file(n.advance());
        p->open_for_append();
    }

    return p;
}

template <size_t P>
std::shared_ptr<log_file<P>> log_storage<P>::get_file(FileNumber n) const
{
    SharedLatchContext cs(&_file_map_latch);
    auto it = _files.find(n);
    if (it == _files.end()) { return nullptr; }
    return it->second;
}

template <size_t P>
std::shared_ptr<log_file<P>> log_storage<P>::create_file(FileNumber fnum)
{
    auto p = get_file(fnum);
    if (p) {
        auto what = "Partition " + fnum.str() + " already exists";
        throw std::runtime_error(what);
    }

    p = std::make_shared<LogFile>(_logpath, fnum);
    p->set_size(0);

    {
        // Add partition to map but only exit function once it has been
        // reduced to _max_files
        ExclusiveLatchContext cs(&_file_map_latch);
        _current[fnum.hi()] = p;
        _files[fnum] = p;
    }

    wakeup_recycler();

    // The check below does not require the mutex
    if (_max_files > 0 && _files.size() > _max_files) {
        // Log full! Try to clean-up old files
        try_delete();
    }

    return p;
}

template <size_t P>
void log_storage<P>::wakeup_recycler()
{
    if (!_delete_old_files) { return; }
    _recycler.wakeup();
}

template <size_t P>
unsigned log_storage<P>::delete_old_files()
{
    if (!_delete_old_files) { return 0; }
    return 0;
    // TODO implement!

    // if (older_than == 0 && smlevel_0::chkpt) {
    //     lsn_t min_lsn = smlevel_0::chkpt->get_min_active_lsn();
    //     older_than = min_lsn.is_null() ? smlevel_0::log->durable_lsn().hi() : min_lsn.hi();
    //     if (smlevel_0::logArchiver) {
    //         lsn_t lastArchivedLSN = smlevel_0::logArchiver->getDirectory()->getLastLSN();
    //         if (older_than > lastArchivedLSN.hi()) {
    //             older_than = lastArchivedLSN.hi();
    //         }
    //     }
    // }

    // list<std::shared_ptr<LogFile>> to_be_deleted;

    // {
    //     ExclusiveLatchContext cs(&_file_map_latch);

    //     partition_map_t::iterator it = _files.begin();
    //     while (it != _files.end()) {
    //         if (it->first < older_than) {
    //             to_be_deleted.push_front(it->second);
    //             it = _files.erase(it);
    //         }
    //         else { it++; }
    //     }
    // }

    // // Waint until the partitions to be deleted are not referenced anymore
    // while (to_be_deleted.size() > 0) {
    //     auto p = to_be_deleted.front();
    //     to_be_deleted.pop_front();
    //     while (!p.unique()) {
    //         std::this_thread::sleep_for(chrono::milliseconds(1));
    //     }
    //     // Now this partition is owned exclusively by me.  Other threads cannot
    //     // increment reference counters because objects were removed from map,
    //     // and the critical section above guarantees visibility.
    //     p->destroy();
    // }

    // return to_be_deleted.size();
}

template <size_t P>
std::shared_ptr<log_file<P>> log_storage<P>::curr_file(FileHighNumber level) const
{
    SharedLatchContext cs(&_file_map_latch);
    auto it = _current.find(level);
    if (it == _current.end()) { return nullptr; }
    return it->second;
}

template <size_t P>
void log_storage<P>::try_delete()
{
    // TODO implement!
    unsigned deleted = delete_old_files();
    if (deleted == 0) {
        throw std::runtime_error("Log wedged! Cannot recycle partitions");
    }
}

} // namespace legacy
} // namespace fineline
