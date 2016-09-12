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

#ifndef FINELINE_TXNCONTEXT_H
#define FINELINE_TXNCONTEXT_H

#include <stdexcept>
#include <memory>

#include "threadlocal.h"

namespace fineline {

template <class Plog, class Env>
class TxnContext
{
public:
    using SysEnv = Env;
    using LogPage = typename Plog::LogPageType;
    using EpochNumber = typename SysEnv::EpochNumber;

    TxnContext(bool auto_commit = false)
        : auto_commit_(auto_commit), active_(true)
    {}

    ~TxnContext()
    {
        if (active_) {
            if (auto_commit_) { commit(); }
            else { abort(); }
        }
    }

    bool commit()
    {
        // Step 1) Insert log pages into commit buffer
        EpochNumber epoch {0};
        plog_.insert_into_buffer(SysEnv::commit_buffer.get(), epoch);

        // Step 2) Wait for given epoch to be hardened on persistent log
        // TODO this whill hang forever for now, because we don't have a flush daemon running
        bool success = SysEnv::log_flusher->wait_until_hardened(epoch);
        if (!success) { abort(); }
        return success;
    }

    void abort()
    {
        // TODO
    }

    template <class Hdr, class... Args>
    void log(Hdr& hdr, const Args&... args)
    {
        if (!active_) {
            throw std::runtime_error("Cannot log on inactive transaction context");
        }
        plog_.log(hdr, args...);
    }

    Plog* get_plog() { return &plog_; }

protected:

    void finish()
    {
        active_ = false;
    }

    bool auto_commit_;
    bool active_;

    Plog plog_;
};

} // namespace fineline

#endif
