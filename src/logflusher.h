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

#ifndef FINELINE_LOGFLUSHER_H
#define FINELINE_LOGFLUSHER_H

#include <mutex>
#include <atomic>
#include <thread>
#include <condition_variable>

#include "assertions.h"

namespace fineline {

using foster::assert;

template <
    class LogPage,
    template<class> class Buffer,
    template<class> class PersistentLog
>
class LogFlusher
{
public:
    using EpochNumber = typename Buffer<LogPage>::EpochNumber;

    LogFlusher(std::shared_ptr<Buffer<LogPage>> buffer,
            std::shared_ptr<PersistentLog<LogPage>> log)
        : buffer_(buffer), log_(log), shutdown_(false)
    {
        hardened_epoch_ = buffer->get_current_epoch();
        // Thread runs continuously -- no need for a wakeup/wait mechanism.
        // It will wait on consume method of buffer anyway.
        thread_.reset(new std::thread {&LogFlusher::main_loop, this});
    }

    ~LogFlusher()
    {
        shutdown();
        thread_->join();
    }

    bool wait_until_hardened(EpochNumber epoch)
    {
        std::unique_lock<std::mutex> lck {mutex_};
        auto condition = [this,epoch] { return epoch <= hardened_epoch_ || shutdown_; };
        cond_.wait(lck, condition);

        if (shutdown_) { return false; }
        return true;
    }

    void main_loop()
    {
        while (!shutdown_.load()) {
            EpochNumber epoch {0};
            auto page = buffer_->consume(epoch);
            if (shutdown_.load()) { break; }
            if (!page) { break; }

            page->sort_slots();
            log_->append_page(*page, epoch);
            assert<0>(hardened_epoch_ + 1 == epoch, "Log flusher missed an epoch!");
            hardened_epoch_++;

            // no need to acquire mutex because update is atomic and wait loop uses a timeout
            cond_.notify_all();
        }
        assert<1>(shutdown_);
    }

    void shutdown()
    {
        std::unique_lock<std::mutex> lck {mutex_};
        shutdown_ = true;
        buffer_->shutdown();
        cond_.notify_all();
    }

private:
    std::shared_ptr<Buffer<LogPage>> buffer_;
    std::shared_ptr<PersistentLog<LogPage>> log_;
    std::atomic<EpochNumber> hardened_epoch_;
    std::atomic<bool> shutdown_;

    std::mutex mutex_;
    std::condition_variable cond_;
    std::unique_ptr<std::thread> thread_;
};

} // namespace fineline

#endif

