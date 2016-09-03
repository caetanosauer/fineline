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
#include <condition_variable>
#include <atomic>

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
    static constexpr unsigned WaitTimeoutMicrosec = 1000;

    LogFlusher(std::shared_ptr<Buffer<LogPage>> buffer,
            std::shared_ptr<PersistentLog<LogPage>> log)
        : buffer_(buffer), log_(log), hardened_epoch_(0), shutdown_(false)
    {
    }

    bool wait_until_hardened(EpochNumber epoch)
    {
        std::unique_lock<std::mutex> lck {mutex_};
        auto condition = [this,epoch] {
            return epoch <= hardened_epoch_.load() || shutdown_.load();
        };
        auto timeout = std::chrono::microseconds(unsigned(WaitTimeoutMicrosec));
        while (cond_.wait_for(lck, timeout, condition)) {};

        if (shutdown_.load()) { return false; }
        return true;
    }

    void main_loop()
    {
        while (!shutdown_.load()) {
            EpochNumber epoch {0};
            auto page = buffer_->consume(epoch);

            log_->append_page(page);

            assert<0>(hardened_epoch_ + 1 == epoch, "Log flusher missed an epoch!");
            hardened_epoch_++;

            // no need to acquire mutex because update is atomic and wait loop uses a timeout
            cond_.notify_all();
        }
    }

    void shutdown()
    {
        shutdown_ = true;
    }

protected:

private:
    std::shared_ptr<Buffer<LogPage>> buffer_;
    std::shared_ptr<PersistentLog<LogPage>> log_;
    std::atomic<EpochNumber> hardened_epoch_;
    std::atomic<bool> shutdown_;

    std::mutex mutex_;
    std::condition_variable cond_;
};

} // namespace fineline

#endif

