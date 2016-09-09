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

#ifndef FINELINE_RINGBUFFER_H
#define FINELINE_RINGBUFFER_H

#include <atomic>
#include <mutex>
#include <condition_variable>

/**
 * Simple implementation of a circular IO buffer for the Aether-based log buffer.
 *
 * Author: Caetano Sauer
 */
template <class T, size_t Size, class EpochNumberType = uint64_t>
class AsyncRingBuffer
{
public:
    using EpochNumber = EpochNumberType;

    // epoch must start at 1, because 0 is reserved for the "empty" epoch
    static constexpr EpochNumber InitialEpoch = 1;

    AsyncRingBuffer() : begin(InitialEpoch), end(InitialEpoch)
    {
        referenced.fill(false);
    }

    std::shared_ptr<T> produce(EpochNumber& epoch)
    {
        std::unique_lock<std::mutex> lck {mutex};
        epoch = end++;
        cond.wait(lck, [this,epoch]{ return !isFull() && !isReferenced(epoch); });
        return allocate_ptr(epoch);
    }

    std::shared_ptr<T> consume(EpochNumber& epoch)
    {
        std::unique_lock<std::mutex> lck {mutex};
        cond.wait(lck, [this]{ return shutdown_ || (!isEmpty() && !isReferenced(begin)); });
        if (shutdown_) { return std::shared_ptr<T>{nullptr}; }
        epoch = begin++;
        return allocate_ptr(epoch);
    }

    void shutdown() {
        std::unique_lock<std::mutex> lck {mutex};
        shutdown_ = true;
        cond.notify_all();
    }

private:
    std::array<T, Size> buf;
    std::array<bool, Size> referenced;
    EpochNumber begin; // inclusive
    EpochNumber end; // exclusive

    std::mutex mutex;
    std::condition_variable cond;
    std::atomic<bool> shutdown_;

    std::shared_ptr<T> allocate_ptr(EpochNumber& cnt)
    {
        auto deleter = [this,cnt] (T*)
        {
            std::unique_lock<std::mutex> lck {mutex};
            referenced[cnt % Size] = false;
            cond.notify_one();
        };

        referenced[cnt % Size] = true;
        cond.notify_one();

        return std::shared_ptr<T> { &buf[cnt % Size], deleter };
    }

    bool isFull() { return end - begin >= Size; }
    bool isEmpty() { return end == begin; }
    bool isReferenced(EpochNumber cnt) { return referenced[cnt % Size]; }
};

#endif
