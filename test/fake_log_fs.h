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

#ifndef FINELINE_TEST_FAKE_LOG_FS_H
#define FINELINE_TEST_FAKE_LOG_FS_H

#include <atomic>
#include <memory>
#include <array>
#include <vector>
#include <map>
#include <mutex>
#include <stdexcept>

#include "options.h"
#include "legacy/lsn.h"

namespace fineline {
namespace test {

/**
 * Simple test-and-set spinlock for the fake log file.
 * Idea: we could implement an Aether-based fake log file if this somehow
 * becomes the bottleneck in a stress test.
 */
struct TASLock
{
    TASLock() : flag(ATOMIC_FLAG_INIT) {}
    void lock() { while (flag.test_and_set(std::memory_order_acquire)) {}; }
    void unlock() { flag.clear(std::memory_order_release); }
private:
    std::atomic_flag flag;
};

template <size_t PageSize, size_t MaxLevels>
class LogPageVector
{
public:
    using Page = std::array<char, PageSize>;
    using BlockOffset = uint32_t;
    using FileNumber = legacy::UnsignedNumberPair<16,16>;
    using FileHighNumber = uint16_t;
    using FileLowNumber = uint16_t;

    struct FakeLogFile
    {
        FakeLogFile()
        {
        }

        void read(BlockOffset i, void* dest)
        {
            if (vector_.size() <= i) {
                throw std::runtime_error("Invalid offset");
            }
            std::memcpy(dest, &vector_.data()[i], PageSize);
        }

        BlockOffset append(const void* src)
        {
            std::lock_guard<TASLock> lck(lock_);
            vector_.emplace_back();
            BlockOffset i = vector_.size() - 1;
            std::memcpy(&vector_.data()[i], src, PageSize);

            return i;
        }

        FileNumber num() { return 0; }

        std::vector<Page> vector_;
        TASLock lock_;
    };

    LogPageVector(const Options& opt)
    {
        for (size_t i = 0; i < MaxLevels; i++) {
            files_[i] = std::make_shared<FakeLogFile>();
            // files_[i]->vector_.reserve(initial_size);
        }
    }

    std::shared_ptr<FakeLogFile> get_file_for_flush(FileHighNumber num)
    {
        return get_file(FileNumber{num, 0});
    }

    std::shared_ptr<FakeLogFile> curr_file(FileHighNumber num) const
    {
        return get_file(FileNumber{num, 0});
    }

    std::shared_ptr<FakeLogFile> get_file(FileNumber num)
    {
        return files_[num.hi()];
    }

protected:
    std::array<std::shared_ptr<FakeLogFile>, MaxLevels> files_;
};

class StdMapLogIndex
{
public:
    StdMapLogIndex(const Options&)
    {}

    /*
     * TODO extend this when we implement fetch, iterate, and merge
     */

    void insert_block(
            uint32_t /* file */,
            uint32_t block,
            uint64_t /* partition */,
            uint64_t min,
            uint64_t max
    )
    {
        min_index_[min] = std::make_pair(max, block);
    }

protected:
    std::map<uint64_t, std::pair<uint64_t, uint32_t>> min_index_;
};

} // namespace test
} // namespace fineline

#endif
