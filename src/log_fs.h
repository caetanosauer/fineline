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

#ifndef FINELINE_LOG_FS_H
#define FINELINE_LOG_FS_H

#include <memory>

#include "assertions.h"
#include "options.h"

namespace fineline {

using foster::assert;

template <
    class LogPage,
    class LogIndex,
    template <size_t> class LogFileSystem
>
class FileBasedLog
{
public:
    static constexpr unsigned FirstLevelFile = 0;
    static constexpr size_t PageSize = sizeof(LogPage);

    using LogKey = typename LogPage::Key;

    FileBasedLog(const Options& options)
    {
        // FS should be initialized first, because index path may be relative to it
        fs_.reset(new LogFileSystem<PageSize>{options});
        index_.reset(new LogIndex{options});
    }

    void append_page(const LogPage& page)
    {
        assert<3>(page.slots_are_sorted());
        LogKey min_key = page.get_slot(0).key;
        LogKey max_key = page.get_slot(page.slot_count() - 1).key;

        auto file = fs_->get_file_for_flush(FirstLevelFile);
        // TODO we may crash after appending page but before inserting index info!
        size_t offset = file->append(&page);
        // TODO we need a 64-bit PartitionNumber type
        // with 8-bit merge depth and 56-bit partition number
        // for depth=0, partition number is simple file_number (24-bit, FileNumber.lo())
        // and block_number (32-bit)
        uint64_t partition = (uint64_t(file->num().data()) << 32) & offset;
        index_->insert_block(file->num().data(), offset, partition, min_key.node_id, max_key.node_id);
    }

private:

    std::unique_ptr<LogFileSystem<PageSize>> fs_;
    std::unique_ptr<LogIndex> index_;
};

} // namespace fineline

#endif

