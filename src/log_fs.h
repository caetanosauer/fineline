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
    using ThisType = FileBasedLog<LogPage, LogIndex, LogFileSystem>;

    FileBasedLog(const Options& options)
    {
        // FS should be initialized first, because index path may be relative to it
        fs_.reset(new LogFileSystem<PageSize>{options});
        index_.reset(new LogIndex{options});
    }

    template <class EpochNumber>
    void append_page(const LogPage& page, EpochNumber epoch)
    {
        assert<1>(page.slot_count() > 0);
        assert<3>(page.slots_are_sorted());

        LogKey min_key = page.get_slot(0).key;
        LogKey max_key = page.get_slot(page.slot_count() - 1).key;

        assert<3>(max_key >= min_key);
        assert<3>(max_key.node_id >= min_key.node_id);

        auto file = fs_->get_file_for_flush(FirstLevelFile);
        size_t offset = file->append(&page);
        index_->insert_block(file->num().data(), offset, epoch, min_key.node_id, max_key.node_id);
    }

    using LogPageIterator = typename LogPage::Iterator;
    using FetchBlockIterator = typename LogIndex::FetchBlockIterator;

    class LogFileIterator
    {
    public:
        LogFileIterator(ThisType* log, uint64_t key, bool forward = true)
            : log_(log), queried_key_(key), forward_(forward),
            block_index_iter_ {std::move(log->index_->fetch_blocks(key, forward))}
        {
            next_block();
        }

        bool next(LogKey& key, char*& payload)
        {
            bool has_more = true;
            while (has_more) {
                if (!page_iter_.get()) { return false; }
                has_more = page_iter_->next(key, payload);
                if (!has_more) {
                    has_more = next_block();
                    if (!has_more) { return false; }
                    has_more = page_iter_->next(key, payload);
                }
                if (has_more && queried_key_ == key.node_id) {
                    return true;
                }
            }
            return has_more;
        }

    protected:
        bool next_block()
        {
            uint32_t file;
            uint32_t block;
            bool has_more = block_index_iter_->next(file, block);
            if (!has_more) { return false; }

            log_->fs_->get_file(file)->read(block, &page_);
            page_iter_ = std::move(page_.iterate(forward_));

            return true;
        }

    private:
        LogPage page_;
        ThisType* log_;
        uint64_t queried_key_;
        bool forward_;
        std::unique_ptr<LogPageIterator> page_iter_;
        std::unique_ptr<FetchBlockIterator> block_index_iter_;
    };

    std::unique_ptr<LogFileIterator> fetch(uint64_t key, bool forward = true)
    {
        return std::unique_ptr<LogFileIterator>{new LogFileIterator{this, key, forward}};
    }

private:

    std::unique_ptr<LogFileSystem<PageSize>> fs_;
    std::unique_ptr<LogIndex> index_;
};

} // namespace fineline

#endif

