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

#ifndef FINELINE_LEGACY_LOG_SQLITE_H
#define FINELINE_LEGACY_LOG_SQLITE_H

#include <memory>

#include "assertions.h"
#include "log_storage.h"

struct sqlite3;
struct sqlite3_stmt;

namespace fineline {
namespace legacy {

using foster::assert;

class SQLiteIndexedLog
{
public:

    SQLiteIndexedLog(const std::string& path);

    ~SQLiteIndexedLog();

    void insert_block(uint32_t file, uint32_t block, uint64_t partition,
            uint64_t min, uint64_t max);

    sqlite3* get_db() { return db_; }

protected:

    void sql_check(int rc, int expected = 0);
    void connect();
    void disconnect();
    void init();
    void finalize();

private:
    sqlite3* db_;
    sqlite3_stmt* insert_stmt_;
    std::string db_path_;
};

template <class LogPage>
class SQLiteLogAdapter
{
public:
    using LogStorage = log_storage<sizeof(LogPage)>;
    using LogKey = typename LogPage::Key;

    static constexpr unsigned FirstLevelFile = 0;

    SQLiteLogAdapter()
    {
        // TODO: add option mechanism -- most likely boost
        std::string logdir = "logdir";
        bool reformat = true;
        unsigned file_size = 1024;
        unsigned max_files = 0;
        bool delete_old = false;

        storage_.reset(new LogStorage {logdir, reformat, file_size, max_files, delete_old});
        index_.reset(new SQLiteIndexedLog {storage_->get_sqlite_db_path()});
    }

    void append_page(const LogPage& page)
    {
        assert<3>(page.slots_are_sorted());
        LogKey min_key = page.get_slot(0).key;
        LogKey max_key = page.get_slot(page.slot_count() - 1).key;

        auto file = storage_->get_file_for_flush(FirstLevelFile);
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

    std::unique_ptr<LogStorage> storage_;
    std::unique_ptr<SQLiteIndexedLog> index_;
};

} // namespace legacy
} // namespace fineline

#endif
