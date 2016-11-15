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

#ifndef FINELINE_LEGACY_LOG_INDEX_SQLITE_H
#define FINELINE_LEGACY_LOG_INDEX_SQLITE_H

#include <memory>

#include "assertions.h"
#include "log_storage.h"

struct sqlite3;
struct sqlite3_stmt;

namespace fineline {

class Options;

namespace legacy {

using foster::assert;

class SQLiteLogIndex
{
public:

    SQLiteLogIndex(const Options& path);

    ~SQLiteLogIndex();

    void insert_block(
            uint32_t file,
            uint32_t block,
            uint64_t epoch,
            uint64_t min,
            uint64_t max
    );

    class FetchBlockIterator
    {
    public:
        FetchBlockIterator(SQLiteLogIndex* owner, bool forward);
        FetchBlockIterator(SQLiteLogIndex* owner, uint64_t key, bool forward);
        ~FetchBlockIterator();
        bool next(uint32_t& file, uint32_t& block);
    private:
        SQLiteLogIndex* owner_;
        sqlite3_stmt* stmt_;
        bool done_;
    };

    std::unique_ptr<FetchBlockIterator> fetch_blocks(bool forward);
    std::unique_ptr<FetchBlockIterator> fetch_blocks(uint64_t key, bool forward);

    // Used for tests
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
    unsigned max_level_;
};

} // namespace legacy
} // namespace fineline

#endif
