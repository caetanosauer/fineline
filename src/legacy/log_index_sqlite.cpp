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

#include <sqlite3.h>
#include <stdexcept>

#include "log_index_sqlite.h"

#define BOOST_FILESYSTEM_NO_DEPRECATED
#include <boost/filesystem.hpp>
namespace fs = boost::filesystem;

namespace fineline {
namespace legacy {

using foster::assert;

const auto CreateTablesQuery =
    "create table if not exists logblocks ("
    "   file_number int,"
    "   block_number int,"
    "   part_number unsigned big int,"
    "   min_key int,"
    "   max_key int,"
    "   bloom_filter blob(1024),"
    "   primary key(file_number, block_number)"
    ");"
    "create index if not exists nodeid_idx on logblocks ("
    "   min_key, max_key desc);"
;

const auto InsertBlockQuery =
    "insert into logblocks values (?,?,?,?,?,NULL)";

const auto FetchHistoryQuery =
    "select part_number, file_number, block_number "
    "from logblocks "
    "where ? >= min_key and ? <= max_key "
    "order by part_number desc, file_number desc, block_number desc"
;

SQLiteLogIndex::SQLiteLogIndex(const Options& options)
    : db_(nullptr)
{
    db_path_ = options.get<string>("log_index_path");
    if (options.get<bool>("log_index_path_relative")) {
        auto path = fs::path{options.get<string>("logpath")} / db_path_;
        db_path_ = path.string();
    }
    connect();
    init();
}

SQLiteLogIndex::~SQLiteLogIndex()
{
    disconnect();
}

void SQLiteLogIndex::sql_check(int rc, int expected)
{
    if (rc != expected) {
        disconnect();
        throw std::runtime_error("Error executing SQLite3 function");
    }
}

void SQLiteLogIndex::connect()
{
    if (!db_) {
        sql_check(sqlite3_open(db_path_.c_str(), &db_));
    }
}

void SQLiteLogIndex::disconnect()
{
    if (db_) {
        sqlite3_close(db_);
    }
}

void SQLiteLogIndex::init()
{
    sql_check(sqlite3_exec(db_, CreateTablesQuery, 0, 0, 0));
    sql_check(sqlite3_prepare_v2(db_, InsertBlockQuery, -1, &insert_stmt_, 0));
}

void SQLiteLogIndex::finalize()
{
    sqlite3_finalize(insert_stmt_);
}

void SQLiteLogIndex::insert_block(uint32_t file, uint32_t block, uint64_t partition,
        uint64_t min, uint64_t max)
{
    sql_check(sqlite3_reset(insert_stmt_));

    sql_check(sqlite3_bind_int(insert_stmt_, 1, file));
    sql_check(sqlite3_bind_int(insert_stmt_, 2, block));
    sql_check(sqlite3_bind_int(insert_stmt_, 3, partition));
    sql_check(sqlite3_bind_int(insert_stmt_, 4, min));
    sql_check(sqlite3_bind_int(insert_stmt_, 5, max));

    int rc = SQLITE_BUSY;
    while (rc == SQLITE_BUSY) {
        rc = sqlite3_step(insert_stmt_);
    }
    sql_check(rc, SQLITE_DONE);
}

std::shared_ptr<SQLiteLogIndex::FetchBlockIterator> SQLiteLogIndex::fetch_blocks(uint64_t key)
{
    return std::make_shared<SQLiteLogIndex::FetchBlockIterator>(this, key);
}

SQLiteLogIndex::FetchBlockIterator::FetchBlockIterator(SQLiteLogIndex* owner, uint64_t key)
{
    owner_ = owner;
    done_ = false;
    owner_->sql_check(sqlite3_prepare_v2(owner_->db_, FetchHistoryQuery, -1, &stmt_, 0));
    owner_->sql_check(sqlite3_bind_int(stmt_, 1, key));
    owner_->sql_check(sqlite3_bind_int(stmt_, 2, key));
}

SQLiteLogIndex::FetchBlockIterator::~FetchBlockIterator()
{
    owner_->sql_check(sqlite3_finalize(stmt_));
}

bool SQLiteLogIndex::FetchBlockIterator::next(uint64_t& partition, uint32_t& file, uint32_t& block)
{
    if (done_) { return false; }

    int rc = SQLITE_BUSY;
    while (rc == SQLITE_BUSY) {
        rc = sqlite3_step(stmt_);
    }
    if (rc == SQLITE_DONE) {
        done_ = true;
        return false;
    }
    else if (rc == SQLITE_ROW) {
        partition = sqlite3_column_int64(stmt_, 1);
        file = sqlite3_column_int(stmt_, 1);
        block = sqlite3_column_int(stmt_, 2);
    }
    else {
        owner_->sql_check(rc);
        return false;
    }
    return true;
}

} // namespace legacy
} // namespace fineline
