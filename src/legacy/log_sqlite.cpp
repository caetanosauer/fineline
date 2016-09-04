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

#include "log_sqlite.h"

namespace fineline {
namespace legacy {

using foster::assert;

const char* SQLiteIndexedLogDDL =
    "create table if not exists logblocks ("
    "   file_number int,"
    "   block_number int,"
    "   min_key int,"
    "   max_key int,"
    "   merge_depth int,"
    "   bloom_filter blob(1024),"
    "   primary key(file_number, block_number)"
    ");"
    "create index if not exists nodeid_idx on logblocks ("
    "   min_key, max_key desc);"
;

const char* SQLiteQueryInsertBlock =
    "insert into logblocks values (?,?,?,?,?,NULL)";

SQLiteIndexedLog::SQLiteIndexedLog(const std::string& path)
    : db_(nullptr), db_path_(path)
{
    connect();
    init();
}

SQLiteIndexedLog::~SQLiteIndexedLog()
{
    disconnect();
}

void SQLiteIndexedLog::sql_check(int rc, int expected)
{
    if (rc != expected) {
        disconnect();
        throw std::runtime_error("Error executing SQLite3 function");
    }
}

void SQLiteIndexedLog::connect()
{
    if (!db_) {
        sql_check(sqlite3_open(db_path_.c_str(), &db_));
    }
}

void SQLiteIndexedLog::disconnect()
{
    sqlite3_close(db_);
}

void SQLiteIndexedLog::init()
{
    sql_check(sqlite3_exec(db_, SQLiteIndexedLogDDL, 0, 0, 0));
    sql_check(sqlite3_prepare_v2(db_, SQLiteQueryInsertBlock, -1, &insert_stmt_, 0));
}

void SQLiteIndexedLog::finalize()
{
    sqlite3_finalize(insert_stmt_);
}

void SQLiteIndexedLog::insert_block(int file, int block, int min, int max)
{
    sql_check(sqlite3_reset(insert_stmt_));

    sql_check(sqlite3_bind_int(insert_stmt_, 1, file));
    sql_check(sqlite3_bind_int(insert_stmt_, 2, block));
    sql_check(sqlite3_bind_int(insert_stmt_, 3, min));
    sql_check(sqlite3_bind_int(insert_stmt_, 4, max));
    sql_check(sqlite3_bind_int(insert_stmt_, 5, 0));

    int rc = SQLITE_BUSY;
    while (rc == SQLITE_BUSY) {
        rc = sqlite3_step(insert_stmt_);
    }
    sql_check(rc, SQLITE_DONE);
}

} // namespace legacy
} // namespace fineline
