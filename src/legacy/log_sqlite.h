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

#include "assertions.h"

struct sqlite3;
struct sqlite3_stmt;

namespace fineline {
namespace legacy {

class SQLiteIndexedLog
{
public:

    SQLiteIndexedLog(const std::string& path);

    ~SQLiteIndexedLog();

    void insert_block(int file, int block, int min, int max);

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

} // namespace legacy
} // namespace fineline

#endif