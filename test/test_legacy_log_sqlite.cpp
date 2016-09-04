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

#define ENABLE_TESTING

#include <gtest/gtest.h>
#include <cstring>
#include <sqlite3.h>

#include "fixture_tempfile.h"
#include "legacy/log_sqlite.h"

class TestSQLite : public fineline::test::TmpDirFixture
{
protected:
    static constexpr char DBFile[] = "test.db";

    virtual void SetUp()
    {
        fineline::test::TmpDirFixture::SetUp();
        log_ = new fineline::legacy::SQLiteIndexedLog {get_full_path(DBFile)};
    }

    virtual void TearDown()
    {
        delete log_;
        fineline::test::TmpDirFixture::TearDown();
    }

    fineline::legacy::SQLiteIndexedLog* log_;
};

constexpr char TestSQLite::DBFile[];

TEST_F(TestSQLite, InsertionTest)
{
    std::array<std::tuple<int, int, int, int>, 3> tuples;
    tuples[0] = std::make_tuple(1, 1, 10, 20);
    tuples[1] = std::make_tuple(1, 2, 15, 25);
    tuples[2] = std::make_tuple(2, 1, 10, 30);

    for (auto t : tuples) {
        log_->insert_block(std::get<0>(t), std::get<1>(t), std::get<2>(t), std::get<3>(t));
    }

    auto db = log_->get_db();

    int rc;
    sqlite3_stmt* stmt;
    rc = sqlite3_prepare_v2(db,
            "select file_number, block_number, min_key, max_key "
            "from logblocks order by file_number, block_number",
            -1, &stmt, 0);
    ASSERT_EQ(rc, SQLITE_OK);

    unsigned i = 0;
    do {
        rc = sqlite3_step(stmt);
        if (rc == SQLITE_ROW) {
            EXPECT_EQ(sqlite3_column_int(stmt, 0), std::get<0>(tuples[i]));
            EXPECT_EQ(sqlite3_column_int(stmt, 1), std::get<1>(tuples[i]));
            EXPECT_EQ(sqlite3_column_int(stmt, 2), std::get<2>(tuples[i]));
            EXPECT_EQ(sqlite3_column_int(stmt, 3), std::get<3>(tuples[i]));
            i++;
        }
    } while (rc == SQLITE_ROW || rc == SQLITE_BUSY);

    ASSERT_EQ(rc, SQLITE_DONE);
    EXPECT_EQ(i, 3);
}

int main(int argc, char **argv)
{
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

