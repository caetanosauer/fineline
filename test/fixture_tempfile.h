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

#ifndef FINELINE_TEST_FIXTURE_TEMPFILE_H
#define FINELINE_TEST_FIXTURE_TEMPFILE_H

#include <gtest/gtest.h>

#define BOOST_FILESYSTEM_NO_DEPRECATED
#include <boost/filesystem.hpp>
namespace fs = boost::filesystem;

namespace fineline {
namespace test {

class TmpDirFixture : public ::testing::Test {
public:
    std::string get_full_path(std::string fname)
    {
        return (tmpdir_ / fname).string();
    }

protected:
    virtual void SetUp()
    {
        tmpdir_ = fs::temp_directory_path() / fs::unique_path();
        fs::create_directory(tmpdir_);
    }

    virtual void TearDown()
    {
        fs::remove_all(tmpdir_);
    }

private:
    fs::path tmpdir_;
};

} // namespace test
} // namespace fineline

#endif
