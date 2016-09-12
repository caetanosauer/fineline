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
#include <map>

#include "default_templates.h"
#include "logger.h"
#include "persistent_map.h"

#include "fake_envs.cpp"
using TestEnv = fineline::test::FakeLogEnv;
using TxnContext = fineline::test::FakeTxnContext<TestEnv>;
using TxnLogger = fineline::TxnLogger<TxnContext, fineline::DftLogrecHeader>;

TEST(TestInsertions, SimpleInsertions)
{
    const int LOGGER_ID = 1;
    std::map<string, string> map;
    std::map<string, string> recovered_map;

    {
        TxnLogger logger;
        TxnContext ctx;
        logger.initialize(LOGGER_ID);

        fineline::map::insert(map, logger, "key", "value");
        fineline::map::insert(map, logger, "key2", "value_2");
        fineline::map::insert(map, logger, "key0", "value__0");
        fineline::map::insert(map, logger, "key1", "value___1");
        fineline::map::insert(map, logger, "key3", "value____3");

        ctx.commit();
    }

    {
        TxnLogger logger;
        fineline::map::recover(recovered_map, logger, LOGGER_ID);
    }

    for (auto e : recovered_map) {
        auto it = recovered_map.find(e.first);
        ASSERT_TRUE(it != recovered_map.end());
        ASSERT_EQ(it->second, e.second);
    }
}

int main(int argc, char **argv)
{
    ::testing::InitGoogleTest(&argc, argv);
    fineline::test::init<TestEnv>();
    return RUN_ALL_TESTS();
}

