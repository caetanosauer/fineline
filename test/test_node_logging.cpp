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

#include "slot_array.h"
#include "encoding.h"
#include "search.h"
#include "node.h"
#include "pointers.h"

#include "fake_envs.cpp"
using TestEnv = fineline::test::FakeLogEnv;
using TxnContext = fineline::test::FakeTxnContext<TestEnv>;

constexpr size_t DftArrayBytes = 8192;
constexpr size_t DftAlignment = 8;
using PMNK = uint32_t;

using SArray = foster::SlotArray<
    PMNK,
    DftArrayBytes,
    DftAlignment,
    fineline::test::FakeLogger<TestEnv>
>;

template<class K, class V>
using BaseNode = foster::Node<K, V,
      foster::BinarySearch,
      foster::GetEncoder<PMNK>::template type,
      fineline::test::FakeLogger<TestEnv>
>;

// template<class K, class V>
// using Node = foster::FosterNode<K, V,
//       BaseNode,
//       foster::AssignmentEncoder
// >;

using N = BaseNode<string, string>;
using Ptr = foster::PlainPtr<SArray>;

TEST(TestInsertions, SimpleInsertions)
{
    TxnContext ctx;

    SArray n; Ptr node {&n};
    N::insert(node, "key", "value");
    N::insert(node, "key2", "value_2");
    N::insert(node, "key0", "value__0");
    N::insert(node, "key1", "value___1");
    N::insert(node, "key3", "value____3");
    ASSERT_TRUE(N::is_sorted(node));

    string v;
    bool found;
    found = N::find(node, "key0", v);
    ASSERT_TRUE(found);
    ASSERT_EQ(v, "value__0");
}

TEST(TestRedo, SimpleInsertionRedo)
{
    TxnContext ctx;

    SArray n; Ptr node {&n};
    N::insert(node, "key2", "value2");
    N::insert(node, "key0", "value0");
    N::insert(node, "key1", "value1");
    N::insert(node, "key3", "value3");

    SArray n_r; Ptr node_r {&n_r};

    string v;
    bool found;
    fineline::DftLogrecHeader hdr;
    char* payload;

    auto iter = ctx.get_plog()->iterate();
    while (iter->next(hdr, payload)) {
        auto lr = fineline::ConstructLogRec<N, Ptr>(hdr.type(), node_r, payload);
        // std::cout << "REDOING " << *lr << std::endl;
        lr->redo();
    }

    found = N::find(node_r, "key0", v);
    ASSERT_TRUE(found); ASSERT_EQ(v, "value0");
    found = N::find(node_r, "key1", v);
    ASSERT_TRUE(found); ASSERT_EQ(v, "value1");
    found = N::find(node_r, "key2", v);
    ASSERT_TRUE(found); ASSERT_EQ(v, "value2");
    found = N::find(node_r, "key3", v);
    ASSERT_TRUE(found); ASSERT_EQ(v, "value3");

    // ctx.commit();
    // fineline::SysEnv::commit_buffer->force_current_page();
}

int main(int argc, char **argv)
{
    ::testing::InitGoogleTest(&argc, argv);
    fineline::test::init<TestEnv>();
    return RUN_ALL_TESTS();
}

