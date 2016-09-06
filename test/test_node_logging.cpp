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
#include "kv_array.h"
#include "node.h"
#include "pointers.h"
#include "fineline.h"

constexpr size_t DftArrayBytes = 8192;
constexpr size_t DftAlignment = 8;

template<class PMNK_Type>
using SArray = foster::SlotArray<PMNK_Type, DftArrayBytes, DftAlignment>;

template<class K, class V>
using KVArray = foster::KeyValueArray<K, V,
      SArray<uint16_t>,
      foster::BinarySearch<SArray<uint16_t>>,
      foster::DefaultEncoder<K, V, uint16_t>,
      fineline::DftLogger
>;

template<class K, class V>
using BTNode = foster::BtreeNode<K, V,
    KVArray,
    foster::PlainPtr
>;

TEST(TestInsertions, SimpleInsertions)
{
    fineline::DftTxnContext ctx;

    BTNode<string, string> node;
    ASSERT_TRUE(node.is_low_key_infinity());
    ASSERT_TRUE(node.is_high_key_infinity());
    node.insert("key", "value");
    node.insert("key2", "value_2");
    node.insert("key0", "value__0");
    node.insert("key1", "value___1");
    node.insert("key3", "value____3");
    ASSERT_TRUE(node.is_sorted());

    string v;
    bool found;

    found = node.find("key0", &v);
    ASSERT_TRUE(found);
    ASSERT_EQ(v, "value__0");
}

TEST(TestSplit, SimpleSplit)
{
    fineline::DftTxnContext ctx;

    using NodePointer = BTNode<string, string>::NodePointer;

    BTNode<string, string> node;
    node.insert("key2", "value_2");
    node.insert("key0", "value__0");
    node.insert("key1", "value___1");
    node.insert("key3", "value____3");
    node.insert("key4", "value_____4");
    node.insert("key5", "value______5");

    BTNode<string, string> node2;

    node.add_foster_child(NodePointer(&node2));
    ASSERT_TRUE(node.is_low_key_infinity());
    ASSERT_TRUE(node.is_high_key_infinity());
    ASSERT_TRUE(node2.is_low_key_infinity());
    ASSERT_TRUE(node2.is_high_key_infinity());
    ASSERT_TRUE(node.get_foster_child() == NodePointer(&node2));

    node.rebalance_foster_child<NodePointer>();
    EXPECT_TRUE(!node.is_foster_empty());
    string key;
    node.get_foster_key(&key);
    EXPECT_EQ(key, "key3");
    ASSERT_TRUE(node.is_low_key_infinity());
    ASSERT_TRUE(node.is_high_key_infinity());

    node2.get_fence_keys(&key, nullptr);
    ASSERT_TRUE(key == "key3");
    ASSERT_TRUE(node2.is_high_key_infinity());

    node2.insert("key6", "value_______6");
    BTNode<string, string> node3;
    node2.add_foster_child(NodePointer(&node3));
    node2.rebalance_foster_child<NodePointer>();

    BTNode<string, string> node4;
    node2.add_foster_child(NodePointer(&node4));
    node2.rebalance_foster_child<NodePointer>();
}

TEST(TestRedo, SimpleInsertionRedo)
{
    fineline::DftTxnContext ctx;

    BTNode<string, string> node;
    node.insert("key2", "value2");
    node.insert("key0", "value0");
    node.insert("key1", "value1");
    node.insert("key3", "value3");

    BTNode<string, string> node_r;

    string v;
    bool found;
    fineline::DftLogrecHeader hdr;
    char* payload;

    auto iter = ctx.get_plog()->iterate();
    while (iter->next(hdr, payload)) {
        auto lr = fineline::ConstructLogRec(hdr.type, node_r, payload);
        // std::cout << "REDOING " << *lr << std::endl;
        lr->redo();
    }

    found = node_r.find("key0", &v);
    ASSERT_TRUE(found); ASSERT_EQ(v, "value0");
    found = node_r.find("key1", &v);
    ASSERT_TRUE(found); ASSERT_EQ(v, "value1");
    found = node_r.find("key2", &v);
    ASSERT_TRUE(found); ASSERT_EQ(v, "value2");
    found = node_r.find("key3", &v);
    ASSERT_TRUE(found); ASSERT_EQ(v, "value3");

    // ctx.commit();
    // fineline::SysEnv::commit_buffer->force_current_page();
}

int main(int argc, char **argv)
{
    ::testing::InitGoogleTest(&argc, argv);
    fineline::init();
    return RUN_ALL_TESTS();
}

