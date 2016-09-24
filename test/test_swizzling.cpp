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

// TODO: we need to have more dummies/mockups so that we don't need so many classes for a simple test!
#include "slot_array.h"
#include "encoding.h"
#include "search.h"
#include "node.h"
#include "node_foster.h"
#include "node_mgr.h"
#include "adoption.h"
#include "latch_mutex.h"
#include "swizzling.h"

#include "fake_envs.cpp"
using TestEnv = fineline::test::FakeLogEnv;
using TxnContext = fineline::test::FakeTxnContext<TestEnv>;
using Logger = fineline::test::FakeLogger<TestEnv>;

constexpr size_t DftArrayBytes = 8192;
constexpr size_t DftAlignment = 8;
using PMNK = uint32_t;
using NodeId = typename Logger::IdType;

using SArray = foster::SlotArray<
    PMNK,
    DftArrayBytes,
    DftAlignment,
    Logger,
    foster::FosterNodePayloads,
    foster::MutexLatch
>;

template<class K, class V>
using BaseNode = foster::Node<K, V,
      foster::BinarySearch<SArray>,
      foster::GetEncoder<PMNK>::template type
>;

template<class K, class V>
using FosterNode = foster::FosterNode<K, V,
      BaseNode,
      foster::AssignmentEncoder,
      foster::MutexLatch
>;

struct FakeFetcher
{
    using IdType = NodeId;
    static SArray* fetch(NodeId id) { return fake_fetcher_nodes[id]; }
    static void free(SArray* n) { fake_fetcher_nodes[n->id()] = n; }
    static std::map<NodeId, SArray*> fake_fetcher_nodes;
};

std::map<NodeId, SArray*> FakeFetcher::fake_fetcher_nodes;

using Ptr = fineline::SwizzablePtr<SArray, FakeFetcher>;
using Leaf = FosterNode<string, string>;
using Branch = FosterNode<string, Ptr>;

using NodeMgr = foster::BtreeNodeManager<Ptr>;
using Adoption = foster::EagerAdoption<Branch, NodeMgr>;

TEST(TestSwizzling, PointerTest)
{
    SArray node;
    Ptr ptr {&node};
    auto id = node.id();

    // pointer is by default swizzled -- unswizzle it first and check
    EXPECT_TRUE(ptr.is_swizzled());
    ptr.unswizzle(id);
    EXPECT_TRUE(!ptr.is_swizzled());

    ptr.swizzle();
    EXPECT_EQ(ptr.get(), &node);

    // dereferencing an unswizzled pointer should re-swizzle it
    ptr.unswizzle(id);
    ptr->slot_count();
    EXPECT_TRUE(ptr.is_swizzled());
}

TEST(TestSwizzling, ChildTest)
{
    TxnContext ctx;
    constexpr NodeId PARENT_ID = 0;
    constexpr NodeId CHILD_ID = 1;

    // Build a parent node pointing to a single child with an arbitrary key
    SArray parent;
    Ptr parent_ptr {&parent};
    Logger::initialize(parent_ptr, PARENT_ID);
    Branch::initialize(parent_ptr, 1 /* level */);

    SArray child;
    Ptr child_ptr {&child};
    Logger::initialize(child_ptr, CHILD_ID);

    string key = "key";
    Branch::insert(parent_ptr, key, child_ptr);

    // child pointer should be swizzled initially
    Ptr p, p2;
    Branch::find(parent_ptr, key, p);
    EXPECT_TRUE(p.is_swizzled());
    EXPECT_EQ(child_ptr, p);

    // unswizzle and overwrite swizzled pointer
    p.unswizzle(CHILD_ID);
    // TODO implement overwrite
    Branch::remove(parent_ptr, key);
    Branch::insert(parent_ptr, key, p);
    Branch::find(parent_ptr, key, p2);
    EXPECT_TRUE(!p2.is_swizzled());
    EXPECT_EQ(p2, p);

    // traversal should always return swizzled pointer, fetching nodes if necessary
    p2 = Branch::traverse(parent_ptr, key, false, static_cast<Adoption*>(nullptr));
    EXPECT_TRUE(p2.is_swizzled());
    EXPECT_EQ(p2, child_ptr);
}

int main(int argc, char **argv)
{
    ::testing::InitGoogleTest(&argc, argv);
    fineline::test::init<TestEnv>();
    return RUN_ALL_TESTS();
}


