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

#ifndef FINELINE_BENCH_CONTAINER_H
#define FINELINE_BENCH_CONTAINER_H

#include "persistent_map.h"
// #include "foster-btree/src/default_templates.h"

namespace fineline {

template <
    class Map,
    class Logger
>
class PersistentMap
{
public:

    void initialize(unsigned id)
    {
        logger_.initialize(id);
        map_.clear();
    }

    template <typename... Args>
    void put(Args... args)
    {
        map::insert(map_, logger_, args...);
    }

    template <typename K, typename V>
    void get(const K& key, V& value)
    {
        value = map_[key];
    }

protected:
    Logger logger_;
    Map map_;
};

// constexpr size_t DftArrayBytes = 8192;
// constexpr size_t DftAlignment = 8;
// constexpr size_t DftNumLevels = 3;

// template<class Node>
// using NodeMgr = foster::BtreeNodeManager<Node, foster::AtomicCounterIdGenerator<unsigned>>;

// template <class K, class V>
//                 foster::KeyValueArray<K, V,
//                       foster::SlotArray<K, DftArrayBytes, DftAlignment>,
//                       foster::BinarySearch<foster::SlotArray<K, DftArrayBytes, DftAlignment>>,
//                       foster::DefaultEncoder<K, V, K>,
//                       Logger
//                 >,

// // TODO: we have to simplify this stuff, for instance, by removing all K,V arguments
// template<class K, class V, class Logger>
// using FosterBtree =
//     foster::StaticBtree<K, V, DftNumLevels,
//         foster::BtreeLevel<
//             K, V, DftNumLevels,
//             foster::BtreeNode<K, V,
//                 foster::PlainPtr,
//                 foster::MutexLatch
//             >,
//             foster::EagerAdoption,
//             NodeMgr
//         >
// >;


} // namespace fineline

#endif
