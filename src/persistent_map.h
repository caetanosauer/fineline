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

#ifndef FINELINE_PERSISTENT_MAP_H
#define FINELINE_PERSISTENT_MAP_H

namespace fineline {
namespace map {

template <
    class Map,
    class Logger
>
void insert(Map& map, Logger& logger,
        const typename Map::key_type& key,
        const typename Map::mapped_type& value)
{
    map.insert(std::make_pair(key, value));
    logger.log(LRType::Insert, key, value);
}

template <
    class Map,
    class LogrecHeader
>
void redo(Map& map, const LogrecHeader& hdr, char* payload)
{
    using K = typename Map::key_type;
    using V = typename Map::mapped_type;

    K key;
    V value;
    LogEncoder<K, V>::decode(payload, &key, &value);

    switch (hdr.type) {
        case LRType::Insert:
            map.insert(std::make_pair(key, value));
            break;
        case LRType::Construct:
            // nothing to do
            break;
        default:
            auto what = "I don't know how to redo this log record";
            throw std::runtime_error(what);
    }
}

template <
    class Map,
    class Logger
>
void recover(Map& map, Logger&, typename Logger::IdType id)
{
    // TODO: This is where it gets interesting!
    auto log = Logger::SysEnv::log;
    auto iter = log->fetch(id);

    typename Logger::LogrecHeader hdr;
    char* payload;

    while (iter->next(hdr, payload)) {
        redo(map, hdr, payload);
    }
}

} // namespace fineline
} // namespace fineline

#endif
