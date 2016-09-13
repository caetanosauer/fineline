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

#ifndef FINELINE_LOGHEADER_H
#define FINELINE_LOGHEADER_H

#include "lrtype.h"

namespace fineline {

using foster::LRType;
using foster::assert;

// Default alignment = 1/2 of typical cache line (64B)
constexpr size_t LogrecAlignment = 32;

template <
    class NodeId = uint32_t,
    class SeqNum = uint32_t,
    class LogrecLength = uint16_t
>
struct alignas(LogrecAlignment) LogrecHeader
{
    using IdType = NodeId;
    using SeqNumType = SeqNum;

    LogrecHeader() {}

    LogrecHeader(NodeId id, SeqNum seq, LRType type)
        : node_id(id), seq_num(seq), type(type)
        // length is set by LogPage
    {}

    NodeId node_id;
    SeqNum seq_num;
    LogrecLength length;
    LRType type;

    friend bool operator<(
            const LogrecHeader<NodeId, SeqNum, LogrecLength>& a,
            const LogrecHeader<NodeId, SeqNum, LogrecLength>& b)
    {
        return cmp(a, b) < 0;
    }

    friend bool operator<=(
            const LogrecHeader<NodeId, SeqNum, LogrecLength>& a,
            const LogrecHeader<NodeId, SeqNum, LogrecLength>& b)
    {
        return cmp(a, b) <= 0;
    }

    friend bool operator>(
            const LogrecHeader<NodeId, SeqNum, LogrecLength>& a,
            const LogrecHeader<NodeId, SeqNum, LogrecLength>& b)
    {
        return cmp(a, b) > 0;
    }

    friend bool operator>=(
            const LogrecHeader<NodeId, SeqNum, LogrecLength>& a,
            const LogrecHeader<NodeId, SeqNum, LogrecLength>& b)
    {
        return cmp(a, b) >= 0;
    }

    friend bool operator==(
            const LogrecHeader<NodeId, SeqNum, LogrecLength>& a,
            const LogrecHeader<NodeId, SeqNum, LogrecLength>& b)
    {
        return cmp(a, b) == 0;
    }

    friend bool operator!=(
            const LogrecHeader<NodeId, SeqNum, LogrecLength>& a,
            const LogrecHeader<NodeId, SeqNum, LogrecLength>& b)
    {
        return cmp(a, b) != 0;
    }

private:
    static int cmp(
            const LogrecHeader<NodeId, SeqNum, LogrecLength>& a,
            const LogrecHeader<NodeId, SeqNum, LogrecLength>& b)
    {
        /*
         * "raw" comparison operator that uses the binary contents for comparison in order to
         * speed up sort and select operarions. This is also known as a "normalized key".
         * Because I am not sure about the implications of different struct alignments, I am
         * asserting that the two key fields are of the same size.
         */
        static_assert(sizeof(NodeId) == sizeof(SeqNum),
                "LogrecHeader comparison currently requires the same types for NodeId and SeqNum");

        return memcmp(&a, &b, sizeof(NodeId) + sizeof(SeqNum));
    }
};

} // namespace fineline

#endif

