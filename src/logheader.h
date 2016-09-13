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

#include <ostream>

#include "lrtype.h"
#include "encoding.h"

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
        : node_id_(foster::swap_endianness(id)), seq_num_(foster::swap_endianness(seq)),
        type_(type)
        // length is set by LogPage
    {}


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

    friend std::ostream& operator<<(std::ostream& out,
            const LogrecHeader<NodeId, SeqNum, LogrecLength>& hdr)
    {
        return out
            << "{id: " << hdr.node_id()
            << ", seq: " << hdr.seq_num()
            << ", len: " << hdr.length()
            << ", type: " << foster::LRTypeString{}(hdr.type())
            << "}";
    }

    void set_length(LogrecLength len) { length_ = len; }

    NodeId node_id() const { return foster::swap_endianness(node_id_); };
    SeqNum seq_num() const { return foster::swap_endianness(seq_num_); };
    LogrecLength length() const { return length_; }
    LRType type() const { return type_; }

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

    NodeId node_id_;
    SeqNum seq_num_;
    LogrecLength length_;
    LRType type_;
};

} // namespace fineline

#endif

