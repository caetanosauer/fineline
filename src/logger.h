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

#ifndef FINELINE_LOGGER_H
#define FINELINE_LOGGER_H

/**
 * \file kv_array.h
 *
 * A fixed-length array that encodes arbitrary key-value pairs and stores them in a slot array in
 * the appropriate position.
 */

#include "lrtype.h"

namespace fineline {

using foster::LRType;

template <class TxnContext, class LRHeader>
class TxnLogger
{
public:
    using LogrecHeader = LRHeader;
    using SysEnv = typename TxnContext::SysEnv;
    using IdType = typename LogrecHeader::IdType;
    using SeqNumType = typename LogrecHeader::SeqNumType;

    TxnLogger() : id_(0), seq_(0) { }

    template <typename... T>
    void log(LRType type, T... args)
    {
        TxnContext::get()->
            log(LogrecHeader{id_, ++seq_, type}, args...);
    }

    void initialize(IdType id, bool logit = true)
    {
        id_ = id;
        if (logit) { log(LRType::Construct, id); }
    }

    IdType id() { return id_; }
    SeqNumType seq_num() { return seq_; }

private:

    IdType id_;
    SeqNumType seq_;
};

} // namespace fineline

#endif
