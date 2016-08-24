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

#ifndef FINELINE_FINELINE_H
#define FINELINE_FINELINE_H

#include "logpage.h"
#include "plog.h"
#include "txncontext.h"
#include "logger.h"
#include "logrec.h"

namespace fineline {

/*
 * COMPILE-TIME CONSTANTS
 */
constexpr size_t LogPageSize = 8192;
constexpr size_t ExtLogPageSize = 1048576; // 1MB

using NodeIdType = uint32_t;
using SeqNumType = uint32_t;
using LogrecLengthType = uint16_t;

using DftLogrecHeader = LogrecHeader<NodeIdType, SeqNumType, LogrecLengthType>;
using DftLogPage = LogPage<LogPageSize, DftLogrecHeader>;
using ExtLogPage = LogPage<ExtLogPageSize, DftLogrecHeader>;
using OverflowPlog = ChainedPagesPrivateLog<ExtLogPage>;
using DftPlog = TxnPrivateLog<DftLogPage, OverflowPlog>;
using DftTxnContext = ThreadLocalTxnContext<BaseTxnContext<DftPlog>>;
using DftLogger = TxnLogger<DftTxnContext, DftLogrecHeader>;


} // namespace fineline

#endif

