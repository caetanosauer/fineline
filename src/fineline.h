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

#include "default_templates.h"

namespace fineline {

class SysEnv;

using DftTxnContext = ThreadLocalScope<TxnContext<DftPlog, SysEnv>>;
using DftLogger = TxnLogger<DftTxnContext, DftLogrecHeader>;

/*
 * Global function that initializes the global static SysEnv members with system components of
 * the default types specified above.
 */
void init(const Options& = Options{});
void init(int argc, char** argv);

class SysEnv
{
public:
    using EpochNumber = typename DftLogFlusher::EpochNumber;

    static void initialize(const Options& opt)
    {
        std::unique_lock<std::mutex> lck {init_mutex_};
        if (!initialized_) {
            do_init(opt);
            initialized_ = true;
        }
    }

    static std::shared_ptr<DftCommitBuffer> commit_buffer;
    static std::shared_ptr<DftLogBuffer> log_buffer;
    static std::shared_ptr<DftLogFlusher> log_flusher;
    static std::shared_ptr<DftPersistentLog> log;

private:
    static std::mutex init_mutex_;
    static bool initialized_;

    static void do_init(const Options& opt);
};

} // namespace fineline

/*
 * Template instance definitions -- C++ requirement.
 * (yep, I know it doesn't look nice, but that's the way things are with templates)
 */
#include "threadlocal.cpp"
#include "legacy/lsn.cpp"
#include "legacy/carray.cpp"
#include "legacy/log_file.cpp"
#include "legacy/log_storage.cpp"

#endif
