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

#ifndef FINELINE_TEST_FAKE_ENVS_H
#define FINELINE_TEST_FAKE_ENVS_H

#include "fake_log_fs.h"
#include "default_templates.h"

namespace fineline {
namespace test {

template <class Env>
void init(const Options& opt = Options{})
{
    Env::initialize(opt);
}

template <
    class CommitBuffer,
    class LogBuffer,
    class LogFlusher,
    class PersistentLog
>
class GenericEnv
{
public:
    using EpochNumber = typename LogFlusher::EpochNumber;

    static void initialize(const Options& options)
    {
        log_buffer = std::make_shared<LogBuffer>();
        log = std::make_shared<PersistentLog>(options);
        commit_buffer = std::make_shared<CommitBuffer>(log_buffer);
        log_flusher = std::make_shared<LogFlusher>(log_buffer, log);
    }

    static std::shared_ptr<CommitBuffer> commit_buffer;
    static std::shared_ptr<LogBuffer> log_buffer;
    static std::shared_ptr<LogFlusher> log_flusher;
    static std::shared_ptr<PersistentLog> log;
};

template <class Env>
using FakeTxnContext = ThreadLocalScope<TxnContext<DftPlog, Env>>;

template <class Env>
using FakeLogger = TxnLogger<FakeTxnContext<Env>, DftLogrecHeader>;

template <size_t P>
using FakeLogFS = LogPageVector<P, 3>;

template <class LogPage>
using FakePersistentLog = FileBasedLog<LogPage, StdMapLogIndex, FakeLogFS>;

using FakeLogFlusher = LogFlusher<ExtLogPage, DftLogBufferTemp, FakePersistentLog>;

using FakeLogEnv = GenericEnv<DftCommitBuffer, DftLogBuffer, FakeLogFlusher,
      FakePersistentLog<ExtLogPage>>;

} // namespace test
} // namespace fineline

/*
 * Template instance definitions -- C++ requirement.
 * (yep, I know it doesn't look nice, but that's the way things are with templates)
 */
#include "threadlocal.cpp"
#include "legacy/lsn.cpp"
#include "legacy/carray.cpp"

#endif
