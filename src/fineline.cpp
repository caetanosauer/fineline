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

#include "fineline.h"
#include "options.h"

namespace fineline {

void init(const Options& options)
{
    SysEnv::initialize(options);
}

void SysEnv::do_init(const Options& options)
{
    log_buffer = std::make_shared<DftLogBuffer>();
    // TODO init SQLite lot with options
    log = std::make_shared<DftPersistentLog>(options);
    commit_buffer = std::make_shared<DftCommitBuffer>(log_buffer);
    log_flusher = std::make_shared<DftLogFlusher>(log_buffer, log);
}

std::shared_ptr<DftLogBuffer> SysEnv::log_buffer;
std::shared_ptr<DftCommitBuffer> SysEnv::commit_buffer;
std::shared_ptr<DftLogFlusher> SysEnv::log_flusher;
std::shared_ptr<DftPersistentLog> SysEnv::log;
std::mutex SysEnv::init_mutex_;
bool SysEnv::initialized_ = false;

}; // namespace fineline
