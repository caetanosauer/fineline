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

#ifndef FINELINE_TXNCONTEXT_H
#define FINELINE_TXNCONTEXT_H

#include <stdexcept>

namespace fineline {

template <class Plog>
class BaseTxnContext
{
public:

    BaseTxnContext(bool auto_commit = true)
        : auto_commit_(auto_commit), active_(true)
    {}

    ~BaseTxnContext()
    {
        if (active_) {
            if (auto_commit_) { commit(); }
            else { abort(); }
        }
    }

    void commit()
    {
    }

    void abort()
    {
    }

    template <class... Args>
    void log(const Args&... args)
    {
        if (!active_) {
            throw std::runtime_error("Cannot log on inactive transaction context");
        }
        plog_.log(args...);
    }

    Plog* get_plog() { return &plog_; }

protected:

    void finish()
    {
        active_ = false;
    }

    bool auto_commit_;
    bool active_;

    Plog plog_;
};

template <class Base>
class ThreadLocalTxnContext : public Base
{
public:
    template <class... T>
    ThreadLocalTxnContext(T... args) : Base(args...)
    {
        if (current) {
            throw std::runtime_error("Thread context already exists");
        }
        current = this;
    }

    ~ThreadLocalTxnContext()
    {
        current = nullptr;
    }

    static ThreadLocalTxnContext* get()
    {
        // TODO check for nullptr?
        return current;
    }

protected:
    thread_local static ThreadLocalTxnContext* current;
};

} // namespace fineline

#endif
