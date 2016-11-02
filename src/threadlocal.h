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

#ifndef FINELINE_THREAD_LOCAL_H
#define FINELINE_THREAD_LOCAL_H

#include <stdexcept>

namespace fineline {

template <class Base>
class ThreadLocalScope : public Base
{
public:
    template <class... T>
    ThreadLocalScope(T... args) : Base(args...)
    {
        if (current) {
            throw std::runtime_error("Thread-local object already exists");
        }
        current = this;
    }

    ~ThreadLocalScope()
    {
        current = nullptr;
    }

    static Base* get()
    {
        if (!current) {
            throw std::runtime_error("Thread-local object was not initialized \
                    (Did you forget to create a TxnContext?)");
        }
        return current;
    }

protected:
    thread_local static ThreadLocalScope<Base>* current;
};

} // namespace fineline

#endif
