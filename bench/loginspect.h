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

#ifndef FINELINE_BENCH_LOGINSPECT_H
#define FINELINE_BENCH_LOGINSPECT_H

#include <iostream>
#include <memory>

namespace fineline {
namespace loginspect {

template <
    class Log
>
class LogInspector
{
public:

    using LogrecHeader = typename Log::LogKey;

    LogInspector(std::shared_ptr<Log> log)
        : log(log)
    {}

    template <
        class Callback,
        class Filter = std::function<bool(const LogrecHeader&)>
    >
    void exec(Callback f, bool forward = true,
            Filter filter = [](const LogrecHeader&) { return true; })
    {
        auto iter = log->scan(filter, forward);
        LogrecHeader hdr;
        char* payload;
        while (iter->next(hdr, payload)) {
            f(hdr, payload);
        }
    }

    struct PrintCallback
    {
        std::ostream& out = std::cout;

        void operator()(const LogrecHeader& hdr, char*) const
        {
            out << hdr << std::endl;
        }
    };

private:
    std::shared_ptr<Log> log;
};

} // namespace loginspect
} // namespace fineline

#endif
