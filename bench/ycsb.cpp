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

#include "ycsb.h"
#include "container.h"

#include <iostream>
#include <map>

#include "fineline.h"

using namespace fineline;

struct YCSBStaticOptions
{
    static constexpr size_t MinRecSize = 100;
    static constexpr size_t MaxRecSize = 100;
};

using DftYCSB = ycsb::Benchmark<
    PersistentMap<std::map<unsigned, std::string>, DftLogger>,
    DftTxnContext,
    YCSBStaticOptions
>;

int main(int argc, char** argv)
{
    fineline::init(argc, argv);

    DftYCSB bench;
    bench.load();
    bench.execute();

    return EXIT_SUCCESS;
}
