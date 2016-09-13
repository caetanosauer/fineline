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

#ifndef FINELINE_BENCH_YCSB_H
#define FINELINE_BENCH_YCSB_H

#include "generator.h"

namespace fineline {
namespace ycsb {

struct BenchmarkOptions
{
    size_t load_batch_size = 100;
    size_t num_records = 1000;
    size_t num_transactions = 10000;
    float write_ratio = 0.5;
    float zipf_skew = 0.2;
};

template <
    class Container,
    class TxnContext,
    class StaticOptions
>
class Benchmark
{
public:

    Benchmark(const BenchmarkOptions& opt = BenchmarkOptions{})
        : opt_(opt)
    {
        gen::generate_zipf(zipf_,
                opt_.zipf_skew,
                opt_.num_records,
                opt_.num_transactions);
    }

    void load()
    {
        if (loaded_) return;

        unsigned i = 0;
        while (i < opt_.num_records) {
            TxnContext ctx {true};
            unsigned j = 0;
            while (j < opt_.load_batch_size && i < opt_.num_records) {
                auto key = i + 1;
                auto& value = strgen_.next();
                table_.put(key, value);
                i++;
                j++;
            }
        }

        loaded_ = true;
    }

    void execute()
    {
        gen::NumberGenerator<float, 0, 1> gen;

        for (unsigned i = 0; i < opt_.num_transactions; i++) {
            if (gen.next() <= opt_.write_ratio) {
                do_write(i);
            }
            else {
                do_read(i);
            }
        }
    }

protected:

    void do_read(unsigned txn_id)
    {
        TxnContext ctx;
        auto key = zipf_[txn_id];
        std::string value;
        table_.get(key, value);
        ctx.commit();
    }

    void do_write(unsigned txn_id)
    {
        TxnContext ctx;
        auto key = zipf_[txn_id];
        table_.put(key, strgen_.next());
        ctx.commit();
    }

private:
    std::vector<unsigned> zipf_;
    gen::StringGenerator<StaticOptions::MinRecSize, StaticOptions::MaxRecSize> strgen_;
    Container table_;
    BenchmarkOptions opt_;
    bool loaded_ = false;
};

} // namespace ycsb
} // namespace fineline

#endif

