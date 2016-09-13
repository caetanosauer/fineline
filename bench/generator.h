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

#ifndef FINELINE_BENCH_GENERATOR_H
#define FINELINE_BENCH_GENERATOR_H

#include <random>
#include <type_traits>

#include "assertions.h"

namespace fineline {
namespace gen {

using foster::assert;

using DefaultGenerator = std::mt19937;

enum class DistributionType
{
    Uniform
    // TODO add more
};

template <DistributionType type, class T>
struct GetDistribution
{
};

template <class T>
struct GetDistribution<DistributionType::Uniform, T>
{
    using Error = void;
    using type =
        // if T is integer
        typename std::conditional<std::is_integral<T>::value,
            // return int distribution
            std::uniform_int_distribution<T>,
            // else if T is floating point
            typename std::conditional<std::is_floating_point<T>::value,
                // return real distribution
                std::uniform_real_distribution<T>,
                // else return Error
                Error
            >::type
        >::type;
};

template <
    class T,
    long Min,
    long Max,
    DistributionType Distr = DistributionType::Uniform,
    class Gen = DefaultGenerator
>
class NumberGenerator
{
public:
    using Distribution = typename GetDistribution<Distr,T>::type;

    NumberGenerator()
        : distr((T) Min, (T) Max)
    {}

    T next()
    {
        return distr(generator);
    }

protected:
    Gen generator;
    Distribution distr;
};

static constexpr char Alphabet[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
static constexpr size_t AlphabetSize = std::extent<decltype(Alphabet)>::value;

template<
    size_t MinLength,
    size_t MaxLength,
    DistributionType LengthDistr = DistributionType::Uniform,
    class Gen = DefaultGenerator
>
class StringGenerator
{
public:

    StringGenerator()
        : str_(MaxLength, '\0')
    {}

    std::string& next()
    {
        auto len = MaxLength;
        if (MinLength < MaxLength) {
            len = len_gen_.next();
            str_.resize(len);
        }

        for (unsigned i = 0; i < len; i++) {
            auto r = char_gen_.next();
            str_[i] = Alphabet[r];
        }

        return str_;
    }

protected:

    std::string str_;
    NumberGenerator<size_t, 0, MaxLength, LengthDistr> len_gen_;
    NumberGenerator<size_t, 1, AlphabetSize-1, DistributionType::Uniform> char_gen_;
};

/**
 * Thanks to http://www.cse.usf.edu/~christen/tools/genzipf.c
 */
template <class T>
void generate_zipf(std::vector<T>& vec, double alpha, size_t domain_size, size_t count)
{
    static_assert(std::is_integral<T>::value,
            "Zipf distribution can only be generated on integer keys");

    NumberGenerator<double, 0, 1> gen;
    double z;

    // Compute normalization constant
    double c = 0;
    for (unsigned i = 1; i <= domain_size; i++) {
        c += (1.0 / std::pow((double) i, alpha));
    }
    c = 1.0 / c;

    for (unsigned j = 0; j < count; j++) {
        // Pull a uniform random number (0 < z < 1)
        do { z = gen.next(); } while ((z == 0) || (z == 1));

        // Map z to the value
        double sum_prob = 0.0;
        for (T i = 1; i <= domain_size; i++) {
            sum_prob = sum_prob + c / pow((double) i, alpha);
            if (sum_prob >= z) {
                vec.push_back(i);
                break;
            }
        }

        assert<3>(vec.size() == j+1);
    }
}

} // namespace gen
} // namespace fineline

#endif
