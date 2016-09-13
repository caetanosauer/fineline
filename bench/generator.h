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

#include <iostream>
#include <random>
#include <type_traits>

namespace fineline {
namespace gen {

using DefaultGenerator = std::mt19937;

enum class DistributionType
{
    Uniform
    // TODO add more
};

template <DistributionType type>
struct GetDistribution
{
};

template <>
struct GetDistribution<DistributionType::Uniform>
{
    using Error = void;

    template <class T>
    using templ = typename
        // if T is integer
        std::conditional<std::is_integral<T>::value,
            // return int distribution
            std::uniform_int_distribution<T>,
            // else if T is floating point
            std::conditional<std::is_floating_point<T>::value,
                // return real distribution
                std::uniform_real_distribution<T>,
                // else return Error
                Error
            >
        >::type;
};

template <
    class T,
    class Gen,
    template <class> class Distr,
    T Max,
    T Min = T{0}
>
T random()
{
    static Gen generator;
    static Distr<T> distr {Min, Max};
    return distr(generator);
}

template <class T, T Max, T Min = T{0}>
T random_uniform()
{
    return random<T, DefaultGenerator, GetDistribution<DistributionType::Uniform>::templ, Max, Min>
        ();
}

template<class Opt>
std::string generate_string()
{
    static constexpr char alphabet[] =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "abcdefghijklmnopqrstuvwxyz";
    static constexpr size_t alphabet_size = std::extent<decltype(alphabet)>::value;

    auto len = Opt::MaxLength;
    if (Opt::RandomLength) {
        len = random_uniform<size_t, Opt::MaxLength, 1>();
    }

    std::string str(size_t{len}, 'b');
    for (unsigned i = 0; i < len; i++) {
        auto r = random_uniform<size_t, alphabet_size - 1>();
        str[i] = alphabet[r];
    }

    return str;
}

} // namespace gen
} // namespace fineline

#endif
