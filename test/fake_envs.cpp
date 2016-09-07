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

#include "fake_envs.h"

namespace fineline {
namespace test {

template <
    class T1,
    class T2,
    class T3,
    class T4
>
std::shared_ptr<T2> GenericEnv<T1, T2, T3, T4>::log_buffer;

template <
    class T1,
    class T2,
    class T3,
    class T4
>
std::shared_ptr<T1> GenericEnv<T1, T2, T3, T4>::commit_buffer;

template <
    class T1,
    class T2,
    class T3,
    class T4
>
std::shared_ptr<T3> GenericEnv<T1, T2, T3, T4>::log_flusher;

template <
    class T1,
    class T2,
    class T3,
    class T4
>
std::shared_ptr<T4> GenericEnv<T1, T2, T3, T4>::log;

}; // namespace test
}; // namespace fineline

