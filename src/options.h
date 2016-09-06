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

#ifndef FINELINE_SYSOPT_H
#define FINELINE_SYSOPT_H

#include <boost/program_options.hpp>
namespace popt = boost::program_options;

namespace fineline {

class Options {
public:
    Options();
    Options(int argc, char** argv);
    Options(std::string config_file);

    static popt::options_description& get_description();

    template <class T>
    void get(std::string name, T& opt)
    {
        opt = map_[name].as<T>();
    }

    std::string get_string(std::string name)
    {
        return map_[name].as<std::string>();
    }

    unsigned long get_unsigned(std::string name)
    {
        return map_[name].as<unsigned long>();
    }

    long get_int(std::string name)
    {
        return map_[name].as<long>();
    }

protected:
    void parse_from_file(std::string path);

private:
    static void init_description(popt::options_description& opt);

    popt::variables_map map_;
};

} // namespace fineline

#endif
