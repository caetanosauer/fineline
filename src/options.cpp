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

#include "options.h"

#include <mutex>
#include <fstream>

namespace fineline {

using std::string;

void Options::init_description(popt::options_description& opt)
{
    opt.add_options()
        /* General log manager options */
        ("logpath,l", popt::value<string>()->default_value("log"),
         "Path in which log is stored (directory if file-based)")
        ("format", popt::value<bool>()->default_value(false)->implicit_value(true),
         "Whether to format the log, deleting all existing log files or blocks")
        ("log_recycle", popt::value<bool>()->default_value(false)->implicit_value(true),
         "Whether to clean-up log by deleting old files/partitions")
        /* File-based log (legacy::log_storage) options */
        ("log_file_size", popt::value<unsigned>()->default_value(1024),
         "Maximum size of a log file (in MB)")
        ("log_max_files", popt::value<unsigned>()->default_value(0),
         "Maximum number of log files to maintain (0 = unlimited)")
        ("log_index_path", popt::value<string>()->default_value("index.db"),
         "Path to log index file")
        ("log_index_path_relative", popt::value<bool>()->default_value(true),
         "Whether log index path is relative to logpath or absolute")
    ;
}

popt::options_description& Options::get_description()
{
    /*
     * This code makes sure that the descriptions are initialized only once.
     * After that, all callers will get the same descriptions, regardless of
     * multi-threaded schedules.
     */
    static std::once_flag init_once_flag;
    static popt::options_description desc;

    std::call_once(init_once_flag, init_description, std::ref(desc));
    return desc;
}

Options::Options()
{
    // Initialize with default option values
    int argc = 0;
    char* ptr = nullptr;
    popt::store(popt::parse_command_line(argc, &ptr, get_description()), map_);
}

Options::Options(int argc, char** argv)
{
    popt::store(popt::parse_command_line(argc, argv, get_description()), map_);
}

Options::Options(string config_file)
{
    parse_from_file(config_file);
}

void Options::parse_from_file(string path)
{
    std::ifstream is {path};
    popt::store(popt::parse_config_file(is, get_description(), true), map_);
}

} // namespace fineline
