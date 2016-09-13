#include "ycsb.h"

#include <iostream>

using namespace fineline;

struct StringOpt {
    static constexpr size_t MaxLength = 100;
    static constexpr bool RandomLength = true;
};

int main(int, char**)
{
    // Little test program just to see if the two functions are working as expected
    for (int i = 0; i < 100; i++) {
        std::cout
            << "k=" << gen::random_uniform<unsigned, 100>() << '\t'
            << "v=" << gen::generate_string<StringOpt>()
            << std::endl;
    }

    return EXIT_SUCCESS;
}
