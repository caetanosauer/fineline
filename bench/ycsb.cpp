#include "ycsb.h"

#include <iostream>

using namespace fineline;

int main(int, char**)
{
    gen::StringGenerator<1,100> strgen;
    gen::NumberGenerator<unsigned, 1,100> numgen;

    // Little test program just to see if the two functions are working as expected
    for (int i = 0; i < 100; i++) {
        std::cout
            << "k=" << numgen.next() << '\t'
            << "v=" << strgen.next()
            << std::endl;
    }

    return EXIT_SUCCESS;
}
