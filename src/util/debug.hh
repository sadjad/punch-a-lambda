#include <iostream>
#include <string>

#define DEBUG 1
#if DEBUG
#define DEBUGINFO( x ) std::cout << ( std::string( "[storageserver] " ) + x + "\n" ) << std::flush;
#else
#define DEBUGINFO( x )
#endif
