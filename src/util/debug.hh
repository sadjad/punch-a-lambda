#include <iostream>
#include <string>

#define DEBUG 1
#if DEBUG
#define DEBUGINFO( x ) std::cout << ( "[" + std::to_string( __LINE__ ) + "] " + x + "\n" );
#else
#define DEBUGINFO( x )
#endif
