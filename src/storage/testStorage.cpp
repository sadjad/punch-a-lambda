#include <iostream>
#include "LocalStorage.hpp"
#include <cstdlib>
#include <chrono>
#include <cstring>
using namespace std::chrono;
void require_fn( unsigned int line_no, const int condition )
{
  if ( not condition ) {
    throw std::runtime_error( "test failure at line " + std::to_string( line_no ) );
  }
}
template <typename T>
void doNotOptimize(T const& val) {
  asm volatile("" : : "m"(val) : "memory");
}

#define require( C ) require_fn( __LINE__, C )

void test_new_creation1()
{
    auto a = new LocalStorage(4096);
    std::string test{"bump"};
    a->new_object(test, 1000);
    require(! a->new_object(test, 1000).has_value());
    delete a;
}

void test_new_creation2()
{
    auto a = new LocalStorage(4096);
    std::string test{"bump"};
    require(a->new_object(test, 1000).has_value());
    delete a;
}

void stress_test_new_creation()
{
    auto t1 = high_resolution_clock::now();
    auto a = new LocalStorage(1024 * 1024 * 1024);
    
    for(int i = 0; i < 1000; i ++)
    {
        void * ptr = a->new_object("bump", 1024).value();
        memset(ptr, 1, 1024);
        a->delete_object("bump");
    }
    auto t2 = high_resolution_clock::now();
    duration<double, std::milli> ms_double = t2 - t1;
    printf (" == object storage allocation == \n== at %.5f milliseconds == \n ", ms_double.count());

    delete a;

    t1 = high_resolution_clock::now();
    for(int i = 0; i < 1000; i ++)
    {
        void * ptr = malloc(1024);
        doNotOptimize(ptr);
        memset(ptr, 1, 1024);
        free(ptr);
    }
    t2 = high_resolution_clock::now();
    ms_double = t2 - t1;
    printf (" == C allocation == \n== at %.5f milliseconds == \n ", ms_double.count());

}

void test_add_alias()
{
    auto a = new LocalStorage(1024 * 1024 * 1024);
    std::string test{"bump"};
    require(a->new_object(test, 1000).has_value());
    a->add("bump","foobar");
    require(a->locate("foobar").has_value());
    delete a;
}

void test_grow()
{
    auto a = new LocalStorage(1024 * 1024 * 1024);
    std::string test{"bump"};
    require(a->new_object(test, 1000).has_value());
    a->add("bump","foobar");
    require(a->locate("foobar").has_value());

    require(a->grow("bump",2000) == 0);
    require(a->grow("foobar",3000)==0);

    require(a->locate("bump").value().size == 6000);

    delete a;
}

void test_delete();

int main()
{
    test_new_creation1();
    test_new_creation2();
    stress_test_new_creation();
    test_add_alias();
    test_grow();
}
