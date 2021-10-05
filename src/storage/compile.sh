g++ -O3 -shared -fPIC LocalStorage.cpp -o LocalStorage.so --std=c++17
g++ -O3 -I. testStorage.cpp LocalStorage.so -o test --std=c++17
