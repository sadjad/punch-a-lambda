#pragma once
#include <cstring>
#include <functional>
#include <iostream>
#include <optional>
#include <stdlib.h>
#include <string>
#include <unordered_map>
#include <vector>

struct Blob
{
  bool mutablility {};
  size_t size {};
  void* ptr {};
};

class LocalStorage
{
private:
  std::unordered_map<std::string, Blob> storage_ {};
  size_t total_size_;
  size_t max_size_;

public:
  LocalStorage( size_t max_size );
  int get_total_size();
  std::optional<Blob> locate( const std::string& key );
  std::optional<void*> new_object( std::string key, size_t size );
  int new_object_from_string( std::string key, std::string&& object );
  int commit( std::string key );
  int grow( std::string key, size_t size );
  int delete_object( std::string key );
};
