#include "local_storage.hh"

LocalStorage::LocalStorage( size_t max_size )
  : total_size_( 0 )
  , max_size_( max_size )
{}

int LocalStorage::get_total_size()
{
  return total_size_;
}

std::optional<Blob> LocalStorage::locate( const std::string& key )
{
  std::unordered_map<std::string, Blob>::iterator got = storage_.find( key );
  if ( got == storage_.end() ) {
    return {};
  } else {
    return got->second;
  }
}

std::optional<void*> LocalStorage::new_object( std::string key, size_t size )
{
  if ( total_size_ + size > max_size_ ) {
    std::cerr << "Allocation surpassing maximum size" << std::endl;
    return {};
  } else {
    void* ptr = malloc( size );
    total_size_ += size;

    bool ok = storage_.insert( { key, { true, size, ptr } } ).second;
    if ( ok ) {
      return ptr;
    } else {
      std::cerr << "key is in storage" << std::endl;
      return {};
    }
  }
}

int LocalStorage::new_object_from_string( std::string key, std::string&& object )
{
  auto size = object.length();
  if ( total_size_ + size > max_size_ ) {
    std::cerr << "Allocation surpassing maximum size" << std::endl;
    return 1;
  } else {

    void* ptr = malloc( size );
    
    bool ok = storage_.insert( { key, { true, size, ptr } } ).second;
    if ( !ok ) {
      return 1;
    }
    std::memcpy( ptr, object.c_str(), size );
    total_size_ += size;
    return 0;
  }
}

int LocalStorage::commit( std::string key )
{
  
  auto storage_lookup = storage_.find( key );
  if ( storage_lookup != storage_.end() ) {
    storage_lookup->second.mutablility = false;
    return 0;
  } else {
    std::cerr << "commit key not found" << std::endl;
    return 1;
  }
  
}

int LocalStorage::grow( std::string key, size_t size )
{
  if ( size + total_size_ > max_size_ ) {
    std::cerr << "out of memory" << std::endl;
    return 1;
  }
  
  auto storage_lookup = storage_.find( key );
  if ( storage_lookup != storage_.end() ) {
    Blob& blob = storage_lookup->second;
    if ( blob.mutablility == false ) {
      std::cerr << "cannot grow an immutable blob" << std::endl;
      return 1;
    }
    blob.size += size;
    blob.ptr = realloc( blob.ptr, blob.size );
    return 0;
  } else {
    std::cerr << "commit key not found" << std::endl;
    return 1;
  }
  
}

int LocalStorage::delete_object( std::string key )
{
  
  auto storage_lookup = storage_.find( key );
  if ( storage_lookup != storage_.end() ) {
    free( storage_.find( key )->second.ptr );
    total_size_ -= storage_.find( key )->second.size;
    storage_.erase( key );
    return 0;
  } else {
    std::cerr << "commit key not found" << std::endl;
    return 1;
  }
}


