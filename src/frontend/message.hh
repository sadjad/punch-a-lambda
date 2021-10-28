#include "assert.h"
#include <unordered_set>

class UniqueTagGenerator
{
private:
  std::unordered_set<int> allowed;

public:
  UniqueTagGenerator( int size );
  int emit();
  void allow( int key );
};

UniqueTagGenerator::UniqueTagGenerator( int size )
{
  for ( int i = 0; i < size; i++ ) {
    allowed.insert( i );
  }
}

int UniqueTagGenerator::emit()
{
  if ( allowed.size() > 0 ) {
    int key = *( allowed.begin() );
    allowed.erase( key );
    return key;
  } else {
    assert( false );
  }
}

void UniqueTagGenerator::allow( int key )
{
  allowed.insert( key );
}

class MessageHandler
{
public:
  // rely on RVO for the return value

  std::string generate_remote_lookup( int tag, std::string name )
  {
    std::string remote_request { "000010000" + name };
    int* p = (int*)const_cast<char*>( remote_request.c_str() );
    p[0] = name.length() + 9;
    p = (int*)const_cast<char*>( remote_request.c_str() + 5 );
    p[0] = tag;
    return remote_request;
  };
  std::string generate_remote_delete( int tag, std::string name )
  {
    std::string remote_request { "000030000" + name };
    int* p = (int*)const_cast<char*>( remote_request.c_str() );
    p[0] = name.length() + 9;
    p = (int*)const_cast<char*>( remote_request.c_str() + 5 );
    p[0] = tag;
    return remote_request;
  };
  // note that we send remote store as two messages, the first is a plaintext header and the second is a ptr payload
  std::string generate_remote_store_header( int tag, std::string name, int payload_size )
  {
    std::string remote_request { "0000200000000" + name };
    int* p = (int*)const_cast<char*>( remote_request.c_str() );
    p[0] = name.length() + 13 + payload_size;
    p = (int*)const_cast<char*>( remote_request.c_str() + 5 );
    p[0] = tag;
    p = (int*)const_cast<char*>( remote_request.c_str() + 9 );
    p[0] = name.length();
    return remote_request;
  };

  std::string generate_local_object_header( std::string name, int payload_size )
  {
    std::string remote_request { "000020000" + name };
    int* p = (int*)const_cast<char*>( remote_request.c_str() );
    p[0] = name.length() + 9 + payload_size;
    p = (int*)const_cast<char*>( remote_request.c_str() + 5 );
    p[0] = name.length();
    return remote_request;
  };

  std::string generate_remote_error( int tag, std::string error )
  {
    std::string message { "000050000" + error };
    int* p = (int*)const_cast<char*>( message.c_str() );
    p[0] = message.length();
    p = (int*)const_cast<char*>( message.c_str() + 5 );
    p[0] = tag;
    return message;
  };
  std::string generate_remote_success( int tag, std::string error )
  {
    std::string message { "000000000" + error };
    int* p = (int*)const_cast<char*>( message.c_str() );
    p[0] = message.length();
    p = (int*)const_cast<char*>( message.c_str() + 5 );
    p[0] = tag;
    return message;
  };
  std::tuple<std::string, int> parse_remote_lookup( std::string request )
  {
    int tag = *(int*)( request.c_str() + 1 );
    std::string name = request.substr( 5 );
    return { name, tag };
  };
  std::tuple<std::string, int, int> parse_remote_store( std::string request )
  {
    int tag = *(int*)( request.c_str() + 1 );
    int size = *(int*)( request.c_str() + 5 );
    std::string name = request.substr( 9, size ); // name can only be 4 characters for now
    return { name, size, tag };
  };
  std::tuple<std::string, int> parse_remote_error( std::string request )
  {
    int tag = *(int*)( request.c_str() + 1 );
    std::string name = request.substr( 5 );
    return { name, tag };
  };

  std::string parse_local_lookup( std::string request ) { return request.substr( 1 ); };
  std::string generate_local_error( std::string error )
  {
    std::string message { "00005" + error };
    int* p = (int*)const_cast<char*>( message.c_str() );
    p[0] = message.length();
    return message;
  };
  std::string generate_local_success( std::string error )
  {
    std::string message { "00000" + error };
    int* p = (int*)const_cast<char*>( message.c_str() );
    p[0] = message.length();
    return message;
  };
  std::tuple<std::string, int> parse_local_remote_lookup( std::string message )
  {
    int size = *(int*)( message.c_str() + 1 );
    std::cout << "size " << size << ";" << std::endl;
    std::string name = message.substr( 5, size );
    int id = *(int*)( message.c_str() + 5 + size );
    return { name, id };
  }
};