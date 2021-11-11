#include "assert.h"

#include <iostream>
#include <string>
#include <unordered_set>
#include <vector>

#include "util/util.hh"

class UniqueTagGenerator
{
private:
  std::unordered_set<int> allowed {};

public:
  UniqueTagGenerator( int size );
  int emit();
  void allow( int key );
};

namespace msg {

enum class MessageType
{
  Local,
  Remote
};

// only the last byte is used in the msg::Message header
enum class OpCode : uint16_t
{
  // remote requests
  RemoteLookup = 0x1,
  RemoteStore = 0x2,
  RemoteDelete = 0x3,

  // remote responses
  RemoteSuccess = 0x0,
  RemoteObject = 0x2,
  RemoteError = 0x5,

  // local requests
  LocalLookup = 0xF1,
  LocalStore = 0xF2,
  LocalDelete = 0xF6,
  LocalRemoteLookup = 0xF3,
  LocalRemoteStore = 0xF4,
  LocalRemoteDelete = 0xF7,

  // local responses
  LocalSuccess = 0xF0,
  LocalError = 0xF5,
};

enum class MessageField : uint8_t
{
  Name,
  Object,
  Message,
  RemoteNode,
};

struct Message
{
private:
  MessageType type_;
  size_t field_count_;

  uint32_t length_ {};
  OpCode opcode_ {};
  int32_t tag_ {};

  std::vector<std::string> fields_ {};

  void calculate_length();

public:
  Message( const OpCode opcode, const int32_t tag = 0 );
  Message( const MessageType type, const std::string& str );

  //! \returns serialized msg::Message ready to be sent over the wire
  std::string to_string();

  void set_field( const MessageField f, std::string&& s );
  std::string& get_field( const MessageField f );

  OpCode opcode() const { return opcode_; }
  int32_t tag() const { return tag_; }
};

} // namespace msg

class MessageHandler
{
public:
  // generate REMOTE REQUEST

  std::string generate_remote_lookup( int tag, std::string name )
  {
    msg::Message remote_request { msg::OpCode::RemoteLookup, tag };
    remote_request.set_field( msg::MessageField::Name, std::move( name ) );
    return remote_request.to_string();
  }

  std::string generate_remote_delete( int tag, std::string name )
  {
    msg::Message remote_request { msg::OpCode::RemoteDelete, tag };
    remote_request.set_field( msg::MessageField::Name, std::move( name ) );
    return remote_request.to_string();
  }

  // TODO: rewrite this function in the 'new' way
  std::string generate_remote_store_header( int tag, std::string name, int payload_size )
  {
    std::string remote_request { "0000200000000" + name };
    int* p = reinterpret_cast<int*>( const_cast<char*>( remote_request.c_str() ) );
    p[0] = name.length() + 13 + payload_size;
    p = reinterpret_cast<int*>( const_cast<char*>( remote_request.c_str() + 5 ) );
    p[0] = tag;
    p = reinterpret_cast<int*>( const_cast<char*>( remote_request.c_str() + 9 ) );
    p[0] = name.length();
    return remote_request;
  };

  std::string generate_remote_error( int tag, std::string error )
  {
    msg::Message message { msg::OpCode::RemoteError, tag };
    message.set_field( msg::MessageField::Message, move( error ) );
    return message.to_string();
  }

  std::string generate_remote_success( int tag, std::string error )
  {
    msg::Message message { msg::OpCode::RemoteSuccess, tag };
    message.set_field( msg::MessageField::Message, move( error ) );
    return message.to_string();
  }

  // parse REMOTE REQUEST

  std::tuple<std::string, int> parse_remote_lookup( std::string request )
  {
    msg::Message message { msg::MessageType::Remote, request };
    return { message.get_field( msg::MessageField::Name ), message.tag() };
  }

  std::tuple<std::string, int> parse_remote_store( std::string request )
  {
    msg::Message message { msg::MessageType::Remote, request };
    return { message.get_field( msg::MessageField::Name ), message.tag() };
  }

  std::tuple<std::string, int> parse_remote_error( std::string request )
  {
    msg::Message message { msg::MessageType::Remote, request };
    return { message.get_field( msg::MessageField::Message ), message.tag() };
  }

  // generate LOCAL RESPONSE

  // TODO: rewrite this function in the 'new' way
  std::string generate_local_object_header( std::string name, int payload_size )
  {
    std::string remote_request { "000020000" + name };
    int* p = reinterpret_cast<int*>( const_cast<char*>( remote_request.c_str() ) );
    p[0] = name.length() + 9 + payload_size;
    p = reinterpret_cast<int*>( const_cast<char*>( remote_request.c_str() + 5 ) );
    p[0] = name.length();
    return remote_request;
  }

  std::string generate_local_error( std::string error )
  {
    msg::Message message { msg::OpCode::LocalError };
    message.set_field( msg::MessageField::Message, move( error ) );
    return message.to_string();
  }

  std::string generate_local_success( std::string error )
  {
    msg::Message message { msg::OpCode::LocalSuccess };
    message.set_field( msg::MessageField::Message, move( error ) );
    return message.to_string();
  }

  // parse LOCAL REQUEST

  std::tuple<std::string, int> parse_local_remote_lookup( std::string message )
  {
    msg::Message request { msg::MessageType::Local, message };
    const auto name = request.get_field( msg::MessageField::Name );
    const auto id = *reinterpret_cast<const int*>( request.get_field( msg::MessageField::RemoteNode ).c_str() );

    return { name, id };
  }

  std::string parse_local_lookup( std::string request )
  {
    msg::Message message { msg::MessageType::Local, request };
    return message.get_field( msg::MessageField::Name );
  };

  // generate LOCAL REQUEST

  std::string generate_local_lookup( std::string name )
  {
    msg::Message request { msg::OpCode::LocalLookup };
    request.set_field( msg::MessageField::Name, move( name ) );
    return request.to_string();
  };

  std::string generate_local_delete( std::string name )
  {
    msg::Message request { msg::OpCode::LocalDelete };
    request.set_field( msg::MessageField::Name, move( name ) );
    return request.to_string();
  };


  // std::string generate_local_remote_lookup(std:: string name, int id)
  // {
  //   msg::Message request {msg::OpCode::LocalRemoteLookup};
  //   request.set_field()


  // }



};
