#include "message.hh"

#include <cstring>
#include <map>
#include <stdexcept>

using namespace msg;

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
    return 1;
  }
}

void UniqueTagGenerator::allow( int key )
{
  allowed.insert( key );
}

static std::map<std::pair<OpCode, MessageField>, size_t> field_indices = {
  { { OpCode::RemoteLookup, MessageField::Name }, 0 },       { { OpCode::RemoteStore, MessageField::Name }, 0 },
  { { OpCode::RemoteStore, MessageField::Object }, 1 },      { { OpCode::RemoteDelete, MessageField::Name }, 0 },
  { { OpCode::RemoteSuccess, MessageField::Message }, 0 },   { { OpCode::RemoteError, MessageField::Message }, 0 },
  { { OpCode::LocalLookup, MessageField::Name }, 0 },        { { OpCode::LocalStore, MessageField::Name }, 0 },
  { { OpCode::LocalStore, MessageField::Object }, 1 },       { { OpCode::LocalDelete, MessageField::Name }, 0 },
  { { OpCode::LocalRemoteLookup, MessageField::Name }, 0 },  { { OpCode::LocalRemoteStore, MessageField::Name }, 0 },
  { { OpCode::LocalRemoteStore, MessageField::Object }, 1 }, { { OpCode::LocalRemoteDelete, MessageField::Name }, 0 },
  { { OpCode::LocalSuccess, MessageField::Message }, 0 },    { { OpCode::LocalError, MessageField::Message }, 0 }
};

MessageType get_message_type( const OpCode opcode )
{
  return ( to_underlying( opcode ) & 0xF0 ) ? MessageType::Local : MessageType::Remote;
}

size_t get_field_count( const OpCode opcode )
{
  switch ( opcode ) {
    case OpCode::RemoteLookup:
    case OpCode::RemoteDelete:
    case OpCode::RemoteSuccess:
    case OpCode::RemoteError:
    case OpCode::LocalLookup:
    case OpCode::LocalDelete:
    case OpCode::LocalRemoteLookup:
    case OpCode::LocalRemoteDelete:
    case OpCode::LocalSuccess:
    case OpCode::LocalError:
      return 1;

    case OpCode::RemoteStore:
    case OpCode::LocalStore:
    case OpCode::LocalRemoteStore:
      return 2;

    default:
      throw std::runtime_error( "invalid opcode" );
  }
}

Message::Message( const OpCode opcode, const int32_t tag )
  : type_( get_message_type( opcode ) )
  , field_count_( get_field_count( opcode ) )
  , opcode_( opcode )
  , tag_( tag )
{
  fields_.resize( field_count_ );
}

Message::Message( const MessageType type, const std::string& str )
  : type_( type )
  , field_count_( 0 )
{
  const size_t min_length = sizeof( opcode_ ) + ( type_ == MessageType::Remote ) ? sizeof( tag_ ) : 0;

  if ( str.length() < min_length ) {
    throw std::runtime_error( "str too short" );
  }

  length_ = sizeof( length_ ) + str.size();
  size_t index = 0;

  opcode_ = static_cast<OpCode>( ( str[index] - '0' ) + ( ( type_ == MessageType::Remote ) ? 0 : 0xf0 ) );
  index += sizeof( uint8_t );

  if ( type_ == MessageType::Remote ) {
    tag_ = *reinterpret_cast<const uint32_t*>( &str[index] );
    index += sizeof( tag_ );
  }

  field_count_ = get_field_count( opcode_ );
  fields_.resize( field_count_ );

  if ( index == length_ ) {
    // message has no payload
    return;
  }

  for ( size_t i = 0; i < field_count_; i++ ) {
    if ( i != field_count_ - 1 ) {
      if ( index + 4 > length_ ) {
        throw std::runtime_error( "str too short" );
      }

      const uint32_t field_length = *reinterpret_cast<const uint32_t*>( &str[index] );
      index += sizeof( uint32_t );

      if ( index + field_length > length_ ) {
        throw std::runtime_error( "str too short" );
      }

      fields_[i] = str.substr( index, field_length );
      index += field_length;
    } else {
      fields_[i] = str.substr( index );
    }
  }
}

void Message::calculate_length()
{
  length_ = sizeof( length_ ) + sizeof( opcode_ ) + ( type_ == MessageType::Remote ) ? sizeof( tag_ ) : 0;

  for ( size_t i = 0; i < field_count_ - 1; i++ ) {
    length_ += sizeof( uint32_t ) /* field length */ + fields_[i].length() /* value length */;
  }

  length_ += fields_[field_count_ - 1].length();
}

std::string Message::to_string()
{
  std::string result;

  // copying the header
  size_t index = 0;
  calculate_length();
  result.resize( length_ );

  std::memcpy( &result[index], &length_, sizeof( length_ ) );
  index += sizeof( length_ );

  const uint8_t opcode = '0' + ( to_underlying( opcode_ ) & 0xf );
  std::memcpy( &result[index], &opcode, sizeof( opcode ) );
  index += sizeof( opcode );

  if ( type_ == MessageType::Remote ) {
    std::memcpy( &result[index], &tag_, sizeof( tag_ ) );
    index += sizeof( tag_ );
  }

  // making the payload
  for ( size_t i = 0; i < field_count_; i++ ) {
    if ( i != field_count_ - 1 ) {
      *reinterpret_cast<uint32_t*>( &result[index] ) = static_cast<uint32_t>( fields_[i].length() );
      index += sizeof( uint32_t );
    }
    std::memcpy( &result[index], fields_[i].data(), fields_[i].length() );
  }

  return result;
}

void Message::set_field( const MessageField f, std::string&& s )
{
  fields_.at( field_indices.at( std::make_pair( opcode_, f ) ) ) = std::move( s );
}

std::string& Message::get_field( const MessageField f )
{
  return fields_.at( field_indices.at( std::make_pair( opcode_, f ) ) );
}
