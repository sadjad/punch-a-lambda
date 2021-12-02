#pragma once

#include <optional>
#include <queue>

#include "net/socket.hh"
#include "storage/local_storage.hh"
#include "util/eventloop.hh"
#include "util/ring_buffer.hh"
#include "util/split.hh"
#include "util/timerfd.hh"

enum MessageType
{
  pointer,
  plaintext
};

struct Message
{
  std::pair<const void*, size_t> outptr {};
  std::string plain {};
};

struct OutboundMessage
{
  MessageType message_type_ {};
  Message message {};

  OutboundMessage( std::string&& str )
    : message_type_( MessageType::plaintext )
  {
    message.plain = std::move( str );
  }

  OutboundMessage( const void* ptr, const size_t len )
    : message_type_( MessageType::pointer )
  {
    message.outptr.first = ptr;
    message.outptr.second = len;
  }
};

struct ClientHandler
{
  TCPSocket socket_recv_ {};
  std::optional<TCPSocket> socket_send_ { std::nullopt };

  RingBuffer send_buffer_ { 81920 };
  RingBuffer read_buffer_ { 81920 };

  std::string temp_inbound_message_ {};
  size_t expected_length { 4 };
  int receive_state { 0 };
  std::list<std::string> inbound_messages_ {};
  std::list<OutboundMessage> outbound_messages_ {};

  std::unordered_map<int, std::vector<OutboundMessage>> buffered_remote_responses_ {};
  std::queue<int> ordered_tags {};

  std::vector<EventLoop::RuleHandle> things_to_kill {};

  std::ofstream debug_ { "/tmp/debug1", std::ios::binary | std::ios::trunc };


  // fsm:
  // state 0: starting state, don't know expected length
  // state 1: in the middle of a message
  // check test.py to see python reference FSM for receiving

  ClientHandler( TCPSocket&& socket )
    : socket_recv_( std::move( socket ) )
  {}

  ClientHandler( TCPSocket&& socket_recv, TCPSocket&& socket_send )
    : socket_recv_( std::move( socket_recv ) )
    , socket_send_( std::move( socket_send ) ),
    debug_("/tmp/debug", std::ios::binary | std::ios::trunc )

  {}

  void parse()
  {
    temp_inbound_message_.append( read_buffer_.readable_region() );
    read_buffer_.pop( read_buffer_.readable_region().length() );
    if ( receive_state == 0 ) {
      if ( temp_inbound_message_.length() >= 4 ) {
        expected_length = *reinterpret_cast<const int*>( temp_inbound_message_.c_str() );
        // ERROR( "parse expected length " + std::to_string( expected_length ) );
        if ( temp_inbound_message_.length() > expected_length - 1 ) {
          inbound_messages_.emplace_back( move( temp_inbound_message_.substr( 4, expected_length - 4 ) ) );
          temp_inbound_message_ = temp_inbound_message_.substr( expected_length );
          expected_length = 4;
          receive_state = 0;
          return;
        } else {
          receive_state = 1;
          return;
        }
      } else {
        receive_state = 0;
        return;
      }
    } else {
      if ( temp_inbound_message_.length() > expected_length - 1 ) {
        inbound_messages_.emplace_back( move( temp_inbound_message_.substr( 4, expected_length - 4 ) ) );
        temp_inbound_message_ = temp_inbound_message_.substr( expected_length );
        expected_length = 4;
        receive_state = 0;
        return;
      } else {
        receive_state = 1;
        return;
      }
    }
  }

  void produce()
  {
    auto& message = outbound_messages_.front();
    if ( message.message_type_ == plaintext ) {
      // ERROR( "producing plaintext" );
      const size_t bytes_wrote = send_buffer_.write( message.message.plain );
      if ( bytes_wrote == message.message.plain.length() ) {
        outbound_messages_.pop_front();
      } else {
        message.message.plain = message.message.plain.substr( bytes_wrote );
      }
    } else {
      // write the memory location pointed to by this pointer to the send buffer. first create a stringview from this
      // pointer

      std::string_view a( reinterpret_cast<const char*>( message.message.outptr.first ),
                          message.message.outptr.second );
      // ERROR( "producing ptr" );
      const size_t bytes_wrote = send_buffer_.write( a );
      if ( bytes_wrote == a.length() ) {
        outbound_messages_.pop_front();
      } else {
        message.message.outptr.second -= bytes_wrote;
        message.message.outptr.first = reinterpret_cast<const void*>(
          reinterpret_cast<const char*>( message.message.outptr.first ) + bytes_wrote );
      }
    }
  }

  void install_rules( EventLoop& loop, std::function<void( void )>&& close_callback )
  {
    if ( socket_send_.has_value() ) {
      things_to_kill.push_back( loop.add_rule(
        "recv",
        Direction::In,
        socket_recv_,
        [&] { read_buffer_.read_from( socket_recv_ ); },
        [&] { return not read_buffer_.writable_region().empty(); },
        [&, f = close_callback] {
          std::cout << "client died (recv socket)" << std::endl;
          f();
        } ) );

      things_to_kill.push_back( loop.add_rule(
        "send",
        Direction::Out,
        *socket_send_,
        [&] { send_buffer_.write_to( *socket_send_ ); },
        [&] { return not send_buffer_.readable_region().empty(); },
        [&, f = close_callback] {
          std::cout << "client died (send socket)" << std::endl;
          f();
        } ) );
    } else {
      things_to_kill.push_back( loop.add_rule(
        "send/recv",
        socket_recv_,
        [&] { read_buffer_.read_from( socket_recv_ ); },
        [&] { return not read_buffer_.writable_region().empty(); },
        [&] { send_buffer_.write_to( socket_recv_ ); },
        [&] { return not send_buffer_.readable_region().empty(); },
        [&, f = close_callback] {
          std::cout << "client died (send socket)" << std::endl;
          f();
        } ) );
    }

    things_to_kill.push_back( loop.add_rule(
      "receive messages",
      [&] {
        while ( temp_inbound_message_.length() >= expected_length or not read_buffer_.readable_region().empty() ) {
          parse();
        }
      },
      [&] {
        std::cout << expected_length << std::endl;
        debug_ << read_buffer_.readable_region() << std::endl << std::endl;
        return temp_inbound_message_.length() >= expected_length or not read_buffer_.readable_region().empty();
      } ) );

    things_to_kill.push_back( loop.add_rule(
      "write responses",
      [&] {
        while ( outbound_messages_.size() > 0 and not send_buffer_.writable_region().empty() ) {
          produce();
        }
      },
      [&] { return outbound_messages_.size() > 0 and not send_buffer_.writable_region().empty(); } ) );
  }

  ~ClientHandler()
  {
    for ( auto& it : things_to_kill ) {
      it.cancel();
    }
  }
};
