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
};

struct ClientHandler
{
  TCPSocket socket_ {};
  RingBuffer send_buffer_ { 4096 };
  RingBuffer read_buffer_ { 4096 };

  std::string temp_inbound_message_ {};
  size_t expected_length { 4 };
  int receive_state { 0 };
  std::list<std::string> inbound_messages_ {};
  std::list<OutboundMessage> outbound_messages_ {};

  std::unordered_map<int, std::vector<OutboundMessage>> buffered_remote_responses_ {};
  std::queue<int> ordered_tags {};

  // fsm:
  // state 0: starting state, don't know expected length
  // state 1: in the middle of a message
  // check test.py to see python reference FSM for receiving

  void parse()
  {
    temp_inbound_message_.append( read_buffer_.readable_region() );
    read_buffer_.pop( read_buffer_.readable_region().length() );
    if ( receive_state == 0 ) {
      if ( temp_inbound_message_.length() > 4 ) {
        expected_length = *reinterpret_cast<const int*>( temp_inbound_message_.c_str() );
        std::cout << expected_length << std::endl;
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
    auto message = outbound_messages_.front();
    if ( message.message_type_ == plaintext ) {
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
      const size_t bytes_wrote = send_buffer_.write( a );
      if ( bytes_wrote == a.length() ) {
        outbound_messages_.pop_front();
      } else {
        message.message.outptr.second -= bytes_wrote;
      }
    }
  }
};