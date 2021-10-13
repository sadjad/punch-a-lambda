#include <chrono>
#include <fcntl.h>
#include <fstream>
#include <iostream>
#include <list>
#include <map>
#include <set>
#include <string>
#include <string_view>
#include <sstream>

#include "net/http_client.hh"
#include "net/session.hh"
#include "net/socket.hh"
#include "storage/LocalStorage.hpp"
#include "util/eventloop.hh"
#include "util/split.hh"
#include "util/timerfd.hh"

using namespace std;
using namespace std::chrono;

struct ClientHandler{
  TCPSocket socket_;
  RingBuffer send_buffer_;
  RingBuffer read_buffer_;

  std::string temp_inbound_message_;
  int expected_length = 4;
  int receive_state = 0;
  std::list<std::string> inbound_messages_;
  std::list<OutboundMessage> outbound_messages_;

};

enum MessageType {pointer, plaintext};

struct Message{
  std::pair<const void *, size_t> outptr;
  std::string plain;
};

struct OutboundMessage{
  MessageType message_type_;
  Message message;
};

class StorageServer
{
  private:
    LocalStorage my_storage_;
    std::vector<EventLoop::RuleHandle> rules_ {};
    TCPSocket listener_socket_;
    std::list<ClientHandler> clients_;

    
  public:
    StorageServer(size_t size);
    void install_rules(EventLoop & event_loop);

};

StorageServer::StorageServer(size_t size):
my_storage_(size),
rules_ {},
listener_socket_( [&]{
  TCPSocket listener_socket;
  listener_socket.set_blocking( false );
  listener_socket.set_reuseaddr();
  listener_socket.bind( { "127.0.0.1",  8080} );
  listener_socket.listen();
  return listener_socket;
}())
{}

void StorageServer::install_rules( EventLoop& event_loop )
{

  event_loop.add_rule(
    "Listener",
    Direction::In,
    listener_socket_,
    [&]{
      ClientHandler a({std::move( listener_socket_.accept() ), RingBuffer(4096), RingBuffer(4096)} );
      clients_.emplace_back( std::move(a) );
      auto client_it = prev( clients_.end() );
      
      client_it->socket_.set_blocking( false );
      std::cout << "accepted connection" << std::endl;

      event_loop.add_rule(
        "http",
        client_it->socket_,
        [&, client_it] { std::cout << "http read " << std::endl; client_it->read_buffer_.read_from(client_it->socket_); std::cout <<  client_it->read_buffer_.readable_region().length() << std::endl;},
        [&, client_it] {
          std::cout << "http read " << std::endl;
            return not client_it->read_buffer_.writable_region().empty(); 
        },
        [&, client_it] { std::cout << "http write " << std::endl;  client_it->send_buffer_.write_to(client_it->socket_); },
        [&, client_it] {
          std::cout << "http write " << std::endl;
            return not client_it->send_buffer_.readable_region().empty(); 
        },
        [&, client_it] {std::cout << "died" << std::endl;
          client_it->socket_.close();
          clients_.erase(client_it);
        });
      

      // fsm:
      // state 0: starting state, don't know expected length
      // state 1: in the middle of a message
      // check test.py to see python reference FSM for receiving

      event_loop.add_rule(
        "receive messages",
        [&, client_it] {
            client_it->temp_inbound_message_.append(client_it->read_buffer_.readable_region());
            client_it->read_buffer_.pop(client_it->read_buffer_.readable_region().length());
            if(client_it->receive_state == 0)
            {
              if(client_it->temp_inbound_message_.length() > 4)
              {
                client_it->expected_length =  * (int * )(client_it->temp_inbound_message_.c_str() );
                std::cout << client_it->expected_length << std::endl;
                if(client_it->temp_inbound_message_.length() > client_it->expected_length - 1)
                {
                  client_it->inbound_messages_.emplace_back(move(client_it->temp_inbound_message_.substr(4, client_it->expected_length - 4)));
                  client_it->temp_inbound_message_ = client_it->temp_inbound_message_.substr(client_it->expected_length);
                  client_it->expected_length = 4;
                  client_it->receive_state = 0;
                  return; 
                } else{
                  client_it->receive_state = 1;
                  return;
                }
              } else {
                client_it->receive_state = 0;
                return;
              }
            } else {
              if(client_it->temp_inbound_message_.length() > client_it->expected_length - 1)
              {
                client_it->inbound_messages_.emplace_back(move(client_it->temp_inbound_message_.substr(4, client_it->expected_length - 4)));
                client_it->temp_inbound_message_ = client_it->temp_inbound_message_.substr(client_it->expected_length);
                client_it->expected_length = 4;
                client_it->receive_state = 0;
                return;
              } else {
                client_it->receive_state = 1;
                return;
              }
            }
        },
        [&, client_it] {std::cout << "cond:" << client_it->temp_inbound_message_ << std::endl; return client_it->temp_inbound_message_.length() > 0 or not client_it->read_buffer_.readable_region().empty();}
      );

      event_loop.add_rule(
        "pop messages",
        [&, client_it] {
          std::string message = client_it->inbound_messages_.front();
          client_it->inbound_messages_.pop_front();
          int length = message.length();
          std::cout << "message recevid " << message << std::endl;

          int opcode = stoi(message.substr(0,1));
          switch(opcode){

              // new object creation in localstorage, returns the pointer value as a string
              // currently useless without shared memory, but will be useful when shared memory is implemented.

              case 0:
              {
                int size = * (int * )(message.c_str() + 1);
                std::cout << "size " << size << ";" << std::endl;
                std::string name = message.substr(5);
                std::cout << "storing:" << name << ";" << std::endl;
                auto a = my_storage_.new_object(name,size);
                if (a.has_value())
                {
                   std::stringstream result;
                   result << a.value();
                   OutboundMessage response = {plaintext, {{},move(result.str())}};
                   client_it->outbound_messages_.emplace_back( move( response ) );
                } else {
                   OutboundMessage response = {plaintext, {{},"new object creation failed"}};
                   client_it->outbound_messages_.emplace_back( move( response ) );
                }
                break;
              }

              // look up an object in localstorage and stream out its contents to the output socket

              case 1:
              {
                std::string name = message.substr(1);
                std::cout << "looking up:" << name << ";" << std::endl;
                auto a = my_storage_.locate(name);
                if(a.has_value())
                {
                   OutboundMessage response = {pointer, {{a.value().ptr, a.value().size},{}}};
                   client_it->outbound_messages_.emplace_back( move( response ) );
                } else {
                  OutboundMessage response = {plaintext, {{},"can't find object"}};
                  client_it->outbound_messages_.emplace_back( move(response) );
                }
                break;
              }
              case 2:
              {
                int size = * (int * )(message.c_str() + 1);
                std::cout << "size " << size << ";" << std::endl;
                std::string name = message.substr(5,size); // name can only be 4 characters for now 

                auto success = my_storage_.new_object_from_string(name, move(message.substr(5+size)));
                if(success == 0)
                {
                  OutboundMessage response = {plaintext, {{},move("made new object with pointer")}};
                  client_it->outbound_messages_.emplace_back( move(response) );
                } else {
                  OutboundMessage response = {plaintext, {{},move("can't create new object with ptr")}};
                  client_it->outbound_messages_.emplace_back( move(response) );
                }
                break;
              }
              default:
              {
                OutboundMessage response = {plaintext, {{},move("unidentified opcode")}};
                client_it->outbound_messages_.emplace_back( response );
                break;
              }
          }
        },
        [&, client_it] {return client_it->inbound_messages_.size() > 0;}
      );

      event_loop.add_rule(
        "write responses",
        [&, client_it]{
          auto message = client_it->outbound_messages_.front();
          if(message.message_type_ == plaintext){
            int bytes_wrote = client_it->send_buffer_.write(message.message.plain);
            if ( bytes_wrote == message.message.plain.length() ) {
              client_it->outbound_messages_.pop_front();
            } else {
              message.message.plain = message.message.plain.substr( bytes_wrote );
            }
          } else {
            // write the memory location pointed to by this pointer to the send buffer. first create a stringview from this pointer
            std::string_view a ((const char *) message.message.outptr.first, message.message.outptr.second);
            int bytes_wrote = client_it->send_buffer_.write(a);
            if ( bytes_wrote == a.length() ) {
              client_it->outbound_messages_.pop_front();
            } else {
              message.message.outptr.second -= bytes_wrote;
            }
          }
          
        },
        [&, client_it] {return client_it->outbound_messages_.size() > 0 and not client_it->send_buffer_.writable_region().empty();}
      );

    },
    [&] { return true; } );
}

int main( int argc, char* argv[] )
{

  EventLoop loop;
  StorageServer echo( 200 );
  echo.install_rules( loop );
  loop.set_fd_failure_callback( [] {} );
  while ( loop.wait_next_event( -1 ) != EventLoop::Result::Exit );

  return EXIT_SUCCESS;
}
