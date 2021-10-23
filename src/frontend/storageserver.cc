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

#include "clienthandler.hh"
#include "message.hh"

using namespace std;
using namespace std::chrono;

class StorageServer
{
  private:
    LocalStorage my_storage_;
    std::vector<EventLoop::RuleHandle> rules_ {};
    TCPSocket listener_socket_;
    std::list<ClientHandler> clients_;
    std::vector<ClientHandler> connections_;
    MessageHandler message_handler_;
    UniqueTagGenerator tag_generator_;
    std::unordered_map<int, ClientHandler *> outstanding_remote_requests_ {}; 
    
  public:
    StorageServer(size_t size);
    void connect(std::vector<std::string> ips, EventLoop & event_loop);
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
}()),
tag_generator_(1000) // supports 1000 concurrent tags, should be more than enough 
{}

void StorageServer::connect(std::vector<std::string> ips, EventLoop & event_loop)
{
  int count = 0;
  for(auto &ip : ips)
  {
      Address address { ip, static_cast<uint16_t>( 8000 )};
      TCPSocket socket;
      socket.bind({ "0",static_cast<uint16_t>(8000) } );
      //socket.set_blocking( false );
      socket.connect( address );
      ClientHandler conn({std::move(socket), RingBuffer(4096), RingBuffer(4096), {}, 4, 0, {}, {}});
      connections_.emplace_back(std::move(conn));
      auto conn_it = prev( connections_.end() );
      std::cout << "opening up connection to remote socket at " << ip << std::endl;
      
      event_loop.add_rule(
        "http-peer",
        conn_it->socket_,
        [&, conn_it] { std::cout << "http read " << std::endl; conn_it->read_buffer_.read_from(conn_it->socket_); std::cout <<  conn_it->read_buffer_.readable_region().length() << std::endl;},
        [&, conn_it] {
          std::cout << "http read " << std::endl;
            return not conn_it->read_buffer_.writable_region().empty(); 
        },
        [&, conn_it] { std::cout << "http write " << std::endl;  conn_it->send_buffer_.write_to(conn_it->socket_); },
        [&, conn_it] {
          std::cout << "http write " << std::endl;
            return not conn_it->send_buffer_.readable_region().empty(); 
        },
        [&, conn_it] {std::cout << "died" << std::endl;
          conn_it->socket_.close();
          connections_.erase(conn_it);
        });
      
      event_loop.add_rule(
        "receive messages-peer",
        [&, conn_it] {
            conn_it->parse(event_loop);
        },
        [&, conn_it] {std::cout << "cond:" << conn_it->temp_inbound_message_ << std::endl; return conn_it->temp_inbound_message_.length() > 0 or not conn_it->read_buffer_.readable_region().empty();}
      );

      event_loop.add_rule(
        "pop messages",
        [&, conn_it] {
          std::string message = conn_it->inbound_messages_.front();
          conn_it->inbound_messages_.pop_front();
          int length = message.length();
          std::cout << "message recevid " << message << std::endl;

          int opcode = stoi(message.substr(0,1));
          switch(opcode){

              // new object creation in localstorage, returns the pointer value as a string
              // currently useless without shared memory, but will be useful when shared memory is implemented.
              // look up an object in localstorage and stream out its contents to the output socket

              case 1:
              {
                auto result = message_handler_.parse_remote_lookup(message);
                std::string name = std::get<0>(result);
                int tag = std::get<1>(result);
                std::cout << "looking up:" << name << ";" << std::endl;
                auto a = my_storage_.locate(name);
                if(a.has_value())
                {
                  // we are actually going to just send a opcode 2 response right back to the one who sent the request. 
                   std::string remote_request = message_handler_.generate_remote_store_header(tag, name, a.value().size);
                   OutboundMessage response_header = {plaintext, {{}, move(remote_request)}};
                    conn_it->outbound_messages_.emplace_back( move( response_header ) );
                   OutboundMessage response = {pointer, {{a.value().ptr, a.value().size},{}}};
                   conn_it->outbound_messages_.emplace_back( move( response ) );
                } else {
                  std::string message = message_handler_.generate_remote_error(tag, "can't find object");
                  OutboundMessage response = {plaintext, {{},std::move(message)}};
                  conn_it->outbound_messages_.emplace_back( move(response) );
                }
                break;
              }
              case 2:
              {
                auto result = message_handler_.parse_remote_store(message);
                std::string name = std::get<0>(result);
                int size = std::get<1>(result);
                int tag = std::get<2>(result);

                auto success = my_storage_.new_object_from_string(name, move(message.substr(9+size)));
                if(success == 0)
                {
                  auto requesting_client = outstanding_remote_requests_.find(tag);
                  if(requesting_client == outstanding_remote_requests_.end())
                  {
                    std::cout << "received remote object that nobody has asked for, storing it locally" << std::endl;
                  }
                  else
                  {
                    auto a = my_storage_.locate(name).value();
                    OutboundMessage response = {pointer, {{a.ptr, a.size},{}}};
                    requesting_client->second->buffered_remote_responses_[tag] = response;
                  }

                } else {
                  auto requesting_client = outstanding_remote_requests_.find(tag);
                  if(requesting_client == outstanding_remote_requests_.end())
                  {
                    std::cout << "received remote object nobody asked for, couldn't store it" << std::endl;
                  }
                  else
                  {
                    OutboundMessage response = {plaintext, {{},message_handler_.generate_local_error("can't create new object with ptr")}};
                    requesting_client->second->buffered_remote_responses_[tag] = response;
                  }
                }
                break;
              }
              // got an opcode with an error code related to a remote request likely 
              case 5:
              {
                auto error = message_handler_.parse_remote_error(message);
                int tag = std::get<1>(error);
                std::string message = std::get<0>(error);
                auto requesting_client = outstanding_remote_requests_.find(tag);
                if(requesting_client == outstanding_remote_requests_.end())
                {
                  std::cout << "received an error with a wierd tag, something's wrong" << std::endl;
                }
                else
                {
                    OutboundMessage response = {plaintext, {{},move(message)}};
                    requesting_client->second->buffered_remote_responses_[tag] = response;
                }
              }
              default:
              {
                OutboundMessage response = {plaintext, {{},message_handler_.generate_local_error("unidentified opcode")}};
                conn_it->outbound_messages_.emplace_back( response );
                break;
              }
          }
        },
        [&, conn_it] {return conn_it->inbound_messages_.size() > 0;}
      );

      event_loop.add_rule(
        "write responses",
        [&, conn_it]{
          conn_it->produce(event_loop);
        },
        [&, conn_it] {return conn_it->outbound_messages_.size() > 0 and not conn_it->send_buffer_.writable_region().empty();}
      );

  }
  
}

void StorageServer::install_rules( EventLoop& event_loop )
{

  event_loop.add_rule(
    "Listener",
    Direction::In,
    listener_socket_,
    [&]{
      ClientHandler a({std::move( listener_socket_.accept() ), RingBuffer(4096), RingBuffer(4096), {}, 4, 0, {}, {}} );
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
          std::cout << "remove all references of this client in outstanding_remote_request not implemented yet"  << std::endl;
          client_it->socket_.close();
          clients_.erase(client_it);
        });
      

      event_loop.add_rule(
        "receive messages",
        [&, client_it] {
            client_it->parse(event_loop);
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
                   OutboundMessage response = {plaintext, {{},message_handler_.generate_local_error("new object creation failed")}};
                   client_it->outbound_messages_.emplace_back( move( response ) );
                }
                break;
              }

              // look up an object in localstorage and stream out its contents to the output socket

              case 1:
              {
                std::string name = message_handler_.parse_local_lookup(message);
                std::cout << "looking up:" << name << ";" << std::endl;
                auto a = my_storage_.locate(name);
                if(a.has_value())
                {
                   OutboundMessage response = {pointer, {{a.value().ptr, a.value().size},{}}};
                   client_it->outbound_messages_.emplace_back( move( response ) );
                } else {
                  OutboundMessage response = {plaintext, {{},message_handler_.generate_local_error("can't find object")}};
                  client_it->outbound_messages_.emplace_back( move(response) );
                }
                break;
              }

              // stores a new object by string into the localstorage

              case 2:
              {
                int size = * (int * )(message.c_str() + 1);
                std::cout << "size " << size << ";" << std::endl;
                std::string name = message.substr(5,size); 
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

              // tells the storage server to send a get request to a remote server
              case 3:
              {
                int size = * (int * )(message.c_str() + 1);
                std::cout << "size " << size << ";" << std::endl;
                std::string name = message.substr(5,size); 
                int id = *(int *)(message.c_str() + 5 + size);
                // (len(i) + 5).to_bytes(4,'little') + bytes("1","utf-8") + bytes(i,"utf-8")

                // generate a unique tag for this local request which will be used to identify it
                int tag = tag_generator_.emit();
                std::string remote_request = message_handler_.generate_remote_lookup(tag, name);
                // we need to remember which client who made this request
                outstanding_remote_requests_.insert({tag, & *client_it});
                // push the tag into local FIFO queue to maintain response order
                client_it->ordered_tags.push(tag);
                
                std::cout << id << std::endl;
                std::cout << remote_request << std::endl;
                OutboundMessage response = {plaintext, {{}, remote_request}};
                connections_[id].outbound_messages_.emplace_back(move(response));

              }

              default:
              {
                OutboundMessage response = {plaintext, {{},message_handler_.generate_local_error("unidentified opcode")}};
                client_it->outbound_messages_.emplace_back( move(response) );
                break;
              }

          }
        },
        [&, client_it] {return client_it->inbound_messages_.size() > 0;}
      );

      event_loop.add_rule(
        "buffer to responses",
        [&, client_it]{
            client_it->outbound_messages_.emplace_back(client_it->buffered_remote_responses_.find(client_it->ordered_tags.front())->second);
            client_it->buffered_remote_responses_.erase(client_it->ordered_tags.front());
            client_it->ordered_tags.pop();
        },
        [&, client_it]{return client_it->buffered_remote_responses_.find(client_it->ordered_tags.front()) != client_it->buffered_remote_responses_.end(); }
      );

      event_loop.add_rule(
        "write responses",
        [&, client_it]{
          client_it->produce(event_loop);
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
  echo.connect({argv[1]}, loop);
  loop.set_fd_failure_callback( [] {} );
  while ( loop.wait_next_event( -1 ) != EventLoop::Result::Exit );

  return EXIT_SUCCESS;
}
