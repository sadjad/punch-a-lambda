#include <chrono>
#include <fcntl.h>
#include <fstream>
#include <iostream>
#include <list>
#include <map>
#include <set>
#include <sstream>
#include <string>
#include <string_view>

#include "nat/peer.hh"
#include "storage/clienthandler.hh"
#include "storage/message.hh"

class StorageServer
{
private:
  LocalStorage my_storage_;
  std::vector<EventLoop::RuleHandle> rules_ {};
  TCPSocket ready_socket_ {};
  TCPSocket listener_socket_ {};
  std::list<ClientHandler> clients_ {};
  // must not use unordered_map because we are going to need the iterator to persist in our event loop lambda
  // declarations!
  std::map<int, ClientHandler> connections_ {};
  MessageHandler message_handler_ {};
  UniqueTagGenerator tag_generator_;
  std::unordered_map<int, ClientHandler*> outstanding_remote_requests_ {};

public:
  StorageServer( size_t size );
  void connect_lambda( std::string coordinator_ip,
                       uint16_t coordinator_port,
                       uint32_t thread_id,
                       uint32_t block_dim,
                       EventLoop& event_loop );
  void connect( std::map<size_t, std::string>& ips, EventLoop& event_loop );
  void install_rules( EventLoop& event_loop );
};

StorageServer::StorageServer( size_t size )
  : my_storage_( size )
  , rules_ {}
  , listener_socket_( [&] {
    TCPSocket listener_socket;
    listener_socket.set_blocking( false );
    listener_socket.set_reuseaddr();
    listener_socket.bind( { "127.0.0.1", 8080 } );
    listener_socket.listen();
    return listener_socket;
  }() )
  , tag_generator_( 1000 ) // supports 1000 concurrent tags, should be more than enough
{}

void StorageServer::connect_lambda( std::string coordinator_ip,
                                    uint16_t coordinator_port,
                                    uint32_t thread_id,
                                    uint32_t block_dim,
                                    EventLoop& event_loop )
{
  std::ofstream fout { "/tmp/out" };
  std::map<size_t, std::string> peer_addresses
    = get_peer_addresses( thread_id, coordinator_ip, coordinator_port, block_dim, fout );
  this->connect( peer_addresses, event_loop );
  ready_socket_.set_blocking( false );
  ready_socket_.set_reuseaddr();
  ready_socket_.bind( { "127.0.0.1", 8079 } );
  ready_socket_.listen();
}

void StorageServer::connect( std::map<size_t, std::string>& ips, EventLoop& event_loop )
{
  for ( auto& it : ips ) {
    int id = it.first;
    std::string ip = it.second;
    Address address { ip, static_cast<uint16_t>( 8000 ) };
    TCPSocket socket;
    socket.set_reuseaddr();
    socket.bind( { "0", static_cast<uint16_t>( 8000 ) } );
    // socket.set_blocking( false );
    socket.connect( address );
    auto r = connections_.emplace(
      id, ClientHandler { std::move( socket ), RingBuffer( 4096 ), RingBuffer( 4096 ), {}, 4, 0, {}, {} } );
    if ( !r.second ) {
      assert( false );
    }
    auto conn_it = r.first;

    std::cout << "opening up connection to remote socket at " << ip << std::endl;

    event_loop.add_rule(
      "http-peer",
      conn_it->second.socket_,
      [&, conn_it] {
        conn_it->second.read_buffer_.read_from( conn_it->second.socket_ );
        std::cout << conn_it->second.read_buffer_.readable_region().length() << std::endl;
      },
      [&, conn_it] { return not conn_it->second.read_buffer_.writable_region().empty(); },
      [&, conn_it] { conn_it->second.send_buffer_.write_to( conn_it->second.socket_ ); },
      [&, conn_it] { return not conn_it->second.send_buffer_.readable_region().empty(); },
      [&, conn_it] {
        std::cout << "died" << std::endl;
        conn_it->second.socket_.close();
        connections_.erase( conn_it );
      } );

    event_loop.add_rule(
      "receive messages-peer",
      [&, conn_it] { conn_it->second.parse(); },
      [&, conn_it] {
        return conn_it->second.temp_inbound_message_.length() > 0
               or not conn_it->second.read_buffer_.readable_region().empty();
      } );

    event_loop.add_rule(
      "pop messages",
      [&, conn_it] {
        std::string msg = conn_it->second.inbound_messages_.front();
        conn_it->second.inbound_messages_.pop_front();
        std::cout << "message recevid " << msg << std::endl;

        int opcode = stoi( msg.substr( 0, 1 ) );
        switch ( opcode ) {

            // new object creation in localstorage, returns the pointer value as a string
            // currently useless without shared memory, but will be useful when shared memory is implemented.
            // look up an object in localstorage and stream out its contents to the output socket

          case 1: {
            auto result = message_handler_.parse_remote_lookup( msg );
            std::string name = std::get<0>( result );
            int tag = std::get<1>( result );
            std::cout << "looking up:" << name << ";" << std::endl;
            auto a = my_storage_.locate( name );
            if ( a.has_value() ) {
              // we are actually going to just send a opcode 2 response right back to the one who sent the request.
              std::string remote_request = message_handler_.generate_remote_store_header( tag, name, a.value().size );
              OutboundMessage response_header = { plaintext, { {}, std::move( remote_request ) } };
              conn_it->second.outbound_messages_.emplace_back( std::move( response_header ) );
              OutboundMessage response = { pointer, { { a.value().ptr, a.value().size }, {} } };
              conn_it->second.outbound_messages_.emplace_back( std::move( response ) );
            } else {
              std::string message = message_handler_.generate_remote_error( tag, "can't find object" );
              OutboundMessage response = { plaintext, { {}, std::move( message ) } };
              conn_it->second.outbound_messages_.emplace_back( std::move( response ) );
            }
            break;
          }
          // remote store request from this connection, must have been initiated by a remote lookup request sent from
          // here
          case 2: {
            auto result = message_handler_.parse_remote_store( msg );
            std::string name = std::get<0>( result );
            int size = std::get<1>( result );
            int tag = std::get<2>( result );

            auto success = my_storage_.new_object_from_string( name, std::move( msg.substr( 9 + size ) ) );
            if ( success == 0 ) {
              auto requesting_client = outstanding_remote_requests_.find( tag );
              if ( requesting_client == outstanding_remote_requests_.end() ) {
                std::cout << "received remote object that nobody has asked for, storing it locally" << std::endl;
              } else {
                auto a = my_storage_.locate( name ).value();
                OutboundMessage response_header
                  = { plaintext, { {}, message_handler_.generate_local_object_header( name, a.size ) } };
                OutboundMessage response = { pointer, { { a.ptr, a.size }, {} } };
                requesting_client->second->buffered_remote_responses_[tag] = { response_header, response };
              }

            } else {
              auto requesting_client = outstanding_remote_requests_.find( tag );
              if ( requesting_client == outstanding_remote_requests_.end() ) {
                std::cout << "received remote object nobody asked for, couldn't store it" << std::endl;
              } else {
                auto a = my_storage_.locate( name );
                if ( a.has_value() ) {
                  auto b = a.value();
                  OutboundMessage response_header
                    = { plaintext, { {}, message_handler_.generate_local_object_header( name, b.size ) } };
                  OutboundMessage response = { pointer, { { b.ptr, b.size }, {} } };
                  requesting_client->second->buffered_remote_responses_[tag] = { response_header, response };
                } else {
                  OutboundMessage response
                    = { plaintext,
                        { {},
                          message_handler_.generate_local_error( "can't create new local object with ptr, object also "
                                                                 "not in storage (could it be too big?)" ) } };
                  requesting_client->second->buffered_remote_responses_[tag] = { response };
                }
              }
            }
            // reallow this tag.
            tag_generator_.allow( tag );
            break;
          }
          // delete
          case 3: {
            // parse remote delete and parse remote lookup should be the same.
            auto result = message_handler_.parse_remote_lookup( msg );
            std::string name = std::get<0>( result );
            int tag = std::get<1>( result );
            std::cout << "looking up:" << name << ";" << std::endl;
            int a = my_storage_.delete_object( name );
            if ( a == 0 ) {
              OutboundMessage response
                = { plaintext, { {}, message_handler_.generate_remote_success( tag, "deleted " + name ) } };
              conn_it->second.outbound_messages_.emplace_back( std::move( response ) );
            } else {
              OutboundMessage response
                = { plaintext, { {}, message_handler_.generate_remote_error( tag, "failed to delete " + name ) } };
              conn_it->second.outbound_messages_.emplace_back( std::move( response ) );
            }
            tag_generator_.allow( tag );
            break;
          }

          // got an opcode with an error code related to a remote request likely

          // currently remote success and remote failure get handled the same way
          case 0:
          case 5: {
            auto error = message_handler_.parse_remote_error( msg );
            int tag = std::get<1>( error );
            std::string message = std::get<0>( error );
            auto requesting_client = outstanding_remote_requests_.find( tag );
            if ( requesting_client == outstanding_remote_requests_.end() ) {
              std::cout << "received a remote message with a wierd tag, something's wrong" << std::endl;
            } else {
              OutboundMessage response = { plaintext, { {}, std::move( message ) } };
              requesting_client->second->buffered_remote_responses_[tag] = { response };
            }
            break;
          }
          default: {
            OutboundMessage response
              = { plaintext, { {}, message_handler_.generate_local_error( "unidentified opcode" ) } };
            conn_it->second.outbound_messages_.emplace_back( response );
            break;
          }
        }
      },
      [&, conn_it] { return conn_it->second.inbound_messages_.size() > 0; } );

    event_loop.add_rule(
      "write responses",
      [&, conn_it] { conn_it->second.produce(); },
      [&, conn_it] {
        return conn_it->second.outbound_messages_.size() > 0
               and not conn_it->second.send_buffer_.writable_region().empty();
      } );
  }
}

void StorageServer::install_rules( EventLoop& event_loop )
{

  event_loop.add_rule(
    "Listener",
    Direction::In,
    listener_socket_,
    [&] {
      ClientHandler new_client(
        { std::move( listener_socket_.accept() ), RingBuffer( 4096 ), RingBuffer( 4096 ), {}, 4, 0, {}, {} } );
      clients_.emplace_back( std::move( new_client ) );
      auto client_it = prev( clients_.end() );

      client_it->socket_.set_blocking( false );
      std::cout << "accepted connection" << std::endl;

      event_loop.add_rule(
        "http",
        client_it->socket_,
        [&, client_it] {
          std::cout << "http read " << std::endl;
          client_it->read_buffer_.read_from( client_it->socket_ );
          std::cout << client_it->read_buffer_.readable_region().length() << std::endl;
        },
        [&, client_it] {
          std::cout << "http read " << std::endl;
          return not client_it->read_buffer_.writable_region().empty();
        },
        [&, client_it] {
          std::cout << "http write " << std::endl;
          client_it->send_buffer_.write_to( client_it->socket_ );
        },
        [&, client_it] {
          std::cout << "http write " << std::endl;
          return not client_it->send_buffer_.readable_region().empty();
        },
        [&, client_it] {
          std::cout << "died" << std::endl;
          std::cout << "remove all references of this client in outstanding_remote_request not implemented yet"
                    << std::endl;
          client_it->socket_.close();
          clients_.erase( client_it );
        } );

      event_loop.add_rule(
        "receive messages",
        [&, client_it] { client_it->parse(); },
        [&, client_it] {
          return client_it->temp_inbound_message_.length() > 0 or not client_it->read_buffer_.readable_region().empty();
        } );

      event_loop.add_rule(
        "pop messages",
        [&, client_it] {
          std::string message = client_it->inbound_messages_.front();
          client_it->inbound_messages_.pop_front();
          std::cout << "message recevid " << message << std::endl;

          int opcode = stoi( message.substr( 0, 1 ) );
          switch ( opcode ) {

              // new object creation in localstorage, returns the pointer value as a string
              // currently useless without shared memory, but will be useful when shared memory is implemented.

            case 0: {
              int size = *reinterpret_cast<const int*>( message.c_str() + 1 );
              std::cout << "size " << size << ";" << std::endl;
              std::string name = message.substr( 5 );
              std::cout << "storing:" << name << ";" << std::endl;
              auto a = my_storage_.new_object( name, size );
              if ( a.has_value() ) {
                std::stringstream result;
                result << a.value();
                OutboundMessage response = { plaintext, { {}, std::move( result.str() ) } };
                client_it->outbound_messages_.emplace_back( std::move( response ) );
              } else {
                OutboundMessage response
                  = { plaintext, { {}, message_handler_.generate_local_error( "new object creation failed" ) } };
                client_it->outbound_messages_.emplace_back( std::move( response ) );
              }
              break;
            }

              // look up an object in localstorage and stream out its contents to the output socket

            case 1: {
              std::string name = message_handler_.parse_local_lookup( message );
              std::cout << "looking up:" << name << ";" << std::endl;
              auto a = my_storage_.locate( name );
              if ( a.has_value() ) {
                OutboundMessage response_header
                  = { plaintext, { {}, message_handler_.generate_local_object_header( name, a.value().size ) } };
                OutboundMessage response = { pointer, { { a.value().ptr, a.value().size }, {} } };
                client_it->outbound_messages_.emplace_back( std::move( response_header ) );
                client_it->outbound_messages_.emplace_back( std::move( response ) );
              } else {
                OutboundMessage response
                  = { plaintext, { {}, message_handler_.generate_local_error( "can't find object" ) } };
                client_it->outbound_messages_.emplace_back( std::move( response ) );
              }
              break;
            }

              // stores a new object by string into the localstorage

            case 2: {
              int size = *reinterpret_cast<const int*>( message.c_str() + 1 );
              std::cout << "size " << size << ";" << std::endl;
              std::string name = message.substr( 5, size );
              auto success = my_storage_.new_object_from_string( name, std::move( message.substr( 5 + size ) ) );
              if ( success == 0 ) {
                OutboundMessage response = { plaintext, { {}, std::move( "made new object with pointer" ) } };
                client_it->outbound_messages_.emplace_back( std::move( response ) );
              } else {
                OutboundMessage response
                  = { plaintext, { {}, message_handler_.generate_local_error( "can't create new object with ptr" ) } };
                client_it->outbound_messages_.emplace_back( std::move( response ) );
              }
              break;
            }

            // tells the storage server to send a get request to a remote server
            case 3: {
              auto result = message_handler_.parse_local_remote_lookup( message );
              std::string name = std::get<0>( result );
              int id = std::get<1>( result );

              // generate a unique tag for this local request which will be used to identify it
              int tag = tag_generator_.emit();
              std::string remote_request = message_handler_.generate_remote_lookup( tag, name );
              // we need to remember which client who made this request
              outstanding_remote_requests_.insert( { tag, &*client_it } );
              // push the tag into local FIFO queue to maintain response order
              client_it->ordered_tags.push( tag );

              std::cout << remote_request << std::endl;
              OutboundMessage response = { plaintext, { {}, remote_request } };
              std::cout << id << std::endl;
              connections_.at( id ).outbound_messages_.emplace_back( std::move( response ) );
              break;
            }

            case 6: {
              std::string name = message_handler_.parse_local_lookup( message );
              int result = my_storage_.delete_object( name );
              if ( result == 0 ) {
                OutboundMessage response
                  = { plaintext, { {}, message_handler_.generate_local_success( "deleted " + name ) } };
                client_it->outbound_messages_.emplace_back( std::move( response ) );
              } else {
                OutboundMessage response
                  = { plaintext, { {}, message_handler_.generate_local_error( "failed to delete " + name ) } };
                client_it->outbound_messages_.emplace_back( std::move( response ) );
              }
              break;
            }

            case 7: {
              auto result = message_handler_.parse_local_remote_lookup( message );
              std::string name = std::get<0>( result );
              int id = std::get<1>( result );
              int tag = tag_generator_.emit();
              std::string remote_request = message_handler_.generate_remote_delete( tag, name );
              outstanding_remote_requests_.insert( { tag, &*client_it } );
              client_it->ordered_tags.push( tag );

              OutboundMessage response = { plaintext, { {}, remote_request } };
              std::cout << id << std::endl;
              connections_.at( id ).outbound_messages_.emplace_back( std::move( response ) );
              break;
            }

            default: {
              OutboundMessage response
                = { plaintext, { {}, message_handler_.generate_local_error( "unidentified opcode" ) } };
              client_it->outbound_messages_.emplace_back( std::move( response ) );
              break;
            }
          }
        },
        [&, client_it] { return client_it->inbound_messages_.size() > 0; } );

      event_loop.add_rule(
        "buffer to responses",
        [&, client_it] {
          for ( auto it : client_it->buffered_remote_responses_.find( client_it->ordered_tags.front() )->second ) {
            client_it->outbound_messages_.emplace_back(
              it ); // better call the copy constructor here, we will remove the thing later.
          }
          client_it->buffered_remote_responses_.erase( client_it->ordered_tags.front() );
          client_it->ordered_tags.pop();
        },
        [&, client_it] {
          return client_it->buffered_remote_responses_.find( client_it->ordered_tags.front() )
                 != client_it->buffered_remote_responses_.end();
        } );

      event_loop.add_rule(
        "write responses",
        [&, client_it] { client_it->produce(); },
        [&, client_it] {
          return client_it->outbound_messages_.size() > 0 and not client_it->send_buffer_.writable_region().empty();
        } );
    },
    [&] { return true; } );
}

int main( int argc, char* argv[] )
{
  if ( argc != 5 ) {
    std::cerr << "Usage: MASTER_IP MASTER_PORT THREADID BLOCKDIM " << std::endl;
    return EXIT_FAILURE;
  }

  EventLoop loop;
  StorageServer echo( 200 );
  echo.install_rules( loop );
  // std::map<size_t, std::string> input {{0,argv[1]}};
  // echo.connect(input, loop);
  echo.connect_lambda( argv[1], atoi( argv[2] ), atoi( argv[3] ), atoi( argv[4] ), loop );

  loop.set_fd_failure_callback( [] {} );
  while ( loop.wait_next_event( -1 ) != EventLoop::Result::Exit )
    ;

  return EXIT_SUCCESS;
}
