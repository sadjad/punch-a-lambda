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
#include "util/debug.hh"

class StorageServer
{
private:
  LocalStorage my_storage_;
  uint16_t port_;
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
  StorageServer( size_t size, const uint16_t port );
  void connect_lambda( std::string coordinator_ip,
                       uint16_t coordinator_port,
                       uint32_t thread_id,
                       uint32_t block_dim,
                       EventLoop& event_loop );
  void connect( const uint32_t my_id, std::map<size_t, std::string>& ips, EventLoop& event_loop );
  void setup_ready_socket( EventLoop& event_loop );
  void install_rules( EventLoop& event_loop );
};

StorageServer::StorageServer( size_t size, const uint16_t port )
  : my_storage_( size )
  , port_( port )
  , rules_()
  , listener_socket_( [port] {
    TCPSocket listener_socket;
    listener_socket.set_blocking( false );
    listener_socket.set_reuseaddr();
    listener_socket.bind( { "127.0.0.1", port } );
    listener_socket.listen();
    return listener_socket;
  }() )
  , tag_generator_( 300000 ) // supports 1000 concurrent tags, should be more than enough
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
  this->connect( thread_id, peer_addresses, event_loop );
}

void StorageServer::setup_ready_socket( EventLoop& event_loop )
{
  ready_socket_.set_blocking( false );
  ready_socket_.set_reuseaddr();
  ready_socket_.bind( { "127.0.0.1", static_cast<uint16_t>( port_ - 1 ) } );
  ready_socket_.listen();

  event_loop.add_rule(
    "ready_socket",
    Direction::In,
    ready_socket_,
    [&] {
      auto socket = ready_socket_.accept();
      socket.set_blocking( true );
      socket.write( "1" );
      socket.close();
    },
    [] { return true; } );
}

void StorageServer::connect( const uint32_t my_id, std::map<size_t, std::string>& ips, EventLoop& event_loop )
{
  for ( auto& it : ips ) {
    int id = it.first;
    std::string ip = it.second;

    // TCPSocket socket_send;
    // Address address_recv { ip, static_cast<uint16_t>( 10000 + id ) };
    // socket_send.set_reuseaddr();
    // socket_send.set_blocking( false );
    // socket_send.bind( { "0", static_cast<uint16_t>( 20000 + my_id ) } );
    // socket_send.connect( address_recv );

    TCPSocket socket_recv;
    Address address_send { ip, static_cast<uint16_t>( 20000 + id ) };
    socket_recv.set_reuseaddr();
    socket_recv.set_blocking( false );
    socket_recv.bind( { "0", static_cast<uint16_t>( 20000 + my_id ) } );
    socket_recv.connect( address_send );

    // auto r = connections_.emplace( std::piecewise_construct,
    //                                std::forward_as_tuple( id ),
    //                                std::forward_as_tuple( std::move( socket_recv ), std::move( socket_send ) ) );
    auto r = connections_.emplace( std::piecewise_construct,
                                   std::forward_as_tuple( id ),
                                   std::forward_as_tuple( std::move( socket_recv )) );
    if ( !r.second ) {
      assert( false );
    }
    auto conn_it = r.first;

    std::cout << "opening up connection to remote socket at " << ip << std::endl;

    conn_it->second.install_rules( event_loop, [&, conn_it] {
      DEBUGINFO( "died" );
      conn_it->second.socket_recv_.close();
      if ( conn_it->second.socket_send_.has_value() ) {
        conn_it->second.socket_send_->close();
      }
      connections_.erase( conn_it );
    } );

    event_loop.add_rule(
      "pop messages",
      [&, conn_it] {
        while ( not conn_it->second.inbound_messages_.empty() ) {
          std::string raw_message = conn_it->second.inbound_messages_.front();
          conn_it->second.inbound_messages_.pop_front();

          msg::Message message { msg::MessageType::Remote, raw_message };
          const auto tag = message.tag();

          DEBUGINFO( "message received: " + message.debug_info() );

          using MF = msg::MessageField;
          using OpCode = msg::OpCode;

          switch ( message.opcode() ) {

              // new object creation in localstorage, returns the pointer value as a string
              // currently useless without shared memory, but will be useful when shared memory is implemented.
              // look up an object in localstorage and stream out its contents to the output socket

            case OpCode::RemoteLookup: {
              const std::string& name = message.get_field( MF::Name );
              auto a = my_storage_.locate( name );

              if ( a.has_value() ) {
                DEBUGINFO( "found object: " + name );

                // we are actually going to just send a opcode 2 response right back to the one who sent the request.
                std::string remote_request = message_handler_.generate_remote_store( tag, name, a->ptr, a->size );
                OutboundMessage response { std::move( remote_request ) };
                conn_it->second.outbound_messages_.emplace_back( std::move( response ) );
              } else {
                DEBUGINFO( "did not find object: " + name );

                OutboundMessage response { message_handler_.generate_remote_error( tag, "can't find object" ) };
                conn_it->second.outbound_messages_.emplace_back( std::move( response ) );
              }
              break;
            }
            // remote store request from this connection, must have been initiated by a remote lookup request sent from
            // here
            case OpCode::RemoteStore: {
              const std::string& name = message.get_field( MF::Name );
              std::string& object = message.get_field( MF::Object );
              auto success = my_storage_.new_object_from_string( name, std::move( object ) );

              if ( success == 0 ) {
                auto requesting_client = outstanding_remote_requests_.find( tag );
                if ( requesting_client == outstanding_remote_requests_.end() ) {
                  std::cout << "received remote object that nobody has asked for, storing it locally" << std::endl;
                } else {
                  auto a = my_storage_.locate( name ).value();
                  OutboundMessage response { message_handler_.generate_local_object( name, a.ptr, a.size ) };
                  requesting_client->second->buffered_remote_responses_[tag] = { response };
                }

              } else {
                auto requesting_client = outstanding_remote_requests_.find( tag );
                if ( requesting_client == outstanding_remote_requests_.end() ) {
                  std::cout << "received remote object nobody asked for, couldn't store it" << std::endl;
                } else {
                  auto a = my_storage_.locate( name );
                  if ( a.has_value() ) {
                    auto b = a.value();
                    OutboundMessage response { message_handler_.generate_local_object( name, b.ptr, b.size ) };
                    requesting_client->second->buffered_remote_responses_[tag] = { response };
                  } else {
                    OutboundMessage response { message_handler_.generate_local_error(
                      "can't create new local object with ptr, object also "
                      "not in storage (could it be too big?)" ) };
                    requesting_client->second->buffered_remote_responses_[tag] = { response };
                  }
                }
              }
              // reallow this tag.
              tag_generator_.allow( tag );
              break;
            }
            // delete
            case OpCode::RemoteDelete: {
              // parse remote delete and parse remote lookup should be the same.
              const std::string& name = message.get_field( MF::Name );

              int a = my_storage_.delete_object( name );
              if ( a == 0 ) {
                OutboundMessage response { message_handler_.generate_remote_success( tag, "deleted " + name ) };
                conn_it->second.outbound_messages_.emplace_back( std::move( response ) );
              } else {
                OutboundMessage response { message_handler_.generate_remote_error( tag, "failed to delete " + name ) };
                conn_it->second.outbound_messages_.emplace_back( std::move( response ) );
              }
              tag_generator_.allow( tag );
              break;
            }

            // got an opcode with an error code related to a remote request likely

            // currently remote success and remote failure get handled the same way
            case OpCode::RemoteSuccess:
            case OpCode::RemoteError: {
              std::string& msg = message.get_field( MF::Message );

              auto requesting_client = outstanding_remote_requests_.find( tag );
              if ( requesting_client == outstanding_remote_requests_.end() ) {
                std::cout << "received a remote message with a wierd tag, something's wrong" << std::endl;
              } else {
                OutboundMessage response { std::move( msg ) };
                requesting_client->second->buffered_remote_responses_[tag] = { response };
              }
              break;
            }
            default: {
              OutboundMessage response { message_handler_.generate_local_error( "unidentified opcode" ) };
              conn_it->second.outbound_messages_.emplace_back( response );
              break;
            }
          }
        }
      },
      [&, conn_it] { return conn_it->second.inbound_messages_.size() > 0; } );
  }
}

void StorageServer::install_rules( EventLoop& event_loop )
{

  event_loop.add_rule(
    "Listener",
    Direction::In,
    listener_socket_,
    [&] {
      clients_.emplace_back( listener_socket_.accept() );
      auto client_it = prev( clients_.end() );

      client_it->socket_recv_.set_blocking( false );
      std::cout << "accepted connection" << std::endl;

      std::vector<EventLoop::RuleHandle> rules_to_delete;

      rules_to_delete.push_back( event_loop.add_rule(
        "pop messages",
        [&, client_it] {
          while ( not client_it->inbound_messages_.empty() ) {
            std::string raw_message = client_it->inbound_messages_.front();
            client_it->inbound_messages_.pop_front();

            msg::Message message { msg::MessageType::Local, raw_message };

            using MF = msg::MessageField;
            using OpCode = msg::OpCode;

            switch ( message.opcode() ) {

                // new object creation in localstorage, returns the pointer value as a string
                // currently useless without shared memory, but will be useful when shared memory is implemented.

                // case 0: {
                //   int size = *reinterpret_cast<const int*>( message.c_str() + 1 );
                //   std::string name = message.substr( 5 );
                //   DEBUGINFO( "storing:" + name + ";" );
                //   auto a = my_storage_.new_object( name, size );
                //   if ( a.has_value() ) {
                //     std::stringstream result;
                //     result << a.value();
                //     OutboundMessage response { std::move( result.str() ) };
                //     client_it->outbound_messages_.emplace_back( std::move( response ) );
                //   } else {
                //     OutboundMessage response { message_handler_.generate_local_error( "new object creation failed" )
                //     }; client_it->outbound_messages_.emplace_back( std::move( response ) );
                //   }
                //   break;
                // }

                // look up an object in localstorage and stream out its contents to the output socket

              case OpCode::LocalLookup: {
                const std::string& name = message.get_field( MF::Name );

                auto a = my_storage_.locate( name );
                if ( a.has_value() ) {
                  OutboundMessage response_header { message_handler_.generate_local_object_header( name,
                                                                                                   a.value().size ) };
                  std::string_view bump( reinterpret_cast<const char*>( a.value().ptr ), a.value().size );
                  OutboundMessage response { a.value().ptr, a.value().size };
                  client_it->outbound_messages_.emplace_back( std::move( response_header ) );
                  client_it->outbound_messages_.emplace_back( std::move( response ) );
                } else {
                  OutboundMessage response { message_handler_.generate_local_error( "can't find object" ) };
                  client_it->outbound_messages_.emplace_back( std::move( response ) );
                }
                break;
              }

                // stores a new object by string into the localstorage

              case OpCode::LocalStore: {
                const std::string& name = message.get_field( MF::Name );
                std::string& object = message.get_field( MF::Object );
                auto success = my_storage_.new_object_from_string( name, std::move( object ) );

                if ( success == 0 ) {
                  OutboundMessage response { message_handler_.generate_local_success(
                    "made new object with pointer" ) };
                  client_it->outbound_messages_.emplace_back( std::move( response ) );
                } else {
                  OutboundMessage response { message_handler_.generate_local_error(
                    "can't create new object with ptr" ) };
                  client_it->outbound_messages_.emplace_back( std::move( response ) );
                }
                break;
              }

              // tells the storage server to send a get request to a remote server
              case OpCode::LocalRemoteLookup: {
                const std::string& name = message.get_field( MF::Name );
                const int id = *reinterpret_cast<const int*>( message.get_field( MF::RemoteNode ).c_str() );

                // generate a unique tag for this local request which will be used to identify it
                const int tag = tag_generator_.emit();
                std::string remote_request = message_handler_.generate_remote_lookup( tag, name );
                // we need to remember which client who made this request
                outstanding_remote_requests_.insert( { tag, &*client_it } );
                // push the tag into local FIFO queue to maintain response order
                client_it->ordered_tags.push( tag );

                OutboundMessage response { std::move( remote_request ) };
                connections_.at( id ).outbound_messages_.emplace_back( std::move( response ) );
                break;
              }

              case OpCode::LocalRemoteStore:
                throw std::runtime_error( "not implemented" );

              case OpCode::LocalDelete: {
                const std::string& name = message.get_field( MF::Name );

                int result = my_storage_.delete_object( name );
                if ( result == 0 ) {
                  OutboundMessage response { message_handler_.generate_local_success( "deleted " + name ) };
                  client_it->outbound_messages_.emplace_back( std::move( response ) );
                } else {
                  OutboundMessage response { message_handler_.generate_local_error( "failed to delete " + name ) };
                  client_it->outbound_messages_.emplace_back( std::move( response ) );
                }
                break;
              }

              case OpCode::LocalRemoteDelete: {
                const std::string& name = message.get_field( MF::Name );
                const int id = *reinterpret_cast<const int*>( message.get_field( MF::RemoteNode ).c_str() );
                const int tag = tag_generator_.emit();

                std::string remote_request = message_handler_.generate_remote_delete( tag, name );
                outstanding_remote_requests_.insert( { tag, &*client_it } );
                client_it->ordered_tags.push( tag );

                OutboundMessage response { move( remote_request ) };
                connections_.at( id ).outbound_messages_.emplace_back( std::move( response ) );
                break;
              }

              default: {
                OutboundMessage response { message_handler_.generate_local_error( "unidentified opcode" ) };
                client_it->outbound_messages_.emplace_back( std::move( response ) );
                break;
              }
            }
          }
        },
        [&, client_it] { return client_it->inbound_messages_.size() > 0; } ) );

      rules_to_delete.push_back( event_loop.add_rule(
        "buffer to responses",
        [&, client_it] {
          while ( not client_it->ordered_tags.empty()
                  && client_it->buffered_remote_responses_.find( client_it->ordered_tags.front() )
                       != client_it->buffered_remote_responses_.end() ) {
            for ( auto it : client_it->buffered_remote_responses_.find( client_it->ordered_tags.front() )->second ) {
              client_it->outbound_messages_.emplace_back(
                it ); // better call the copy constructor here, we will remove the thing later.
            }
            client_it->buffered_remote_responses_.erase( client_it->ordered_tags.front() );
            client_it->ordered_tags.pop();
          }
        },
        [&, client_it] {
          return not client_it->ordered_tags.empty()
                 && client_it->buffered_remote_responses_.find( client_it->ordered_tags.front() )
                      != client_it->buffered_remote_responses_.end();
        } ) );

      client_it->install_rules( event_loop, [&, client_it, rtd = rules_to_delete] {
        std::cout << "died" << std::endl;
        std::cout << "remove all references of this client in outstanding_remote_request not implemented yet"
                  << std::endl;

        clients_.erase( client_it );

        for ( size_t i = 0; i < rtd.size(); i++ ) {
          rtd[i].cancel();
        }
      } );
    },
    [&] { return true; } );
}

int main( int argc, char* argv[] )
{
  if ( argc != 6 ) {
    std::cerr << "Usage: MASTER_IP MASTER_PORT LISTEN_PORT THREADID BLOCKDIM " << std::endl;
    return EXIT_FAILURE;
  }

  const std::string master_ip { argv[1] };
  const uint16_t master_port = static_cast<uint16_t>( std::stoul( argv[2] ) );
  const uint16_t listen_port = static_cast<uint16_t>( std::stoul( argv[3] ) );
  const uint32_t thread_id = std::stoul( argv[4] );
  const uint32_t block_dim = std::stoul( argv[5] );

  EventLoop loop;
  StorageServer storage_server( 2'000'000'000, listen_port );
  storage_server.install_rules( loop );

  // std::map<size_t, std::string> input {{0,argv[1]}};
  // storage_server.connect(input, loop);
  storage_server.connect_lambda( master_ip, master_port, thread_id, block_dim, loop );
  storage_server.setup_ready_socket( loop );
  // storage_server.set_up_local();

  loop.set_fd_failure_callback( [] {} );
  while ( loop.wait_next_event( -1 ) != EventLoop::Result::Exit )
    ;

  std::cout << loop.summary() << std::endl;

  return EXIT_SUCCESS;
}
