#include <iostream>
#include <optional>
#include <sstream>

#include "net/socket.hh"
#include "util/eventloop.hh"

using namespace std;

struct Worker
{
  TCPSocket socket;
  uint32_t id { 0 };
  size_t write_index { 0 };

  Worker( TCPSocket&& s )
    : socket( move( s ) )
  {}
};

int main( int argc, char* argv[] )
{
  if ( argc != 3 ) {
    cerr << "Usage: lambdafunc <port> <client_count>" << endl;
    return EXIT_FAILURE;
  }

  const uint16_t listen_port = static_cast<uint16_t>( stoi( argv[1] ) );
  const size_t client_count = stoull( argv[2] );
  size_t initialized_clients = 0;

  TCPSocket listen_socket {};
  listen_socket.set_reuseaddr();
  listen_socket.set_blocking( false );
  listen_socket.bind( { "0", listen_port } );
  listen_socket.listen();

  EventLoop loop;

  vector<Worker> connected_clients {};
  optional<string> message {};

  loop.add_rule(
    "incoming connection",
    Direction::In,
    listen_socket,
    [&] {
      connected_clients.emplace_back( move( listen_socket.accept() ) );
      auto& peer = connected_clients.back();

      cerr << "connection[" << connected_clients.size()
           << "]: " << peer.socket.peer_address().to_string() << endl;

      loop.add_rule(
        "peer read/write",
        peer.socket,
        [&] { // in callback
          string buffer( 4, '\0' );
          peer.socket.read( { buffer } );
          peer.id = *reinterpret_cast<uint32_t*>( buffer.data() );
          initialized_clients++;

          cerr << "peer id for " << peer.socket.peer_address().to_string()
               << " is " << peer.id << endl;
        },
        [&] { return peer.id == 0; },
        [&] { // out callback
          peer.write_index += peer.socket.write(
            string_view { *message }.substr(
              peer.write_index ) );
        },
        [&] {
          return message.has_value() && peer.write_index < message->length();
        } );
    },
    [&] { return connected_clients.size() < client_count; } );

  loop.add_rule(
    "all connected",
    [&] {
      cerr << "all peers connected" << endl;

      ostringstream oss;
      for ( auto& client : connected_clients ) {
        oss << client.id << '\t' << client.socket.peer_address().ip() << '\n';
      }

      cerr << oss.str();
      message.emplace( oss.str() );
    },
    [&] {
      return !message.has_value() and initialized_clients == client_count;
    } );

  while ( loop.wait_next_event( -1 ) != EventLoop::Result::Exit )
    ;

  return EXIT_SUCCESS;
}
