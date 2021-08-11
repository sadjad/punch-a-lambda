#include <chrono>
#include <fstream>
#include <iostream>
#include <map>
#include <string>
#include <string_view>

#include "net/socket.hh"
#include "util/eventloop.hh"
#include "util/split.hh"
#include "util/timerfd.hh"

using namespace std;
using namespace std::chrono;

struct Worker
{
  size_t thread_id;
  Address addr;
  TCPSocket socket {};

  Worker( const size_t thread_id_, const Address& addr_ )
    : thread_id( thread_id_ )
    , addr( addr_ )
  {
    socket.set_reuseaddr();
    socket.set_blocking( false );
  }
};

map<size_t, Address> get_peer_addresses( const uint32_t thread_id,
                                         const string& master_ip,
                                         const uint16_t master_port )
{
  map<size_t, Address> peers;

  string msg = "my name is:" + to_string( thread_id );

  // Discover my public ip address
  TCPSocket master_socket {};
  master_socket.set_blocking( true );
  master_socket.set_reuseaddr();
  // master_socket.bind( { "0", 40001 } );
  master_socket.connect( { master_ip, master_port } );
  master_socket.write_all( msg );

  string response {};
  string buffer( 1024, '\0' );

  while ( true ) {
    if ( not master_socket.read( { buffer } ) ) {
      break;
    }
    response += buffer;

    if ( response.find( "addresses" ) != string::npos ) {
      break;
    }
  }

  vector<string_view> peer_ips_strs;
  split( response, ';', peer_ips_strs );
  peer_ips_strs.erase( peer_ips_strs.begin() );

  for ( auto& d : peer_ips_strs ) {
    vector<string> id_ip;
    split( d, ':', id_ip );

    const uint16_t id = static_cast<uint16_t>( stoul( id_ip[0] ) );
    const string& ip = id_ip[1];

    if ( id == thread_id ) {
      continue;
    }

    cout << "peer[" << id << "]: " << ip << endl;

    peers.emplace( id, Address { ip, static_cast<uint16_t>( 14000 + id ) } );
  }

  return peers;
}

int main( int argc, char* argv[] )
{
  if ( argc != 5 ) {
    cerr
      << "Usage: lambdafunc <master_ip> <master_port> <thread_id> <block_dim>"
      << endl;
    return EXIT_FAILURE;
  }

  ofstream fout { "out" };

  EventLoop loop;

  const string master_ip { argv[1] };
  const uint16_t master_port = static_cast<uint16_t>( stoul( argv[2] ) );
  const uint32_t thread_id = static_cast<uint32_t>( stoul( argv[3] ) );

  map<size_t, Worker> peers;

  for ( auto& [peer_id, peer_addr] :
        get_peer_addresses( thread_id, master_ip, master_port ) ) {
    peers.emplace( piecewise_construct,
                   forward_as_tuple( peer_id ),
                   forward_as_tuple( peer_id, peer_addr ) );
  }

  string send_buffer( 1 * 1024 * 1024, '\0' );
  string read_buffer( 1 * 1024 * 1024, '\0' );

  size_t bytes_sent = 0;
  size_t bytes_received = 0;

  for ( auto& p : peers ) {
    const auto peer_id = p.first;
    Worker& peer = p.second;

    peer.socket.bind( { "0", static_cast<uint16_t>( 14000 + thread_id ) } );
    peer.socket.set_blocking( false );
    peer.socket.connect( peer.addr );

    loop.add_rule(
      "peer"s + to_string( peer_id ),
      peer.socket,
      [&] { bytes_received += peer.socket.read( { read_buffer } ); },
      [&] { return true; },
      [&] { bytes_sent += peer.socket.write( send_buffer ); },
      [&] { return peer_id == 2; } );
  }

  bool terminated = false;

  TimerFD termination_timer { seconds { 10 } };

  loop.add_rule(
    "termination",
    Direction::In,
    termination_timer,
    [&] { terminated = true; },
    [&] { return not terminated; } );

  const auto start = steady_clock::now();

  while ( not terminated
          and loop.wait_next_event( -1 ) != EventLoop::Result::Exit )
    ;

  const auto end = steady_clock::now();

  fout << "time=" << duration_cast<milliseconds>( end - start ).count() << endl
       << "bytes_sent=" << bytes_sent << endl
       << "bytes_received=" << bytes_received << endl;

  return EXIT_SUCCESS;
}
