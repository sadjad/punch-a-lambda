#include <chrono>
#include <fcntl.h>
#include <fstream>
#include <iostream>
#include <map>
#include <set>
#include <string>
#include <string_view>

#include "net/socket.hh"
#include "storage/LocalStorage.hpp"
#include "util/eventloop.hh"
#include "util/split.hh"
#include "util/timerfd.hh"

using namespace std;
using namespace std::chrono;

ofstream fout { "/tmp/out" };

enum class WorkerType
{
  Send,
  Recv
};

struct Worker
{
  size_t thread_id;
  Address addr;
  TCPSocket socket {};
  WorkerType type;

  size_t bytes_transferred { 0 };

  Worker( const size_t thread_id_,
          const Address& addr_,
          const WorkerType type_ )
    : thread_id( thread_id_ )
    , addr( addr_ )
    , type( type_ )
  {
    socket.set_reuseaddr();
    socket.set_blocking( false );
  }
};

string generate_random_buffer( const size_t len )
{
  srand( time( nullptr ) );
  string res( len, '\0' );
  for ( size_t i = 0; i < len; i++ ) {
    res[i] = static_cast<char>( rand() % 256 );
  }
  return res;
}

map<size_t, string> get_peer_addresses( const uint32_t thread_id,
                                        const string& master_ip,
                                        const uint16_t master_port,
                                        const uint32_t block_dim )
{
  map<size_t, string> peers;

  string msg = "my name is:" + to_string( thread_id );

  // Discover my public ip address
  TCPSocket master_socket {};
  master_socket.set_blocking( true );
  master_socket.set_reuseaddr();
  // master_socket.bind( { "0", 40001 } );
  master_socket.connect( { master_ip, master_port } );
  master_socket.write_all( msg );

  string response {};
  string buffer( 1024 * 1024, '\0' );

  while ( true ) {
    auto len = master_socket.read( { buffer } );

    if ( not len ) {
      break;
    }

    if ( buffer.substr( 0, len ).find( ";END" ) != string::npos ) {
      response += buffer.substr( 0, len - 4 );
      break;
    } else {
      response += buffer.substr( 0, len );
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
      fout << "public_addr=" << ip << " (" << thread_id << ")" << endl;
      continue;
    }

    if ( ( id % block_dim ) != ( thread_id % block_dim ) ) {
      continue;
    }

    peers.emplace( id, ip );
  }

  return peers;
}

int main( int argc, char* argv[] )
{

  auto a = new LocalStorage( 4096 );

  if ( argc < 5 ) {
    cerr
      << "Usage: lambdafunc <master_ip> <master_port> <thread_id> <block_dim> "
      << "<active-worker>..." << endl;
    return EXIT_FAILURE;
  }

  // ofstream fout { "out" };

  EventLoop loop;

  const string master_ip { argv[1] };
  const uint16_t master_port = static_cast<uint16_t>( stoul( argv[2] ) );
  const uint32_t thread_id = static_cast<uint32_t>( stoul( argv[3] ) );
  const uint32_t block_dim = static_cast<uint32_t>( stoul( argv[4] ) );

  set<uint32_t> send_workers;
  set<uint32_t> recv_workers;
  for ( int i = 5; i < argc; i++ ) {
    if ( argv[i][0] == 'x' ) {
      send_workers.insert( atoi( &argv[i][1] ) );
    } else {
      recv_workers.insert( atoi( argv[i] ) );
    }
  }

  list<Worker> peers;

  for ( auto& [peer_id, peer_ip] :
        get_peer_addresses( thread_id, master_ip, master_port, block_dim ) ) {
    peers.emplace(
      peers.end(),
      peer_id,
      Address { peer_ip, static_cast<uint16_t>( 14000 + peer_id ) },
      WorkerType::Send );

    peers.emplace(
      peers.end(),
      peer_id,
      Address { peer_ip, static_cast<uint16_t>( 18000 + peer_id ) },
      WorkerType::Recv );
  }

  string send_buffer = generate_random_buffer( 1 * 1024 * 1024 );
  string read_buffer( 1 * 1024 * 1024, '\0' );

  size_t bytes_sent = 0;
  size_t bytes_recv = 0;

  for ( auto& peer : peers ) {
    peer.socket.bind(
      { "0",
        static_cast<uint16_t>( ( peer.type == WorkerType::Send ? 18000 : 14000 )
                               + thread_id ) } );

    peer.socket.set_blocking( false );
    peer.socket.connect( peer.addr );
    /* fout << "peer[" << peer.thread_id
         << "]: " << ( peer.type == WorkerType::Send ? "(send)" : "(recv)" )
         << endl; */

    loop.add_rule(
      "peer"s + to_string( peer.thread_id ),
      peer.socket,
      [&] { bytes_recv += peer.socket.read( { read_buffer } ); },
      [&] {
        return peer.type == WorkerType::Send and recv_workers.count( thread_id )
               and send_workers.count( peer.thread_id );
      },
      [&] { bytes_sent += peer.socket.write( send_buffer ); },
      [&] {
        return peer.type == WorkerType::Recv and send_workers.count( thread_id )
               and recv_workers.count( peer.thread_id );
      },
      [&] { fout << "peer died " << peer.thread_id << endl; } );
  }

  bool terminated = false;

  TimerFD logging_timer { seconds { 1 } };

  loop.add_rule(
    "log",
    Direction::In,
    logging_timer,
    [&] {
      logging_timer.read_event();
      fout << "bytes_sent=" << bytes_sent << ",bytes_recv=" << bytes_recv
           << endl;
    },
    [] { return true; } );

  TimerFD termination_timer { seconds { 30 } };

  loop.add_rule(
    "termination",
    Direction::In,
    termination_timer,
    [&] { terminated = true; },
    [&] { return not terminated; } );

  loop.set_fd_failure_callback( [&] {
    fout << "socket error occurred" << endl;
    terminated = true;
  } );

  const auto start = steady_clock::now();

  while ( not terminated
          and loop.wait_next_event( -1 ) != EventLoop::Result::Exit )
    ;

  const auto end = steady_clock::now();

  fout << "time=" << duration_cast<milliseconds>( end - start ).count() << endl
       << "total_bytes_sent=" << bytes_sent << endl
       << "total_bytes_recv=" << bytes_recv << endl;

  return EXIT_SUCCESS;
}
