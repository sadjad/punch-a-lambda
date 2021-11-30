#include "peer.hh"

#include <vector>

#include "net/socket.hh"
#include "util/split.hh"

using namespace std;

map<size_t, string> get_peer_addresses( const uint32_t thread_id,
                                        const string& master_ip,
                                        const uint16_t master_port,
                                        const uint32_t block_dim,
                                        ofstream& fout )
{
  map<size_t, string> peers;

  string msg = "name:" + to_string( thread_id );

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
