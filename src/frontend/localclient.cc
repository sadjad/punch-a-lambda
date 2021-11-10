#include "nat/peer.hh"

#include <set>

#include "net/socket.hh"
#include "storage/clienthandler.hh"
#include "storage/local_storage.hh"
#include "storage/message.hh"
#include "util/eventloop.hh"
#include "util/split.hh"
#include "util/timerfd.hh"

using namespace std;
using namespace std::chrono;

int main( int argc, char* argv[] )
{

  std::cout << argc << std::endl;
  std::cout << argv[0] << std::endl;

  EventLoop loop;

  TCPSocket ready;
  ready.set_reuseaddr();
  ready.set_blocking( true );
  ready.connect( { "127.0.0.1", 8079 } );

  cout << "storage server ready" << endl;

  TCPSocket client_socket;
  client_socket.set_reuseaddr();
  client_socket.set_blocking( false );
  client_socket.connect( { "127.0.0.1", 8080 } );

  ClientHandler new_client( std::move( client_socket ) );

  new_client.install_rules( loop, [&] {
    std::cout << "storage server died" << std::endl;
    new_client.socket_.close();
  } );

  MessageHandler message_handler_;

  std::vector<std::string> objects {};
  std::vector<std::string> names {};

  int it = 1000;

  for ( int i = 0; i < it; i++ ) {
    objects.emplace_back( "object" + to_string( i ) );
    names.emplace_back( "name" + to_string( i + 1 ) );
  }

  for ( int i = 0; i < it; i++ ) {
    OutboundMessage request1_header
      = { plaintext, { {}, message_handler_.generate_local_object_header( names[i], objects[i].size() ) } };
    OutboundMessage request1 = { pointer, { { objects[i].c_str(), objects[i].size() }, {} } };

    new_client.outbound_messages_.push_back( request1_header );
    new_client.outbound_messages_.push_back( request1 );

    OutboundMessage request2 = { plaintext, { {}, message_handler_.generate_local_lookup( names[i] ) } };

    new_client.outbound_messages_.push_back( request2 );
  }

  loop.add_rule(
    "print inbound messages",
    [&] {
      while ( not new_client.inbound_messages_.empty() ) {
        std::cout << "inbound messages" << new_client.inbound_messages_.front() << std::endl;
        new_client.inbound_messages_.pop_front();
      }
    },
    [&] { return new_client.inbound_messages_.size() > 0; } );

  TimerFD termination_timer { seconds { 30 } };

  bool terminated = false;
  loop.add_rule(
    "termination", Direction::In, termination_timer, [&] { terminated = true; }, [&] { return not terminated; } );

  loop.set_fd_failure_callback( [&] {
    std::cout << "socket error occurred" << endl;
    terminated = true;
  } );

  const auto start = steady_clock::now();

  while ( not terminated and loop.wait_next_event( -1 ) != EventLoop::Result::Exit )
    ;

  const auto end = steady_clock::now();

  std::cout << "time=" << duration_cast<milliseconds>( end - start ).count() << endl;

  return EXIT_SUCCESS;
}
