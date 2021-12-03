#include "nat/peer.hh"

#include <set>

#include "net/socket.hh"
#include "storage/clienthandler.hh"
#include "storage/local_storage.hh"
#include "storage/message.hh"
#include "util/eventloop.hh"
#include "util/split.hh"
#include "util/timerfd.hh"
#include <thread>

using namespace std;
using namespace std::chrono;

int main( int argc, char* argv[] )
{

  std::ofstream fout { "/tmp/out" };

  std::cout << argc << std::endl;
  std::cout << argv[0] << std::endl;

  int whoami = atoi( argv[1] );

  EventLoop loop;

  TCPSocket ready;
  ready.set_reuseaddr();
  ready.set_blocking( true );
  ready.connect( { "127.0.0.1",8079 } );

  cout << "storage server ready" << endl;

  TCPSocket client_socket;
  client_socket.set_reuseaddr();
  client_socket.set_blocking( false );
  client_socket.connect( { "127.0.0.1", 8080 } );

  ClientHandler new_client( std::move( client_socket ) );

  new_client.install_rules( loop, [&] {
    std::cout << "storage server died" << std::endl;
    new_client.socket_recv_.close();
  } );

  int count = 0;
  int it = 100;

  loop.add_rule(
    "print inbound messages",
    [&] {
      while ( not new_client.inbound_messages_.empty() ) {
        fout << "inbound messages" << new_client.inbound_messages_.front() << std::endl;
        new_client.inbound_messages_.pop_front();
        
        if ( count == it-1 || count == 0 ) {
          auto millisec_since_epoch = duration_cast<milliseconds>( system_clock::now().time_since_epoch() ).count();
          std::cout << millisec_since_epoch << std::endl;
        }
        count += 1;

      }
    },
    [&] { return new_client.inbound_messages_.size() > 0; } );

  TimerFD termination_timer { seconds {20 } };

  bool terminated = false;
  loop.add_rule(
    "termination", Direction::In, termination_timer, [&] { terminated = true; }, [&] { return not terminated; } );

  MessageHandler message_handler_;

  size_t size = 10000;
  std::vector<std::string> objects {};
  std::vector<std::string> names {};

  objects.reserve( it );
  names.reserve( it );

  for ( int i = 0; i < it; i++ ) {
    objects.emplace_back( size, 0 );
    names.emplace_back( "name" + to_string(i) );
  }

  if ( whoami == 0 ) // sender
  {
    for ( int i = 0; i < it; i++ ) {
      OutboundMessage request1_header { message_handler_.generate_local_object_header( names[i], objects[i].size() ) };
      OutboundMessage request1 { objects[i].data(), objects[i].size() };

      new_client.outbound_messages_.push_back( request1_header );
      new_client.outbound_messages_.push_back( request1 );

      OutboundMessage request2 { message_handler_.generate_local_lookup( names[i] ) };
      new_client.outbound_messages_.push_back( request2 );
    }

    // for (int i = 0; i < it; i ++) {
    //   OutboundMessage request = {plaintext, {{}, message_handler_.generate_local_delete(names[i])}};
    //   new_client.outbound_messages_.push_back(request);
    // }
  }

  if ( whoami == 1 ) {



    for ( int i = 0; i < it; i++ ) {
      OutboundMessage request { message_handler_.generate_local_remote_lookup( names[i], 0 ) };
      new_client.outbound_messages_.push_back( request );
    }

    seconds dura( 5 );
    std::this_thread::sleep_for( dura );
  }

  if ( whoami == 4 ) {



    for ( int i = 0; i < it; i++ ) {
      OutboundMessage request { message_handler_.generate_local_remote_lookup( names[i], 1 ) };
      new_client.outbound_messages_.push_back( request );
    }

    seconds dura( 5 );
    std::this_thread::sleep_for( dura );
  }
  
  std::string object( size, 0 );
  if ( whoami == 2 ) // benchmark
  {

    OutboundMessage request1_header { message_handler_.generate_local_object_header( "quokka", size ) };
    OutboundMessage request1 { object.data(), size };
    new_client.outbound_messages_.push_back( request1_header );
    new_client.outbound_messages_.push_back( request1 );
    for ( int i = 0; i < 1000000; i++ ) {
      OutboundMessage request2 { message_handler_.generate_local_lookup( "quokka" ) };
      new_client.outbound_messages_.push_back( request2 );
    }
  }

  if ( whoami == 3 ) // benchmark
  {

    for ( int i = 0; i < 1000000; i++ ) {
      OutboundMessage request1_header { message_handler_.generate_local_object_header( "quokka" + to_string( i ),
                                                                                       size ) };
      OutboundMessage request1 { object.data(), size };
      new_client.outbound_messages_.push_back( request1_header );
      new_client.outbound_messages_.push_back( request1 );
      OutboundMessage request { message_handler_.generate_local_delete( "quokka" + to_string( i ) ) };
      new_client.outbound_messages_.push_back( request );
    }
  }

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
