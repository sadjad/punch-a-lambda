#include <chrono>
#include <fcntl.h>
#include <fstream>
#include <iostream>
#include <map>
#include <list>
#include <set>
#include <string>
#include <string_view>

#include "net/socket.hh"
#include "util/eventloop.hh"
#include "util/split.hh"
#include "util/timerfd.hh"
#include "storage/LocalStorage.hpp"
#include "net/http_client.hh"
#include "net/session.hh"

using namespace std;
using namespace std::chrono;

class StorageServer
{
  private:
    LocalStorage my_storage_;
    std::vector<EventLoop::RuleHandle> rules_ {};
    TCPSocket listener_socket_;
    string send_buffer_;
    string read_buffer_;
    HTTPResponse response_;
    std::list<TCPSocket> clients_;
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
}()),
send_buffer_(1 * 1024 * 1024, '\0'),
read_buffer_(1 * 1024 * 1024, '\0' )
{}


/*

 RuleHandle add_rule(
    const size_t category_id,
    const FileDescriptor& fd,
    const CallbackT& in_callback,
    const InterestT& in_interest,
    const CallbackT& out_callback,
    const InterestT& out_interest,
    const CallbackT& cancel = [] {} );

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
*/




void StorageServer::install_rules(EventLoop & event_loop)
{

  event_loop.add_rule(
    "Listener",
    Direction::In,
    listener_socket_,
    [&]{
      clients_.emplace_back( move( listener_socket_.accept() ) );
      auto client_it = prev( clients_.end() );
      
      //client_it->set_blocking( false );
      std::cout << "accepted connection" << std::endl;

      event_loop.add_rule(
        "http",
        *client_it,
        [&] { int bytes_recv = client_it->read( { read_buffer_ } ); std::cout << bytes_recv << std::endl; },
        [&] {
            return true; // how to check if the socket is readable?
        },
        [&] { int bytes_sent = client_it->write( send_buffer_ ); },
        [&] {
            return true; // how to check if the socket is writeable?
        },
        [&] {std::cout << "died" << std::endl;
          clients_.erase(client_it);
        });
    },
    [&]{
      return true;
    }
  );

  // auto & tcp_sock = http_.session().socket();
  // event_loop.add_rule(
  //   "http",
  //   tcp_sock,
  //   [&] { simple_string_span test; tcp_sock.read(test); read_buffer_.read_from(test);},
  //   [&] {
  //       return true; // how to check if the socket is readable?
  //   },
  //   [&] { write_buffer_.write_to(tcp_sock);},
  //   [&] {
  //       return true; // how to check if the socket is writeable?
  //   }
  // );

  // rules_.push_back( event_loop.add_rule(
  //       "HTTP write",
  //       [&] { http_.write(write_buffer_); },
  //       [&] {
  //           return ( not http_.requests_empty()) and ( not write_buffer_.writable_region().empty() ); // how to check if the socket is writeable?
  //       } ) );
  // rules_.push_back( event_loop.add_rule(
  //       "HTTP read",
  //       [&] { http_.read(read_buffer_);},
  //       [&] {
  //           return ( not read_buffer_.readable_region().empty() ); // how to check if the socket is writeable?
  //       } ) );
}

int main( int argc, char* argv[] )
{

  EventLoop loop;
  StorageServer echo(100);
  echo.install_rules(loop);
  loop.set_fd_failure_callback([]{});
   while ( loop.wait_next_event( -1 ) != EventLoop::Result::Exit );
  
  return EXIT_SUCCESS;
}
