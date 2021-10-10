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

struct ClientHandler{
  TCPSocket socket_;
  RingBuffer send_buffer_;
  RingBuffer read_buffer_;
};

class StorageServer
{
  private:
    LocalStorage my_storage_;
    std::vector<EventLoop::RuleHandle> rules_ {};
    TCPSocket listener_socket_;
    std::list<ClientHandler> clients_;
    std::list<std::string> outbound_messages_;
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
}())
{}

void StorageServer::install_rules(EventLoop & event_loop)
{

  event_loop.add_rule(
    "Listener",
    Direction::In,
    listener_socket_,
    [&]{
      ClientHandler a({std::move( listener_socket_.accept() ), RingBuffer(4096), RingBuffer(4096)} );
      clients_.emplace_back( std::move(a) );
      auto client_it = prev( clients_.end() );
      
      client_it->socket_.set_blocking( false );
      std::cout << "accepted connection" << std::endl;

      event_loop.add_rule(
        "http",
        client_it->socket_,
        [&, client_it] { std::cout << "http read " << std::endl; client_it->read_buffer_.read_from(client_it->socket_);},
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
          client_it->socket_.close();
          clients_.erase(client_it);
        });
      
      event_loop.add_rule(
        "pop messages",
        [&, client_it] {
          std::string message;
          int length = client_it->read_buffer_.readable_region()[0];
          std::cout << length << std::endl;
          message.append(client_it->read_buffer_.readable_region().substr(0,length));
          client_it->read_buffer_.pop(length);
          std::cout << message.length() << std::endl;
          outbound_messages_.emplace_back(move(message));
          
        },
        [&, client_it] {return client_it->read_buffer_.readable_region().length() > (int)client_it->read_buffer_.readable_region()[0] - 1;}
      );

      event_loop.add_rule(
        "write responses",
        [&, client_it]{
          auto message = outbound_messages_.front();
          int bytes_wrote = client_it->send_buffer_.write(message);
          std::cout << bytes_wrote << std::endl;
          if(bytes_wrote == message.length())
          {
            outbound_messages_.pop_front();
          } else {
            message = message.substr(bytes_wrote,message.length());
          }
          std::cout << outbound_messages_.size() << std::endl;
        },
        [&, client_it] {return outbound_messages_.size() > 0 and not client_it->send_buffer_.writable_region().empty();}
      );

    },
    [&]{
      return true;
    }
  );
}

int main( int argc, char* argv[] )
{

  EventLoop loop;
  StorageServer echo(100);
  echo.install_rules(loop);
  loop.set_fd_failure_callback([]{});
   while ( loop.wait_next_event( -1 ) != EventLoop::Result::Exit );
  
  RingBuffer test(4096);
  std::cout << test.writable_region().length() << std::endl;
  std::cout << test.readable_region().length() << std::endl;

  std::string_view k = "1111111";
  test.write(k);
  std::cout << test.writable_region().length() << std::endl;
  std::cout << test.readable_region().length() << std::endl;
  std::cout << k << std::endl;
  std::string_view q = "1234567";
  test.read_from(q);
  std::cout << test.writable_region().length() << std::endl;
  std::cout << test.readable_region().length() << std::endl;
  std::cout << q << std::endl;
  std::cout << test.readable_region()[10] << std::endl;

  std::string output;
  output.append(test.readable_region().substr(0,10));
  std::cout << output << std::endl;

  test.pop(10);
  output.append(test.readable_region().substr(0,4));
  std::cout << output << std::endl;

  return EXIT_SUCCESS;
}
