#include "persistent-connection.h"

#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include <pthread.h>

#include <iostream>

using namespace std;

const int kSecondsBetweenReconnectAttempt = 3;

PersistentConnection::PersistentConnection(const std::string& hostname, int port)
    : hostname_(hostname), port_(port), socket_(-1) {
}

bool PersistentConnection::SendMessage(const void* data, size_t n,
                                       void* result_data, size_t result_size) {
  std::lock_guard<std::mutex> l(mu_);
  bzero(result_data, result_size);
  if (IsConnected()) {
    const int num_written = write(socket_, data, n);
    if (num_written >= (int)n) {
      const int num_read = read(socket_, result_data, result_size);
      if (num_read >= (int)result_size) {
        return num_read == (int)result_size;
      }
    }
    SocketFailed();
  }
  return false;
}

void PersistentConnection::SocketFailed() {
  cerr << "socket failed\n";
  close(socket_);
  socket_ = -1;
  socket_fail_cond_.notify_one();
}

void PersistentConnection::Run() {
  hostent* server = nullptr;
  while(1) {
    if (server == nullptr) {
      server = gethostbyname(hostname_.c_str());
      if (server == nullptr) {
        cerr << "ERROR, no such host\n";
        sleep(2);
        continue;
      }
    }
    {
      std::unique_lock<std::mutex> l(mu_);
      socket_ = socket(AF_INET, SOCK_STREAM, 0);

      if (IsConnected()) {
        sockaddr_in serv_addr;
        bzero((char*) &serv_addr, sizeof(serv_addr));
        serv_addr.sin_family = AF_INET;
        bcopy((char*) server->h_addr, (char*) &serv_addr.sin_addr.s_addr,
              server->h_length);
        serv_addr.sin_port = htons(port_);

        if (connect(socket_, (sockaddr*) &serv_addr, sizeof(serv_addr)) >= 0) {
          socket_fail_cond_.wait(l, [this]() -> bool { return !IsConnected(); });
          cout << "waited, trying to reconnect\n";
        } else {
          cout << "ERROR connecting\n";
          close(socket_);
          socket_ = -1;
        }
      } else {
        cerr << "ERROR opening socket\n";
      }
    }

    sleep(kSecondsBetweenReconnectAttempt);
  }
}
