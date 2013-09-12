#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>

#include <iostream>
#include <string>
#include <thread>
#include <vector>

#include "elector.h"
#include "elector-proto.h"
#include "persistent-connection.h"

using namespace std;

void error(const char *msg)
{
    perror(msg);
    exit(1);
}

void server_connection_thread(Elector* elector, int sockfd) {
  char buffer[256];
  while(1) {
    int n = read(sockfd,buffer,255);
    if (n < 0) {
      cerr << "ERROR reading from socket\n";
      break;
    }
    if (n == (sizeof(AcceptRequest) + 1) && buffer[0] == 'A') {
      auto resp = elector->HandleAcceptRequest(*((const AcceptRequest*)(buffer + 1)));
      n = write(sockfd, resp, sizeof(AcceptResponse));
      delete resp;
    } else if (n == (sizeof(PrepareRequest) + 1) && buffer[0] == 'P') {
      auto resp = elector->HandlePrepareRequest(*((const PrepareRequest*)(buffer + 1)));
      n = write(sockfd, resp, sizeof(PrepareResponse));
      delete resp;
    } else if (n <= 0) {
      cerr << "ERROR writing to socket\n";
      break;
    } else {
      cout << "Unrecognized message of len " << n << "\n";
    }
    if (n < 0) {
      cout << "Cannot send response\n";
      break;
    }
  }
  close(sockfd);
}

void server_thread(Elector* elector, int own_port) {
  int sockfd = socket(AF_INET, SOCK_STREAM, 0);
  if (sockfd < 0)
     error("ERROR opening socket");
  struct sockaddr_in serv_addr;
  bzero((char *) &serv_addr, sizeof(serv_addr));
  serv_addr.sin_family = AF_INET;
  serv_addr.sin_addr.s_addr = INADDR_ANY;
  serv_addr.sin_port = htons(own_port);
  if (bind(sockfd, (struct sockaddr *) &serv_addr,
           sizeof(serv_addr)) < 0) {
    error("ERROR on binding");
  }
  listen(sockfd,5);

  struct sockaddr_in cli_addr;
  socklen_t clilen = sizeof(cli_addr);
  vector<std::thread> threads;
  while (1) {
    int newsockfd = accept(sockfd,
              (struct sockaddr *) &cli_addr,
              &clilen);
    if (newsockfd < 0)
       error("ERROR on accept");
    cout << "accepted\n";
    threads.push_back(std::thread(server_connection_thread, elector, newsockfd));
  }
  for (auto& thread : threads) {
    thread.join();
  }
  close(sockfd);
}

int main(int argc, char *argv[]) {
  if (argc < 3) {
    cerr << "usage " << argv[0] << " replica_index [hostname:port]+\n";
    exit(0);
  }
  const size_t own_replica_index = atoi(argv[1]);

  vector<thread> client_threads;
  vector<ElectorStub*> replicas(argc - 2);

  int server_port = -1;
  for (size_t i = 0; i < replicas.size(); ++i) {
    const string hostport_str(argv[i + 2]);
    const int colon = hostport_str.find(':');
    const int portno = atoi(hostport_str.substr(colon + 1).c_str());

    if (own_replica_index == i) {
      server_port = portno;
      replicas[i] = NULL;
    } else {
      auto conn = new PersistentConnection(hostport_str.substr(0, colon), portno);
      replicas[i] = new ElectorStub(conn);
      client_threads.push_back(std::thread(std::bind(&PersistentConnection::Run, conn)));
    }
  }
  Elector elector(replicas);
  std::thread elector_t(std::bind(&Elector::Run, &elector));
  std::thread serv_t(server_thread, &elector, server_port);

  serv_t.join();
  elector_t.join();
  for (auto &thread : client_threads) {
    thread.join();
  }
  return 0;
}
