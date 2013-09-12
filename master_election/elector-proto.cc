#include "elector-proto.h"

#include <iostream>
#include <functional>

#include "persistent-connection.h"

void ElectorStub::SendPrepareRequest(
    const PrepareRequest& req,
    std::function<void(const PrepareResponse&, bool)> cb) {
  std::cout << "send prepare\n";
  PrepareResponse resp;
  char buffer[1 + sizeof(PrepareRequest)];
  buffer[0] = 'P';
  memcpy(buffer + 1, &req, sizeof(PrepareRequest));
  bool success = connection_->SendMessage(buffer, sizeof(PrepareRequest) + 1,
                                          &resp, sizeof(PrepareResponse));
  cb(resp, success);
}

void ElectorStub::SendAcceptRequest(
    const AcceptRequest& req,
    std::function<void(const AcceptResponse&, bool)> cb) {
  std::cout << "send accept\n";
  AcceptResponse resp;
  char buffer[1 + sizeof(AcceptRequest)];
  buffer[0] = 'A';
  memcpy(buffer + 1, &req, sizeof(AcceptRequest));
  bool success = connection_->SendMessage(buffer, sizeof(AcceptRequest) + 1,
                                          &resp, sizeof(AcceptResponse));
  cb(resp, success);
}
