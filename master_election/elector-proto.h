#ifndef ELECTOR_PROTO_H_
#define ELECTOR_PROTO_H_

#include <cstring>
#include <functional>

class PersistentConnection;

// TODO(kskalski): I want PROTOCOL BUFFERS!
struct PrepareRequest {
  uint32_t sequence_nr;
  int proposer_index;
};
struct PrepareResponse {
  uint32_t max_seen_sequence_nr;
  bool ack;
  int master_index;
  uint64_t master_lease_valid_until;
};
struct AcceptRequest {
  uint32_t sequence_nr;
  int master_index;
};
struct AcceptResponse {
  bool ack;
  uint32_t max_seen_sequence_nr;
};

class ElectorStub {
 public:
   ElectorStub(PersistentConnection* connection) : connection_(connection) {}

   void SendPrepareRequest(const PrepareRequest& req,
                           std::function<void(const PrepareResponse&, bool)> cb);
   void SendAcceptRequest(const AcceptRequest& req,
                          std::function<void(const AcceptResponse&, bool)> cb);

 private:
   PersistentConnection* connection_;
};

#endif /* ELECTOR_PROTO_H_ */
