#ifndef ELECTOR_H_
#define ELECTOR_H_

#include <cstdint>
#include <vector>
#include <mutex>

struct PrepareRequest;
struct AcceptRequest;
struct PrepareResponse;
struct AcceptResponse;
class ElectorStub;

template<class T> class CountBarrier;

struct ProposalState {
  uint32_t sequence_nr;
  uint32_t num_acks;
  std::vector<uint32_t> replica_is_master_count;
  std::vector<uint64_t> replica_is_master_until;
  // Called by handler of each reply. Calls aggregated reply handler after all replies arrive.
  CountBarrier<void()>* barrier;
};

class Elector {
 public:
  Elector(const std::vector<ElectorStub*>& replicas)
      : replicas_(replicas),
        own_index_(FindOwnReplica(replicas)),
        sequence_nr_(0),
        master_index_(-1),
        master_lease_valid_until_(0) {
  }

  // Periodically checks if master is elected and performs new election or least re-newal.
  // Blocks infinitely, should be started in dedicated thread.
  void Run();

  // Handlers for incoming Prepare/Accept RPCs from other replicas.
  PrepareResponse* HandlePrepareRequest(const PrepareRequest& req);
  AcceptResponse* HandleAcceptRequest(const AcceptRequest& req);

 private:
  static size_t FindOwnReplica(const std::vector<ElectorStub*>& replicas);

  // Convenience functions checking if master replica is known and has valid lease.
  bool IAmTheMasterLocked() const;
  bool IsMasterElectedLocked() const;

  // Handlers for async replies from other replicas for Prepare/Accept RPCs sent out earlier.
  void HandlePrepareReply(const PrepareResponse& resp, bool success);
  void HandleAcceptReply(const AcceptResponse& resp, bool success, CountBarrier<void()>* barrier);

  // Broadcast information that this replica wants to start new election with given number.
  void SendOutPrepareRequests(uint32_t sequence_nr);
  void HandleAllPrepareResponses();

  // Broadcast information that this replica decided to be master, responses are still checked
  // to confirm that all replicas obey this sovereign act.
  void SendOutAcceptRequests(uint32_t sequence_nr);
  void HandleAllAcceptResponses();

  // All replicas including this one (replicas_[own_index_] == nullptr).
  const std::vector<ElectorStub*> replicas_;
  const size_t own_index_;

  // We perform at most one proposal or accept broadcast at a time.
  ProposalState my_proposal_;
  uint32_t num_accept_acks_;

  // Essentially sequence_nr_ reflects promise that this replica won't accept any new master
  // election sequence with lower or equal number.
  uint32_t sequence_nr_;
  // Current master: <0 not elected, >=0 index into replicas_
  int master_index_;
  time_t master_lease_valid_until_;
  std::mutex mu_;
};

#endif /* ELECTOR_H_ */
