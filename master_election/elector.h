#ifndef ELECTOR_H_
#define ELECTOR_H_

#include <condition_variable>
#include <cstdint>
#include <vector>
#include <mutex>

struct PrepareRequest;
struct AcceptRequest;
struct PrepareResponse;
struct AcceptResponse;
class ElectorStub;

// TODO(kskalski): Move to separate utilities file
class Latch {
 public:
  Latch(size_t count) : count_(count) {}
  void operator =(const Latch &other) {
    count_ = other.count_;
  }

  void CountDown() {
    std::lock_guard<std::mutex> l(mu_);
    if (--count_ == 0) {
      cond_.notify_one();
    }
  }

  void Wait() {
    std::unique_lock<std::mutex> l(mu_);
    while (count_ > 0) {
      cond_.wait(l);
    }
  }

 private:
  size_t count_;
  std::mutex mu_;
  std::condition_variable cond_;
};

struct ReplicaInfo {
  ReplicaInfo()
    : acked(false), is_master_count(0), is_master_until(std::numeric_limits<uint64_t>::max()) {}

  bool acked;
  uint32_t is_master_count;
  uint64_t is_master_until;
};

class Elector {
 public:
  Elector(const std::vector<ElectorStub*>& replicas)
      : replicas_(replicas),
        own_index_(FindOwnReplica(replicas)),
        comm_in_progress_(0),
        replica_info_(replicas.size()),
        sequence_nr_(0),
        master_index_(-1),
        master_lease_valid_until_(0) {
  }

  // Periodically checks if master is elected and performs new election or lease re-newal.
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
  void HandleAcceptReply(const AcceptResponse& resp, bool success);

  // Broadcast information that this replica wants to start new election with given number.
  void PerformPreparePhase();
  void HandleAllPrepareResponses();

  // Broadcast information that this replica decided to be master, responses are still checked
  // to confirm that all replicas obey this sovereign act.
  void PerformAcceptPhrase();
  void HandleAllAcceptResponses();

  // All replicas including this one (replicas_[own_index_] == nullptr).
  const std::vector<ElectorStub*> replicas_;
  const size_t own_index_;

  // We perform at most one proposal or accept broadcast at a time, each reply handler calls
  // latch to count down. Once all of RPCs are replied control continues to aggregate them.
  Latch comm_in_progress_;
  std::vector<ReplicaInfo> replica_info_;

  // Essentially sequence_nr_ reflects promise that this replica won't accept any new master
  // election sequence with lower or equal number.
  uint32_t sequence_nr_;
  // Sequence number used for our last election proposal, stays the same during our mastership.
  uint32_t my_proposal_sequence_nr_;
  // Current master: <0 not elected, >=0 index into replicas_
  int master_index_;
  time_t master_lease_valid_until_;
  std::mutex mu_;
};

#endif /* ELECTOR_H_ */
