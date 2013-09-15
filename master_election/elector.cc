#include "elector.h"

#include <time.h>
#include <unistd.h>

#include <atomic>
#include <string>
#include <iostream>
#include <functional>
#include <thread>

#include "elector-proto.h"

const time_t kMasterLeaseSelfTimeout = 20;
const time_t kMasterLeaseObeyTimeout = 30;
const time_t kMasterLeaseRenewBeforeTimeout = 10;

template<class M, class A, class B>
std::function<void (A, B)> NewMemberCallback2(M* obj, void (M::* fn)(A, B)) {
  return [obj, fn](A a, B b) -> void { (obj->*fn)(a, b); };
}

size_t Elector::FindOwnReplica(const std::vector<ElectorStub*>& replicas) {
  for (size_t i = 0; i < replicas.size(); ++i) {
    if (replicas[i] == NULL) {
      return i;
    }
  }
  return replicas.size();
}

void Elector::Run() {
  while (1) {
    bool perform_prepare = false, perform_accept = false;
    {
      std::lock_guard<std::mutex> l(mu_);
      if (!IsMasterElectedLocked()) {
        // Nobody elected, let's try to rule them all.
        perform_prepare = true;
        master_index_ = -1;
      } else if (IAmTheMasterLocked()) {
        if (my_proposal_sequence_nr_ < sequence_nr_) {
          // I'm the master, but there was some contender for title, bump up my sequence number
          perform_prepare = true;
        } else if (time(NULL) > master_lease_valid_until_ - kMasterLeaseRenewBeforeTimeout) {
          // I'm the master, but my lease time is near end
          perform_accept = true;
        }
      }
    }
    if (perform_prepare) {
      // let's start the election party
      PerformPreparePhase();
    } else if (perform_accept) {
      PerformAcceptPhrase();
    }
    sleep(1);
  }
}

bool Elector::IAmTheMasterLocked() const {
  return master_index_ == static_cast<int>(own_index_) && time(NULL) < master_lease_valid_until_;
}
bool Elector::IsMasterElectedLocked() const {
  return master_index_ >= 0 && time(NULL) < master_lease_valid_until_;
}

PrepareResponse* Elector::HandlePrepareRequest(const PrepareRequest& req) {
  std::cout << "handle prepare, seq=" << req.sequence_nr
            << ", proposer=" << req.proposer_index << "\n";

  auto resp = new PrepareResponse;
  std::lock_guard<std::mutex> l(mu_);
  resp->max_seen_sequence_nr = sequence_nr_;
  if (!(IsMasterElectedLocked() && req.proposer_index != master_index_) &&
      req.sequence_nr > sequence_nr_) {
    sequence_nr_ = req.sequence_nr;
    resp->ack = true;
  } else {
    resp->ack = false;
  }
  resp->master_index = master_index_;
  resp->master_lease_valid_until = master_lease_valid_until_;
  resp->responder_index = own_index_;
  return resp;
}

AcceptResponse* Elector::HandleAcceptRequest(const AcceptRequest& req) {
  std::cout << "handle accept, seq=" << req.sequence_nr
            << ", master=" << req.master_index << "\n";

  auto res = new AcceptResponse;
  std::lock_guard<std::mutex> l(mu_);
  if (!(IsMasterElectedLocked() && req.master_index != master_index_) &&
      req.sequence_nr >= sequence_nr_) {
    master_index_ = req.master_index;
    res->master_lease_valid_until = master_lease_valid_until_ =
        time(NULL) + kMasterLeaseObeyTimeout;
    res->ack = true;
  } else {
    res->ack = false;
  }
  res->max_seen_sequence_nr = sequence_nr_;
  res->responder_index = own_index_;

  return res;
}

void Elector::HandlePrepareReply(const PrepareResponse& resp, bool success) {
  if (success) {
    std::cout << "prepare reply, max_seq=" << resp.max_seen_sequence_nr << ", from="
              << resp.responder_index << ", acked=" << resp.ack << ", accepted master="
              << resp.master_index << "\n";
    std::lock_guard<std::mutex> l(mu_);
    replica_info_[resp.responder_index].acked = resp.ack;
    if (resp.master_index >= 0) {
      ++replica_info_[resp.master_index].is_master_count;
      replica_info_[resp.master_index].is_master_until = std::min(
          replica_info_[resp.master_index].is_master_until, resp.master_lease_valid_until);
    }
    sequence_nr_ = std::max(sequence_nr_, resp.max_seen_sequence_nr);
  } else {
    std::cout << "prepare reply fail\n";
  }
  comm_in_progress_.CountDown();
}

void Elector::HandleAcceptReply(const AcceptResponse& resp, bool success) {
  if (success) {
    std::cout << "accept reply, max_seq=" << resp.max_seen_sequence_nr
              << ", ack=" << resp.ack << "\n";
    std::lock_guard<std::mutex> l(mu_);
    replica_info_[resp.responder_index].acked = resp.ack;
    replica_info_[resp.responder_index].is_master_until = std::min(
        replica_info_[resp.responder_index].is_master_until, resp.master_lease_valid_until);
    sequence_nr_ = std::max(sequence_nr_, resp.max_seen_sequence_nr);
  } else {
    std::cout << "accept reply fail\n";
  }
  comm_in_progress_.CountDown();
}

void Elector::PerformPreparePhase() {
  comm_in_progress_ = Latch(replicas_.size() - 1);
  PrepareRequest prepare;
  {
    std::lock_guard<std::mutex> l(mu_);
    replica_info_.assign(replicas_.size(), ReplicaInfo());
    prepare.sequence_nr = my_proposal_sequence_nr_ = ++sequence_nr_;
  }
  prepare.proposer_index = own_index_;
  std::cout << "Starting election with seq nr=" << my_proposal_sequence_nr_ << "\n";
  for (auto* replica : replicas_) {
    if (replica != nullptr) {
      replica->SendPrepareRequest(prepare,
                                  NewMemberCallback2(this, &Elector::HandlePrepareReply));
    }
  }
  // TODO(kskalski): Instead of waiting for all responses we should proceed when there is only
  // floor(n/2) of them. Improving it will require not reusing fields for accept phase.
  comm_in_progress_.Wait();
  HandleAllPrepareResponses();
}

void Elector::HandleAllPrepareResponses() {
  bool perform_accept = false;
  {
    std::lock_guard<std::mutex> l(mu_);
    uint32_t num_acks = 0;
    for (size_t i = 0; i < replicas_.size(); ++i) {
      if (replica_info_[i].acked) {
        ++num_acks;
      }
      if (i != own_index_ &&
          replica_info_[i].is_master_count >= replicas_.size() / 2 &&
          static_cast<uint64_t>(time(nullptr)) < replica_info_[i].is_master_until) {
        std::cout << "Recovered master: " << i << "\n";
        master_index_ = i;
        master_lease_valid_until_ = replica_info_[i].is_master_until;
        return;
      }
    }
    if (num_acks >= replicas_.size() / 2) {
      perform_accept = true;
    }
  }

  if (perform_accept) {
    PerformAcceptPhrase();
  }
}

void Elector::PerformAcceptPhrase() {
  AcceptRequest accept;
  accept.master_index = own_index_;
  {
    std::lock_guard<std::mutex> l(mu_);
    replica_info_.assign(replicas_.size(), ReplicaInfo());
    accept.sequence_nr = my_proposal_sequence_nr_;
  }

  comm_in_progress_ = Latch(replicas_.size() - 1);
  for (auto* replica : replicas_) {
    if (replica != nullptr) {
      replica->SendAcceptRequest(
          accept, NewMemberCallback2(this, &Elector::HandleAcceptReply));
    }
  }
  comm_in_progress_.Wait();
  HandleAllAcceptResponses();
}

void Elector::HandleAllAcceptResponses() {
  std::lock_guard<std::mutex> l(mu_);
  uint32_t num_acks;
  time_t min_valid_lease = std::numeric_limits<time_t>::max();
  for (const auto& replica : replica_info_) {
    if (replica.acked) {
      ++num_acks;
      min_valid_lease = std::min(min_valid_lease, static_cast<time_t>(replica.is_master_until));
    }
  }
  if (num_acks >= replicas_.size() / 2) {
    if (master_index_ != static_cast<int>(own_index_)) {
      std::cout << "I'm the Master!! " << own_index_ << "\n";
    } else {
      std::cout << "Renewed mastership " << own_index_ << "\n";
    }
    master_index_ = own_index_;
    master_lease_valid_until_ = std::min(min_valid_lease, time(NULL) + kMasterLeaseSelfTimeout);
  }
}
