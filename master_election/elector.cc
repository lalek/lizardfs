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
template<class M, class A, class B, class C>
std::function<void (A, B)> NewMemberCallback3(M* obj, void (M::* fn)(A, B, C), C c) {
  return [obj, fn, c](A a, B b) -> void { (obj->*fn)(a, b, c); };
}

template<class T>
class CountBarrier {
 public:
  CountBarrier(uint32_t count, std::function<T> fn) : count_(count), func_(fn) {}

  void Run() {
    if (--count_ == 0) {
      func_();
      delete this;
    }
  }

 private:
  std::atomic<uint32_t> count_;
  std::function<T> func_;
};

size_t Elector::FindOwnReplica(const std::vector<ElectorStub*>& replicas) {
  for (size_t i = 0; i < replicas.size(); ++i) {
    if (replicas[i] == NULL) {
      return i;
    }
  }
  return replicas.size();
}

void Elector::Run() {
  my_proposal_.replica_is_master_count.resize(replicas_.size());
  my_proposal_.replica_is_master_until.resize(replicas_.size());
  while (1) {
    uint32_t new_sequence_nr = 0;
    bool perform_prepare = false, perform_accept = false;
    {
      std::lock_guard<std::mutex> l(mu_);
      if (!IsMasterElectedLocked()) {
        // Nobody elected, let's try to rule them all.
        perform_prepare = true;
        new_sequence_nr = ++sequence_nr_;
        master_index_ = -1;
      } else if (IAmTheMasterLocked() &&
                 time(NULL) > master_lease_valid_until_ - kMasterLeaseRenewBeforeTimeout) {
        // I'm the master, but my lease time is near end
        perform_accept = true;
        new_sequence_nr = ++sequence_nr_;
      }
    }
    if (perform_prepare) {
      // let's start the election party
      SendOutPrepareRequests(new_sequence_nr);
    } else if (perform_accept) {
      SendOutAcceptRequests(new_sequence_nr);
    }
    // TODO(kskalski): wait for started actions (at the moment they finish synchronously)
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
    master_lease_valid_until_ = time(NULL) + kMasterLeaseObeyTimeout;
    res->ack = true;
  } else {
    res->ack = false;
  }
  res->max_seen_sequence_nr = sequence_nr_;

  return res;
}

void Elector::HandlePrepareReply(const PrepareResponse& resp, bool success) {
  if (success) {
    std::cout << "prepare reply, max_seq=" << resp.max_seen_sequence_nr
              << ", accepted master=" << resp.master_index << "\n";
    std::lock_guard<std::mutex> l(mu_);
    if (resp.ack) {
      ++my_proposal_.num_acks;
    }
    if (resp.master_index >= 0) {
      ++my_proposal_.replica_is_master_count[resp.master_index];
      my_proposal_.replica_is_master_until[resp.master_index] = std::min(
          my_proposal_.replica_is_master_until[resp.master_index], resp.master_lease_valid_until);
    }
    sequence_nr_ = std::max(sequence_nr_, resp.max_seen_sequence_nr);
  } else {
    std::cout << "prepare reply fail\n";
  }
  my_proposal_.barrier->Run();
}

void Elector::HandleAcceptReply(const AcceptResponse& resp, bool success,
                                CountBarrier<void()>* barrier) {
  if (success) {
    std::cout << "accept reply, max_seq=" << resp.max_seen_sequence_nr
              << ", ack=" << resp.ack << "\n";
    std::lock_guard<std::mutex> l(mu_);
    if (resp.ack) {
      ++num_accept_acks_;
    }
    sequence_nr_ = std::max(sequence_nr_, resp.max_seen_sequence_nr);
  } else {
    std::cout << "accept reply fail\n";
  }
  barrier->Run();
}

void Elector::SendOutPrepareRequests(uint32_t sequence_nr) {
  std::cout << "Starting election with seq nr=" << my_proposal_.sequence_nr << "\n";
  my_proposal_.barrier = new CountBarrier<void()>(
      replicas_.size(), std::bind(&Elector::HandleAllPrepareResponses, this));
  my_proposal_.num_acks = 0;
  my_proposal_.replica_is_master_count.assign(replicas_.size(), 0);
  my_proposal_.replica_is_master_until.assign(replicas_.size(),
                                            std::numeric_limits<uint64_t>::max());
  PrepareRequest prepare;
  prepare.sequence_nr = my_proposal_.sequence_nr = sequence_nr;
  prepare.proposer_index = own_index_;
  for (auto* replica : replicas_) {
    if (replica != nullptr) {
      replica->SendPrepareRequest(prepare,
                                  NewMemberCallback2(this, &Elector::HandlePrepareReply));
    }
  }
  my_proposal_.barrier->Run();
}

void Elector::HandleAllPrepareResponses() {
  uint32_t accept_sequence_nr = 0;
  {
    std::lock_guard<std::mutex> l(mu_);
    for (size_t i = 0; i < replicas_.size(); ++i) {
      if (i != own_index_ &&
          my_proposal_.replica_is_master_count[i] >= replicas_.size() / 2 &&
          static_cast<uint64_t>(time(nullptr)) < my_proposal_.replica_is_master_until[i]) {
        std::cout << "Recovered master: " << i << "\n";
        master_index_ = i;
        master_lease_valid_until_ = my_proposal_.replica_is_master_until[i];
        return;
      }
    }
    if (my_proposal_.num_acks >= replicas_.size() / 2) {
      accept_sequence_nr = my_proposal_.sequence_nr;
    }
  }

  if (accept_sequence_nr > 0) {
    SendOutAcceptRequests(accept_sequence_nr);
  }
}

void Elector::SendOutAcceptRequests(uint32_t sequence_nr) {
  AcceptRequest accept;
  accept.master_index = own_index_;
  num_accept_acks_ = 0;
  accept.sequence_nr = sequence_nr;

  auto* barrier = new CountBarrier<void()>(
      replicas_.size(), std::bind(&Elector::HandleAllAcceptResponses, this));
  for (auto* replica : replicas_) {
    if (replica != nullptr) {
      replica->SendAcceptRequest(
          accept, NewMemberCallback3(this, &Elector::HandleAcceptReply, barrier));
    }
  }
  barrier->Run();
}

void Elector::HandleAllAcceptResponses() {
  std::lock_guard<std::mutex> l(mu_);
  if (num_accept_acks_ >= replicas_.size() / 2) {
    if (master_index_ != static_cast<int>(own_index_)) {
      std::cout << "I'm the Master!! " << own_index_ << "\n";
    } else {
      std::cout << "Renewed mastership " << own_index_ << "\n";
    }
    master_index_ = own_index_;
    master_lease_valid_until_ = time(NULL) + kMasterLeaseSelfTimeout;
  }
}
