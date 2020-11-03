// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_REPL_PROGRESS_H_
#define PIKA_REPL_PROGRESS_H_

#include <deque>
#include <memory>
#include <stringstream>

#include "include/pika_define.h"
#include "include/pika_binlog_reader.h"

namespace replica {

struct SyncWinItem {
  LogOffset offset_;
  std::size_t binlog_size_;
  bool acked_;
  bool operator==(const SyncWinItem& other) const {
    return offset_.b_offset.filenum == other.offset_.b_offset.filenum
      && offset_.b_offset.offset == other.offset_.b_offset.offset;
  }
  explicit SyncWinItem(const LogOffset& offset, std::size_t binlog_size = 0)
    : offset_(offset), binlog_size_(binlog_size), acked_(false) {
  }
  std::string ToString() const {
    return offset_.ToString() + " binglog size: " + std::to_string(binlog_size_) +
      " acked: " + std::to_string(acked_);
  }
};

class SyncWindow {
 public:
  SyncWindow() :total_size_(0) {
  }
  void Push(const SyncWinItem& item);
  bool Update(const SyncWinItem& start_item, const SyncWinItem& end_item, LogOffset* acked_offset);
  int Remaining();
  std::string ToStringStatus() const {
    if (win_.empty()) {
      return "      Size: " + std::to_string(win_.size()) + "\r\n";
    } else {
      std::string res;
      res += "      Size: " + std::to_string(win_.size()) + "\r\n";
      res += ("      Begin_item: " + win_.begin()->ToString() + "\r\n");
      res += ("      End_item: " + win_.rbegin()->ToString() + "\r\n");
      return res;
    }
  }
  std::size_t GetTotalBinlogSize() {
    return total_size_;
  }
  void Reset() {
    win_.clear();
    total_size_ = 0;
  }

 private:
  // TODO(whoiami) ring buffer maybe
  std::deque<SyncWinItem> win_;
  std::size_t total_size_;
};_

class Progress {
 public:
  Progress(const PeerID& peer_id, int32_t session_id);
  ~Progress();

  class SlaveState {
   public:
    enum Code {
      kSlaveNotSync    = 0,
      kSlaveDbSync     = 1,
      kSlaveBinlogSync = 2,
    };
    const static std::string SlaveStateMsg[] = {
      "SlaveNotSync",
      "SlaveDbSync",
      "SlaveBinlogSync"
    };
    SlaveState() : code_(Code::kSlaveNotSync) { }
    SlaveState(const Code& code) : code_(code) { }
    std::string ToString() { return SlaveStateMsg[code_]; }
    void SetCode(Code code) { code_ = code; }
    Code Code() const { return code_; }
   private:
    Code code_;
  };

  class BinlogSyncState {
   public:
    enum Code {
      kNotSync       = 0;
      kReadFromCache = 1;
      kReadFromFile  = 2,
    };
    const static std::string BinlogSyncStateMsg[] = {
      "NotSync",
      "ReadFromCache",
      "ReadFromFile"
    };
    BinlogSyncState() : code_(kNotSync) { }
    BinlogSyncState(const Code& code) : code_(code) { }
    std::string ToString() { return BinlogSyncStateMsg[code_]; }
    void SetCode(Code code) { code_ = code; }
    Code Code() const { return code_; }
   private:
    Code code_;
  };

  enum StateType {
    // Do not send binlog to peer in Probe state.
    kStateProbe     = 1;
    kStateReplicate = 2;
  };

  struct Context {
    PeerID peer_id;
    int32_t session_id;
    uint64_t last_send_time_;
    uint64_t last_recv_time_;

    Context(const PeerID& _peer_id, int32_t _session_id)
      : peer_id(_peer_id),
      session_id(_session_id),
      last_send_time_(0),
      last_recv_time_(0) { }

    void SetLastRecvTime(uint64_t t) {
      last_recv_time_ = t;
    }
    void SetLastSendTime(uint64_t t) {
      last_send_time_ = t;
    }

    Status Update(const Context& ctx) {
      if (ctx.session_id != session_id) {
        return Status::Corruption("sessions are not equal");
      }
      if (last_send_time_ < ctx.last_send_time_) {
        last_send_time_ = ctx.last_send_time_;
      }
      if (last_recv_time_ < ctx.last_recv_time_) {
        last_recv_time_ = ctx.last_recv_time_;
      }
      return Status::OK();
    }
  };

  void promote() {
    slash::RWLock l(&slave_mu, true);
    is_learner = false;
  }

 public:
  slash::RMutex slave_mu;

  bool is_learner;
  Context context;
  SyncWindow sync_win;
  SlaveState slave_state;
  BinlogSyncState b_state;
  LogOffset sent_offset;
  LogOffset acked_offset;
  StateType state;
  std::shared_ptr<PikaBinlogReader> binlog_reader;

 public:
  std::string ToStringStatus();
  Status InitBinlogFileReader(const std::shared_ptr<Binlog>& binlog, const BinlogOffset& offset);
  Status Update(const LogOffset& start, const LogOffset& end, LogOffset* updated_offset);
  Status PrepareInfligtBinlog(std::vector<WriteTask>& tasks);
  Status UpdateContext(const Context& ctx) {
    slash::RWLock l(&slave_mu, true);
    context.Update(ctx);
  }
  void UpdateState(const StateType& st) {
    slash::RWLock l(&slave_mu, true);
    state = st;
  }
};

class Configuration {
  std::set<PeerID> voters;
  std::set<PeerID> learners;
};

enum VoteResult {
  kVotePending = 1;
  kVoteGranted = 2;
  kVoteLost    = 3;
};

class ProgressSet {
 public:
  ProgressSet(int consensus_level);
  ~ProgressSet();

  VoteResult VoteStatus();

 public:
  // return the session_id
  int32_t InsertVoter(const PeerID& peer_id);

  // return the session_id
  int32_t InsertLearner(const PeerID& peer_id);

  Status IsReady();

  Status Remove(const PeerID& peer_id);

  Status PromoteLeader(const PeerID& peer_id);

  std::shared_ptr<Progress> Progress(const PeerID& peer_id);

  Status Update(const PeerID& peer_id,
                const LogOffset& start,
                const LogOffset& end,
                LogOffset* updated_offset);

  Status GetInfo(std::stringstream& stream);

  int NumOfMembers() const {
    slash::RWLock l(&rw_lock_, false);
    return progress_.size();
  }

 private:
  LogOffset InternalCalCommittedIndex();
  std::vector<LogOffset> GetAllMatchedIndex();

 private:
  pthread_rwlock_t rw_lock_;

  std::unordered_map<PeerID, std::shared_ptr<Progress>> progress_;
  Configuration configuration_;
  int consensus_level_;
  int32_t session_id_;
};

} // namespace replica

#endif  // PIKA_SLAVE_NODE_H
