// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_REPL_NODE_H_
#define PIKA_REPL_NODE_H_

namespace replica {

class MemLog {
 public:
  struct LogItem {
    LogItem(LogOffset _offset, Task _task)
      : offset(std::move(_offset)), task(std::move(_task)) {
    }
    LogOffset offset;
    Task task;
  };

  MemLog();
  int Size();
  void AppendLog(const LogItem& item) {
    slash::MutexLock l_logs(&logs_mu_);
    logs_.push_back(item);
    last_offset_ = item.offset;
  }
  Status PurgeLogs(const LogOffset& offset, std::vector<LogItem>* logs);
  Status GetRangeLogs(int start, int end, std::vector<LogItem>* logs);
  Status TruncateTo(const LogOffset& offset);

  void  Reset(const LogOffset& offset);

  LogOffset last_offset() {
    slash::MutexLock l_logs(&logs_mu_);
    return last_offset_;
  }
  void SetLastOffset(const LogOffset& offset) {
    slash::MutexLock l_logs(&logs_mu_);
    last_offset_ = offset;
  }
  bool FindLogItem(const LogOffset& offset, LogOffset* found_offset);

 private:
  int InternalFindLogByBinlogOffset(const LogOffset& offset);
  int InternalFindLogByLogicIndex(const LogOffset& offset);
  slash::Mutex logs_mu_;
  std::vector<LogItem> logs_;
  LogOffset last_offset_;
};


class PersistentContext {
 public:
  Context(const std::string& path);
  ~Context();

  Status Init();
  // RWLock should be held when access members.
  Status StableSave();
  void PrepareUpdateAppliedIndex(const LogOffset& offset);
  void UpdateAppliedIndex(const LogOffset& offset);
  void Reset(const LogOffset& applied_index);

  pthread_rwlock_t rwlock_;
  LogOffset applied_index_;
  SyncWindow applied_win_;

  std::string ToString() {
    std::stringstream tmp_stream;
    slash::RWLock l(&rwlock_, false);
    tmp_stream << "  Applied_index " << applied_index_.ToString() << "\r\n";
    tmp_stream << "  Applied window " << applied_win_.ToStringStatus();
    return tmp_stream.str();
  }

 private:
  std::string path_;
  slash::RWFile *save_;
  // No copying allowed;
  Context(const Context&);
  void operator=(const Context&);
};

struct ReplicationGroupNodeConfig {
  ReplicationGroupID group_id;
  std::string log_path;
  int consensus_level;
  std::weak_ptr<StateMachine> state_machine;

  ReplicationGroupNodeConfig(ReplicationGroupID _group_id,
                             std::string _log_path,
                             int _consensus_level,
                             std::weak_ptr<StateMachine> _state_machine)
    : group_id(_group_id),
    log_path(_log_path),
    consensus_level(_consensus_level),
    state_machine(_state_machine) {

  }
};

struct ReplicationGroupNodeContext {
  class RoleState {
   public:
    enum Code {
      kStateUninitialized = 0;
      kStateLeader        = 1;
      kStateCandidate     = 2;
      kStateFollower      = 3;
    };
    const static std::string RoleStateMsg[] = {
      "kStateLeader",
      "kStateCandidate",
      "kStateFollower",
      "kStateUninitialized",
    }
    RoleState() : code_(kStateUninitialized) { }
    RoleState(const Code& code) : code_(code) { }
    std::string ToString() { return RoleStateMsg[code_]; }
    Code Code() const { return code_; }
    void SetCode(Code code) { code_ = code; }
   private:
    Code code_;
  };
  // For compatibility.
  // Replicate state for a follower.
  class ReplState {
    public:
     enum Code {
       kNoConnect   = 0,
       kTryConnect  = 1,
       kTryDBSync   = 2,
       kWaitDBSync  = 3,
       kWaitReply   = 4,
       kConnected   = 5,
       kError       = 6,
       kDBNoConnect = 7
     };
     const static std::string ReplStateMsg[] = {
       "kNoConnect",
       "kTryConnect",
       "kTryDBSync",
       "kWaitDBSync",
       "kWaitReply",
       "kConnected",
       "kError",
       "kDBNoConnect"
     };
     ReplState() : code_(kNoConnect) { }
     ReplState(const Code& code) : code_(code) { }
     std::string ToString() { return ReplStateMsg[code_]; }
     Code Code() const { return code_; }
     void SetCode(Code code) { code_ = code; }
    private:
     Code code_;
  };
  struct PersistentState {
    PeerId voted_for;
    uint64_t committed_index;
    uint32_t term;
    PersistentContext applied_context;
    PersistentState(const std::string& log_path)
      : applied_context(log_path) { }
  };
  struct VolatileState {
    PeerId leader_id;
    RoleState role;
    // For compatibility
    ReplState repl_state;
    int32_t session_id;
  }
  PersistentState ps;
  VolatileState vs;
  PeerId local_id;
  ReplicationGroupID group_id;

  // For compatibility
  int consensus_level;

  ReplicationGroupNodeContext(const Config& config);
};

class ReplicationGroupNode {
 public:
  ReplicationGroupNode(const Config& cfg);

  void Init();
  void RecorverContext();

  ReplicationGroupID GroupID() {
    return context_.group_id;
  }
  Status GetInfo(std::stringstream& stream);
  Status IsSafeToBeRemoved();
  Status IsReady();
  Status Propose(const Task& task);
  void Advance(const LogOffset& offset);
  void Leave() {
    stable_logger_->Leave();
  }

  Status AddPeer(const PeerId& peer) { }
  Status AddPeer(const std::string& ip, unsigned short port, int32_t* session_id);
  Status RemovePeer(const PeerId& peer);
  Status AppendLog(const std::string& data);
  Status TransferLeadershipTo(const PeerId& id) { };

 public:
  // Handle messages from peer
  Status HandleAppendEntriesRequest(const InnerMessage& message, Closure* done); 
  Status HandleTimeoutNowRequest(const InnerMessage& message, Closure* done); 
  Status HandleVoteRequest(const InnerMessage& message, Closure* done); 
  Status HandleVoteResponse(const InnerMessage& message, Closure* done); 

 public:
  Status HandleTrySyncRequest(const InnerMessage::InnerRequest::InnerRequest* request,
                              InnerMessage::InnerResponse::InnerResponse* response);
  Status HandleTrySyncResponse(const InnerMessage::InnerResponse::InnerResponse* response);
  Status HandleDBSyncRequest(const InnerMessage::InnerRequest::InnerRequest* request,
                             InnerMessage::InnerResponse::InnerResponse* response);
  Status HandleDBSyncResponse(const InnerMessage::InnerResponse::InnerResponse* response);
  Status HandleBinlogSyncRequest(const InnerMessage::InnerRequest::InnerRequest* request,
                              InnerMessage::InnerResponse::InnerResponse* response);
  Status HandleBinlogSyncResponse(const PeerID& peer_id,
                                  const InnerMessage::InnerResponse::InnerResponse* response);

  Status SendPartitionBinlogSync(const LogOffset& ack_start,
                                 const LogOffset& ack_end,
                                 bool is_first_send = false);

 public:
  // Handle timer func
  Status HandleElectionTimeout() { }
  Status HandleVoteTimeout() { }
  Status HandleTransferLeaderTimeout() { }

 public:

  void ReportUnreachable(const PeerId& peer_id);

  uint64_t CurrentTerm();

  void UpdateTerm(uint64_t term);

  ReplicationGroupNodeContext::ReplState ReplState() {
    slash::RWLock l(&mutex_, false);
    return context_.vs.repl_state;
  }

  void SetReplState(const ReplicationGroupNodeContext::ReplState& repl_state) {
    slash::RWLock l(&mutex_, true);
    context_.vs.repl_state = repl_state;
  }

  void SetLocalIp(const std::string& local_ip) {
    slash::RWLock l(&mutex_, true);
    context_.local_id.SetIp(local_ip);
  }

  void Activate(const PeerId& leader_id, const ReplState& repl_state) {
    slash::RWLock l(&mutex_, true);
    context_.vs.leader_id = leader_id;
    context_.vs.repl_state = repl_state;
  }

  PeerId LeaderId() {
    slash::RWLock l(&mutex_, false);
    return context_.vs.leader_id;
  }

  ReplicationGroupID ReplicationGroupID() {
    slash::RWLock l(&mutex_, false);
    return context_.group_id;
  }

  Status LeaderNegotiate(const LogOffset& f_last_offset,
      bool* reject, std::vector<LogOffset>* hints);

  bool LogOffsetCheck(
    const InnerMessage::InnerRequest::TrySync& try_sync_request,
    InnerMessage::InnerResponse::TrySync* try_sync_response);


  std::vector<WriteTask> WriteTasks() {
    std::vetor<WriteTask> tasks;
    {
      slash::RWLock l(mutex_);
      write_tasks_.swap(tasks);
    }
    return std::move(tasks);
  }

 private:
  void stepLeader() { }
  void stepCandidate() { }
  void stepFollower() { }

 // The following operations are not thread safe.
 private:
  // NOTE: The logger mutex must be held.
  Status InternalAppendLog(const BinlogItem& item, const Task& task);
  Status InternalAppendBinlog(const BinlogItem& item, const Task& task);
  Status ProposeReplicaLog(const PeerID& peer_id,
                           const std::string& log,
                           const BinlogItem& attribute);

 private:

  slash::RWMutex mutex_;
  ReplicationGroupNodeContext context_;
  ProgressSet progress_set_;
  std::weak_ptr<StateMachine> fsm_caller_;
  std::unique_ptr<StableLog> stable_logger_;
  std::shared_ptr<MemLog> mem_logger_;
  std::vector<WriteTask> write_tasks_;

  // For compatibility
  slash::RWMutex session_mu_;
  int32_t session_id_;
};

} // namespace replica


#endif // PIKA_REPL_NODE_H_
