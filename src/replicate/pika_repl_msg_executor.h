// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//

#ifndef PIKA_REPL_MSG_EXECUTOR_H_
#define PIKA_REPL_MSG_EXECUTOR_H_

// ConcurrentAppendModule can append entries concurrently according to
// the specificed ReplicationGroupID.
class ConcurrentAppendModule {
 public:
  BgWorkers(int worker_num);
  int StartThread();
  int Stop();

  void Schedule(const std::string& rg_id, AppendTaskArg* arg) {
    size_t index = GetHashIndex(rg_id, true);
    bg_workers_[index]->Schedule(&Executor::HandleBinlogAppend, static_cast<void*>(task_arg));
  }

 private:
  size_t GetHashIndex(std::string key, bool upper_half) {
    size_t hash_base = bg_workers_.size() / 2;
    return (str_hash(key) % hash_base) + (upper_half ? 0 : hash_base);
  }
  std::hash<std::string> str_hash;
  std::vector<pink::BGThread> bg_workers_;
};

class Executor {
 public:
  Executor(ReplicationManager* rm);

  void HandleRequest(const PeerID& peer_id,
                     InnerMessage::InnerRequest* request,
                     IOClosure* done);

  void HandleResponse(const PeerID& peer_id,
                      InnerMessage::InnerResponse* response,
                      IOClosure* done);

 public:
  static Status HandleBinlogAppend(void* arg);

 pirvate:
  // Message handle functions
  static Status SendSlaveBinlogChips();

 private:
  ReplicationManager* rm_;
  pink::ThreadPool* threads_;
  ConcurrentAppendModule* concurrent_append_;
};

#endif // PIKA_REPL_MSG_EXECUTOR_H_
