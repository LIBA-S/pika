// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_REPL_MASTER_SLAVE_H
#define PIKA_REPL_MASTER_SLAVE_H_

// MasterSlaveEmulator emulate the Master-Slave mode
class MasterSlaveEmulator {
 public:
  MasterSlaveEmulator(ReplicaManager* rm) : replica_manager_(rm) { }

  void RemoveMaster();
  bool SetMaster(std::string& master_ip, int master_port, bool force_full_sync);
  std::string master_ip();
  int master_port();
  int role();
  int repl_state();
  std::string repl_state_str();
  bool force_full_sync();
  void SetForceFullSync(bool v);
  bool ShouldMetaSync();
  void FinishMetaSync();
  bool MetaSyncDone();
  void ResetMetaSyncStatus();
  bool AllPartitionConnectSuccess();
  bool LoopPartitionStateMachine();
  void SetLoopPartitionStateMachine(bool need_loop);
  int GetMetaSyncTimestamp();
  void UpdateMetaSyncTimestamp();
  bool IsFirstMetaSync();
  void SetFirstMetaSync(bool v);

 private:
  ReplicaManager* replica_manager_;

  pthread_rwlock_t state_protector_;
  std::string master_ip_;
  unsigned short master_port_;
  int repl_state_;
  int role_;
  int last_meta_sync_timestamp_;
  bool first_meta_sync_;
  bool loop_partition_state_machine_;
  bool force_full_sync_;
};

#endif // PIKA_REPL_MASTER_SLAVE_H_
