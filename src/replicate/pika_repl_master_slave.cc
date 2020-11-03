// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "pika_repl_master_slave.h"

std::string MasterSlaveEmulator::master_ip() {
  slash::RWLock l(&state_protector_, false);
  return master_ip_;
}

int MasterSlaveEmulator::master_port() {
  slash::RWLock(&state_protector_, false);
  return master_port_;
}

int MasterSlaveEmulator::role() {
  slash::RWLock(&state_protector_, false);
  return role_;
}

int MasterSlaveEmulator::repl_state() {
  slash::RWLock(&state_protector_, false);
  return repl_state_;
}

std::string MasterSlaveEmulator::repl_state_str() {
  slash::RWLock(&state_protector_, false);
  switch (repl_state_) {
    case PIKA_REPL_NO_CONNECT:
      return "no connect";
    case PIKA_REPL_SHOULD_META_SYNC:
      return "should meta sync";
    case PIKA_REPL_META_SYNC_DONE:
      return "meta sync done";
    case PIKA_REPL_ERROR:
      return "error";
    default:
      return "";
  }
}

bool MasterSlaveEmulator::force_full_sync() {
  return force_full_sync_;
}

void MasterSlaveEmulator::SetForceFullSync(bool v) {
  force_full_sync_ = v;
}

void MasterSlaveEmulator::BecomeMaster() {
  slash::RWLock l(&state_protector_, true);
  role_ |= PIKA_ROLE_MASTER;
}

void MasterSlaveEmulator::SyncError() {
  slash::RWLock l(&state_protector_, true);
  repl_state_ = PIKA_REPL_ERROR;
  LOG(WARNING) << "Sync error, set repl_state to PIKA_REPL_ERROR";
}

void MasterSlaveEmulator::RemoveMaster(std::string& master_ip, int& master_port) {
  slash::RWLock l(&state_protector_, true);
  repl_state_ = PIKA_REPL_NO_CONNECT;
  role_ &= ~PIKA_ROLE_SLAVE;

  master_ip = master_ip_;
  master_port = master_port_;

  master_ip_ = "";
  master_port_ = -1;

  return;
}

bool MasterSlaveEmulator::SetMaster(std::string& master_ip,
                                    int master_port,
                                    bool force_full_sync) {
  if (master_ip == "127.0.0.1") {
    master_ip = host_;
  }
  slash::RWLock l(&state_protector_, true);
  if ((role_ ^ PIKA_ROLE_SLAVE) && repl_state_ == PIKA_REPL_NO_CONNECT) {
    master_ip_ = master_ip;
    master_port_ = master_port;
    role_ |= PIKA_ROLE_SLAVE;
    repl_state_ = PIKA_REPL_SHOULD_META_SYNC;
    first_meta_sync_ = true;
    force_full_sync_ = force_full_sync;
    return true;
  }
  return false;
}

bool MasterSlaveEmulator::ShouldMetaSync() {
  slash::RWLock l(&state_protector_, false);
  return repl_state_ == PIKA_REPL_SHOULD_META_SYNC;
}

void MasterSlaveEmulator::FinishMetaSync() {
  slash::RWLock l(&state_protector_, true);
  assert(repl_state_ == PIKA_REPL_SHOULD_META_SYNC);
  repl_state_ = PIKA_REPL_META_SYNC_DONE;
}

bool MasterSlaveEmulator::MetaSyncDone() {
  slash::RWLock l(&state_protector_, false);
  return repl_state_ == PIKA_REPL_META_SYNC_DONE;
}

void MasterSlaveEmulator::ResetMetaSyncStatus(const UnreachableReason& reason) {
  slash::RWLock sp_l(&state_protector_, true);
  if (role_ & PIKA_ROLE_SLAVE) {
    // not change by slaveof no one, so set repl_state = PIKA_REPL_SHOULD_META_SYNC,
    // continue to connect master
    repl_state_ = PIKA_REPL_SHOULD_META_SYNC;
    loop_partition_state_machine_ = false;
    replica_manager_->copysetNodeManager_->ReportUnreachable(peer_id, reason);
  }
}

bool MasterSlaveEmulator::LoopPartitionStateMachine() {
  slash::RWLock sp_l(&state_protector_, false);
  return loop_partition_state_machine_;
}

void MasterSlaveEmulator::SetLoopPartitionStateMachine(bool need_loop) {
  slash::RWLock sp_l(&state_protector_, true);
  assert(repl_state_ == PIKA_REPL_META_SYNC_DONE);
  loop_partition_state_machine_ = need_loop;
}

int MasterSlaveEmulator::GetMetaSyncTimestamp() {
  slash::RWLock sp_l(&state_protector_, false);
  return last_meta_sync_timestamp_;
}

void MasterSlaveEmulator::UpdateMetaSyncTimestamp() {
  slash::RWLock sp_l(&state_protector_, true);
  last_meta_sync_timestamp_ = time(NULL);
}

bool MasterSlaveEmulator::IsFirstMetaSync() {
  slash::RWLock sp_l(&state_protector_, true);
  return first_meta_sync_;
}

void MasterSlaveEmulator::SetFirstMetaSync(bool v) {
  slash::RWLock sp_l(&state_protector_, true);
  first_meta_sync_ = v;
}
