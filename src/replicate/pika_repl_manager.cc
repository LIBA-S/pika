// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "pika_repl_manager.h"

ReplicationManager::ReplicationManager()
  : executor_(this),
  node_manager_(this) {
}

ReplicationManager::~ReplicationManager() {
  if (transporter_ != nullptr) {
    delete transporter_;
    return;
  }
}

Status ReplicationManager::Propose(const CopysetId& copyset_id, const Task& task) {
  node_manager_->Propose(copyset_id, task);
}

void ReplicationManager::Process(std::shared_ptr<InnerMessage::InnerRequest> req, Closure* done) {
  executor_.HandleRequest(req, done);
}

void ReplicationManager::ReportUnreachable(const NodeId& peer_id, const UnreachableReason& reason) {
  if (master_slave_ != nullptr) {
    master_slave_->ResetMetaSyncStatus(reason);
  } else {
    node_manager_->ReportUnreachable(peer_id, reason);
  }
}

void ReplicationManager::OnMetaSyncResponsed() {
  // 1. Enable binlog
  conf_->SetWriteBinlog("yes");

  // 2. Active all follower copysets
  ReplState state = ReplState::kTryConnect;
  if (master_slave_ != nullptr && master_slave_->force_full_sync()) {
    state = ReplState::kTryDBSync;
  }
  node_manager_->ActiveFollowerCopysets(LeaderId(), repl_state);

  // 3. Update state machine
  if (master_slave_ != nullptr) {
    master_slave_->FinishMetaSync();
  }
}

void ReplicationManager::PreparePartitionTrySync() {
  LOG(INFO) << "Mark try connect finish";
}

//PeerId ReplicationManager::LeaderId() {
//  slash::RWLock rwl(&state_protector_, false);
//  return PeerId(leader_ip_, leader_port_);
//}

void ReplicationManager::RemoveMaster() {
  if (master_slave_ == nullptr) {
    LOG(ERROR) << "Do not in Master & Slave mode";
    return;
  }
  std::string cur_master_ip;
  int cur_master_port
  master_slave_->RemoveMaster(master_ip, master_port);

  if (cur_master_ip != "" && cur_master_port != -1) {
    // Remove connection:
    // 1. remove the node
    // 2. remove the underlying connection.
    node_manager_->RemovePeer(PeerID(cur_master_ip, cur_master_port));
    node_manager_->DoSameThingEveryReplicationGroup(
        ReplicationGroupManager::TaskType::kRemovePeer);
    transporter_->RemovePeer(PeerID(cur_master_ip, cur_master_port + kPortShiftReplServer));
    //g_pika_rm->CloseReplClientConn(master_ip_, master_port_ + kPortShiftReplServer);
    //g_pika_rm->LostConnection(master_ip_, master_port_);
    //UpdateMetaSyncTimestamp();
    LOG(INFO) << "Remove Master Success, ip_port: " << master_ip_ << ":" << master_port_;
  }
  node_manager_->DoSameThingEveryReplicationGroup(
      ReplicationGroupManager::TaskType::kResetReplState);
}

bool ReplicationManager::SetMaster(std::string& master_ip, int master_port, bool force_full_sync) {
  if (master_slave_ == nullptr) {
    LOG(ERROR) << "SetMaster: Do not in Master & Slave mode";
    return false;
  }
  return master_slave_->SetMaster(master_ip, master_port, force_full_sync);
}

Status ReplicationManager::RemoveReplicationGroupNodes(const std::set<ReplicationGroupID>& group_ids) {
  Status s = RemoveReplicationGroupNodesSanityCheck(group_ids);
  if (!s.ok()) {
    return;
  }
  return node_manager_->RemoveReplicationGroupNodes(group_ids);
}

Status ReplicationManager::CreateReplicationGroupNodes(const std::set<ReplicationGroupID>& group_ids) {
  Status s = CreateReplicationGroupNodesSanityCheck(group_ids);
  if (!s.ok()) {
    return;
  }
  std::set<ReplicationGroupNodeConifg> cfgs;
  for (const auto& id : group_ids) {
    std::string table_log_path = conf_->log_path() + "log_" + id.TableName() + "/";
    std::string log_path = conf_->classic_mode()
                           ? table_log_path : table_log_path + std::to_string(id.PartitionId()) + "/";
    cfgs.insert(ReplicationGroupNodeConifg(id, std::move(log_path), conf_->consensus_level(), state_machine_));
  }
  return node_manager_->CreateReplicationGroupNodes(cfgs);
}
