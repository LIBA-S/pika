// Copyright (c) 2019-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "pika_copyset_manager.h"

using ReplState = ReplicationGroupNodeContext::ReplState;
using ReplStateCode = ReplState::Code;

Status ReplicationGroupNodeManager::CreateReplicationGroupNodes(const std::set<ReplicationGroupConfig>& cfgs) {
  slash::RWLock l(&rg_rw_, true);
  for (const auto& cfg: cfgs) {
    replication_groups_[cfg.group_id] = std::make_shared<ReplicationGroupNode>(cfg);
  }
  return Status::OK();
}

Status ReplicationGroupNodeManager::RemoveReplicationGroupNodes(const std::set<ReplicationGroupID>& p_infos) {
  slash::RWLock l(&rg_rw_, true);
  for (const auto& group_id : group_ids) {
    auto iter = replication_groups_.find(group_id);
    if (iter != replication_groups_.end()) {
      iter.second->Leave();
    }
    replication_groups_.erese(group_id);
  }
  return Status::OK();
}

std::shared_ptr<ReplicationGroupNode> ReplicationGroupNodeManager::GetReplicationGroupNode(const ReplicationGroupID& id) {
  slash::RWLock l(&rg_rw_, false);
  auto iter = replication_groups_.find(id);
  if (iter == replication_groups_.end()) {
    return nullptr;
  }
  return iter.second;
}

Status ReplicationGroupNodeManager::Propose(const ReplicationGroupID& copyset_id, const Task& task) {
  std::shared_ptr<ReplicationGroupNode> node = nullptr;
  {
    slash::RWLock l(&rg_rw_, false);
    auto iter = replication_groups_.find(copyset_id);
    if (iter == replication_groups_.end()) {
      return Status::NotFound(copyset_id.ToString());
    }
    node = iter.second;
  }
  return node->Propose(task);
}

void ReplicationGroupNodeManager::ReportUreachable(const PeerId& peer_id, const UnreachableReason& reason) {
  bool continue_to_connect = true;
  bool check_state = reason == UnreachableReason::kFdTimeout;
  std::vector<std::share_ptr<ReplicationGroupNode>> related_nodes;
  {
    slash::RWLock l(&rg_rw_, false);
    for (auto iter : replication_groups_) {
      auto node = iter.second;
      PeerId leader_id = node->LeaderId();
      ReplState repl_state = node->ReplState();
      if (leader_id.Ip() == peer_id.Ip()
          && leader_id.Port() + kPortShiftReplServer == peer_id.Port()
          && repl_state != ReplState::kError
          && (!check_state || repl_state != ReplState::kDBNoConnect) {
        LOG(INFO) << "DB: " << node->GroupID().ToString()
                  << " has been dbslaveof no one, then will not try reconnect.";
        continue_to_connect = false;
        break;
      }
      related_nodes.push(node);
    }
  }
  if (!continue_to_connect) {
    return;
  }
  LOG(WARNING) << "Master conn timeout : " << peer_id.ToString() << " try reconnect";
  for (auto node : related_nodes) {
    node->ReportUreachable(peer_id);
  }
}

void ReplicationGroupNodeManager::ActiveFollowerReplicationGroups(const PeerId& leader_id) {
  slash::RWLock l(&rg_rw_, false);
  for (auto iter : replication_groups_) {
    auto node = iter.second;
    ReplState state = node->ReplState());
    if (state.Code() != ReplStateCode::kNoConnect
        && state.Code() != ReplStateCode::kDBNoConnect]) {
      LOG(WARNING) << "Follower partition in " + state.ToString();
    }
    std::string local_ip;
    Status s = SelectLocalIp(leader_id.Ip(), leader_id.Port(), &local_ip);
    if (s.ok()) {
      node->SetLocalIp(local_ip);
      node->Activate(leader_id, s);
    } else {
      LOG(WARNING) << s.ToString();
    }
  }
}

Staus ReplicationGroupNodeManager::ActiveFollowerReplicationGroup(const std::share_ptr<ReplicationGroupNode>& node,
                                                                  const PeerId& leader_id,
                                                                  const ReplState& s) {
  ReplState state = node->ReplState();
  if (state.Code() != ReplStateCode::kNoConnect
      && state.Code() != ReplStateCode::kDBNoConnect]) {
    return Status::Corruption("Follower partition in " + state.ToString());
  }
  std::string local_ip;
  Status s = SelectLocalIp(leader_id.Ip(), leader_id.Port(), &local_ip);
  if (s.ok()) {
    node->SetLocalIp(local_ip);
    node->Activate(leader_id, s);
  }
  return s;
}

static Status SelectLocalIp(const std::string& remote_ip,
    const int remote_port, std::string* const local_ip) {
  pink::PinkCli* cli = pink::NewRedisCli();
  cli->set_connect_timeout(1500);
  if ((cli->Connect(remote_ip, remote_port, "")).ok()) {
    struct sockaddr_in laddr;
    socklen_t llen = sizeof(laddr);
    getsockname(cli->fd(), (struct sockaddr*) &laddr, &llen);
    std::string tmp_ip(inet_ntoa(laddr.sin_addr));
    *local_ip = tmp_ip;
    cli->Close();
    delete cli;
  } else {
    LOG(WARNING) << "Failed to connect remote node("
                 << remote_ip << ":" << remote_port << ")";
    delete cli;
    return Status::Corruption("connect remote node error");
  }
  return Status::OK();
}

Status ReplicationGroupNodeManager::DoSameThingEveryReplicationGroup(GroupEvent event) {
  auto current_groups = CurrentReplicationGroups();
  for (auto node : current_groups) {
    switch (event.type) {
      case TaskType::kResetReplState:
       {
         node->SetReplState(ReplState::kNoConnect);
         break;
       }
      case TaskType::kPurgeLog:
       {
          node->StableLogger()->PurgeStableLogs();
          break;
       }
      case TaskType::kRemovePeer:
       {
         node->RemovePeer(event.peer_id);
       }
      default:
        break;
    }
  }
  return Status::OK();
}

std::vector<ReplicationGroupNode> ReplicationGroupNodeManager::CurrentReplicationGroups() {
  std::vector<std::share_ptr<ReplicationGroupNode>> nodes;
  {
    slash::RWLock l(&rg_rw_, false);
    std::shared_ptr<ReplicationGroupNode> node = nullptr;
    nodes.reserve(replication_groups_.size());
    for (auto iter : replication_groups_) {
      nodes.push_back(iter.second);
    }
  }
  return std::move(nodes);
}

Status ReplicationGroupNodeManager::GetReplicationGroupInfo(const ReplicationGroupID& group_id,
                                                            std::stringstream& stream) {
  auto node = GetReplicationGroupNode(group_id);
  return node->GetInfo(stream);
}

Status ReplicationGroupNodeManager::CreateReplicationGroupNodesSanityCheck(const std::set<ReplicationGroupID>& ids) {

}

Status ReplicationGroupNodeManager::RemoveReplicationGroupNodesSanityCheck(const std::set<ReplicationGroupID>& ids) {

}

Status ReplicationGroupNodeManager::UpdateLeader(const std::set<ReplicationGroupID>& ids,
                                                 const PeerID& leader_id,
                                                 bool force_full_sync) {

  std::vector<std::share_ptr<ReplicationGroupNode> related_nodes;
  for (const auto& group_id : ids) {
    auto node = GetReplicationGroupNode(replica::ReplicationGroupID(table_name_, slot));
    if (node == nullptr) {
      return Status::Corruption(group_id.ToString());
    }
    if (node->ReplState().Code() != ReplStateCode::kConnected
        || (leader_id.empty())
        || (leader_id != node->LeaderId())) {
      related_nodes.push_back(node);
    }
  }

  Status s = Status::OK();
  ReplState state(force_full_sync
                  ? ReplStateCode::kTryDBSync
                  : ReplStateCode::kTryConnect);
  for (const auto& node : related_nodes) {
    if (node->ReplState().Code() == ReplStateCode::kConnected) {
      // TODO: Remove current repl connection.
      // s = g_pika_rm->SendRemoveSlaveNodeRequest(table_name_, slot);
    }
    if (!s.ok()) {
      break;
    }
    if (node->ReplState().Code() != ReplStateCode::kNoConnect) {
      // reset state
      node->SetReplState(ReplStateCode::kNoConnect);
    }
    if (!leader_id.empty()) {
      s = ActiveFollowerReplicationGroup(node, leader_id, state);
      if (!s.ok()) {
        break;
      }
    }
  }
  return s;
}

Status ReplicationGroupNodeManager::CreateReplicationGroupNodesSanityCheck(const std::set<ReplicationGroupID>& group_ids) {
  for (const auto& group_id : group_ids) {
    auto node = GetReplicationGroupNode(group_id);
    if (node != nullptr) {
      return Status::Corruption("replication group: " + group_id.ToString() + " already exist!");
    }
  }
  return Status::OK();
}

Status ReplicationGroupNodeManager::RemoveReplicationGroupNodesSanityCheck(const std::set<ReplicationGroupID>& group_ids) {
  Status s;
  for (const auto& group_id : group_ids) {
    auto node = GetReplicationGroupNode(group_id);
    if (node == nullptr) {
      return Status::NotFound("replication group: " + group_id.ToString());
    }
    // Lock ?
    s = node->IsSafeToBeRemoved();
    if (!s.ok()) {
      break;
    }
  }
  return s;
}
