// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_RM_H_
#define PIKA_RM_H_

#include <string>
#include <memory>
#include <unordered_map>
#include <queue>
#include <vector>

#include "slash/include/slash_status.h"

#include "include/pika_binlog_reader.h"
#include "include/pika_repl_client.h"
#include "include/pika_repl_server.h"
#include "include/pika_stable_log.h"
#include "include/pika_consensus.h"
#include "include/pika_slave_node.h"

#define kBinlogSendPacketNum 40
#define kBinlogSendBatchNum 100

// unit seconds
#define kSendKeepAliveTimeout (2 * 1000000)
#define kRecvKeepAliveTimeout (20 * 1000000)

namespace replica {

using slash::Status;

class ConfPicker {
 public:
   virtual ~ConfPicker() { }
   virtual std::string requirepass() = 0;
   virtual const std::vector<TableStruct>& table_structs()  = 0; 
   virtual bool classic_mode()  = 0;
   virtual std::string log_path() = 0;
   virtual void SetWriteBinlog(const std::string& value) = 0;
   virtual int consensus_level() = 0;
};

struct Task {
  enum Type {
    kNoType      = 1;
    // Recieved from Upper
    kClientType  = 2;
    // Produced by initialization
    kRedoType    = 3;
    // Recieved from peer
    kReplicaType = 4;
  };
  ReplicationGroupID group_id;
  PeerID peer_id;
  /* 
   * The log field represents the data that should be persisted.
   */
  std::string log;
  /* The done field is invoked when the current task is committed.
   * There are two cases that the done field is nullptr:
   * (1). the task is produced by initialization.
   * (2). the task is received from remote peer(leader).
   */
  Closure* done;
  Type type;
  Task(std::string _log,
       Closure* _done,
       const ReplicationGroupID& _group_id,
       const PeerID& _peer_id,
       const Type& _type)
    : log(std::move(_log)),
    done(_done),
    group_id(_group_id),
    peer_id(_peer_id),
    type(_type) {

  }

  Task(Task&& other)
    : log(std::move(other.log)),
    done(other.done),
    group_id(std::move(other.group_id)),
    peer_id(std::move(other.peer_id)),
    type(other.type) {
      other.done = nullptr;
  }
};

class StateMachine {
 public:
  // The logs can be applied
  virtual Status OnApply(std::vecotor<MemLog::LogItem> logs) = 0;

  virtual bool OnSlaveConnected(const std::string& ip, int64_t port, int fd,
                                const std::vector<TableStruct>& table_structs) = 0;
};

class ReplicationManager : public MessageReporter {
 friend class MessageExecutor;

 public:
  ReplicationManager();
  ~ReplicationManager();

  std::shared_ptr<ReplicationGroupNode> GetReplicationGroupNode(const ReplicationGroupID& id);
  std::shared_ptr<ReplicationGroupNode> GetReplicationGroupNode(const std::string& table_name, uint32_t partition_id);

  /*
   * Master & Slave mode used
   */
  void RemoveMaster();
  bool SetMaster(std::string& master_ip, int master_port, bool force_full_sync);

  Status CreateReplicationGroupNodes(const std::set<ReplicationGroupID>& group_ids);
  Status GetReplicationGroupInfo(const ReplicationGroupID& group_id,
                                 std::stringstream& stream) {
    return node_manager_->GetReplicationGroupInfo(group_id, stream);
  }
  Status CreateReplicationGroupNodesSanityCheck(const std::set<ReplicationGroupID>& group_ids) {
    return node_manager_->CreateReplicationGroupNodesSanityCheck(group_ids);
  }
  Status RemoveReplicationGroupNodesSanityCheck(const std::set<ReplicationGroupID>& group_ids) {
    return node_manager_->RemoveReplicationGroupNodesSanityCheck(group_ids);
  }

  /*
   * Update the leader of ReplicationGroups
   */
  Status UpdateLeader(const std::set<ReplicationGroupID>& group_ids,
                      const PeerID& leader_id,
                      bool force_full_sync) {
    return node_manager_->UpdateLeader(group_ids, leader_id, force_full_sync);
  }
  /*
   * ReplicationGroupNodesSantiyCheck check if the current status match the filter.
   * @param filter: if the replication group match the filter, then return true.
   * @return if all the replication groups match the filter, then return Status::OK().
   */
  Status ReplicationGroupNodesSantiyCheck(const SanityCheckFilter& filter) {
    return node_manager_->SanityCheck(filter);
  }

 public:
  /*
   * Propose proposes that data be appended to the log.
   *
   * @param group_id: the target replication group.
   * @param task : the task that will be appended to the group.
   * */
  Status Propose(const ReplicationGroupID& group_id, const Task& task);

 public:
  // Implement MessageReporter
  virtual void HandleRequest(const PeerID& peer_id,
                             InnerMessage::InnerRequest* req,
                             IOClosure* done) override;
  virtual void HandleResponse(const PeerID& peer_id,
                              InnerMessage::InnerResponse* res,
                              IOClosure* done) override;
  virtual void ReportWriteResult(const PeerID& peer_id,
                                 const ResultType& type) override {
    if (master_slave_ != nullptr && type != MessageReporter::ResultType::kOK) {
      master_slave_->SyncError();
    }
  }
  virtual void ReportUnreachable(const PeerID& peer_id, const UnreachableReason& reason) override;

 private:

  bool OnSlaveConnected(const std::string& ip, unsigned short port, int fd
      const std::vector<TableStruct>& table_structs) {
    if (state_machine_ == nullptr) {
      return false;
    }
    state_machine_->OnSlaveConnected(ip, port, fd, table_structs);
  }

  void OnMetaSyncResponsed();
 
 private:

 private:
  MessageExecutor executor_;
  ReplicationGroupNodeManager node_manager_;
  Transporter* transporter_;
  ConfPicker* conf_;
  std::wealk_ptr<StateMachine> state_machine_;

  MasterSlaveEmulator* master_slave_;

  pink::Thread auxiliary_thread_;
};

class PikaReplicationManager {
 public:
  PikaReplicationManager();
  ~PikaReplicationManager();

  friend Cmd;

  void Start();
  void Stop();

  Status AddSyncPartitionSanityCheck(const std::set<PartitionInfo>& p_infos);
  Status AddSyncPartition(const std::set<PartitionInfo>& p_infos);
  Status RemoveSyncPartitionSanityCheck(const std::set<PartitionInfo>& p_infos);
  Status RemoveSyncPartition(const std::set<PartitionInfo>& p_infos);
  Status ActivateSyncSlavePartition(const RmNode& node, const ReplState& repl_state);
  Status DeactivateSyncSlavePartition(const PartitionInfo& p_info);
  Status SyncTableSanityCheck(const std::string& table_name);
  Status DelSyncTable(const std::string& table_name);

  // For Pika Repl Client Thread
  Status SendMetaSyncRequest();
  Status SendRemoveSlaveNodeRequest(const std::string& table, uint32_t partition_id);
  Status SendPartitionTrySyncRequest(const std::string& table_name, size_t partition_id);
  Status SendPartitionDBSyncRequest(const std::string& table_name, size_t partition_id);
  Status SendPartitionBinlogSyncAckRequest(const std::string& table, uint32_t partition_id,
                                           const LogOffset& ack_start, const LogOffset& ack_end,
                                           bool is_first_send = false);
  Status CloseReplClientConn(const std::string& ip, int32_t port);

  // For Pika Repl Server Thread
  Status SendSlaveBinlogChipsRequest(const std::string& ip, int port, const std::vector<WriteTask>& tasks);

  // For SyncMasterPartition
  std::shared_ptr<SyncMasterPartition> GetSyncMasterPartitionByName(const PartitionInfo& p_info);

  // For SyncSlavePartition
  std::shared_ptr<SyncSlavePartition> GetSyncSlavePartitionByName(const PartitionInfo& p_info);

  Status RunSyncSlavePartitionStateMachine();

  Status CheckSyncTimeout(uint64_t now);

  // To check partition info
  // For pkcluster info command
  Status GetPartitionInfo(
      const std::string& table, uint32_t partition_id, std::string* info);

  void FindCompleteReplica(std::vector<std::string>* replica);
  void FindCommonMaster(std::string* master);
  Status CheckPartitionRole(
      const std::string& table, uint32_t partition_id, int* role);

  void RmStatus(std::string* debug_info);

  static bool CheckSlavePartitionState(const std::string& ip, const int port);

  Status LostConnection(const std::string& ip, int port);

  // Update binlog win and try to send next binlog
  Status UpdateSyncBinlogStatus(const RmNode& slave, const LogOffset& offset_start, const LogOffset& offset_end);

  Status WakeUpBinlogSync();

  // write_queue related
  void ProduceWriteQueue(const std::string& ip, int port, uint32_t partition_id, const std::vector<WriteTask>& tasks);
  int ConsumeWriteQueue();
  void DropItemInWriteQueue(const std::string& ip, int port);

  // Schedule Task
  void ScheduleReplServerBGTask(pink::TaskFunc func, void* arg);
  void ScheduleReplClientBGTask(pink::TaskFunc func, void* arg);
  void ScheduleWriteBinlogTask(const std::string& table_partition,
                               const std::shared_ptr<InnerMessage::InnerResponse> res,
                               std::shared_ptr<pink::PbConn> conn, void* res_private_data);
  void ScheduleWriteDBTask(const std::shared_ptr<Cmd> cmd_ptr, const LogOffset& offset,
                           const std::string& table_name, uint32_t partition_id);

  void ReplServerRemoveClientConn(int fd);
  void ReplServerUpdateClientConnMap(const std::string& ip_port, int fd);

 private:
  void InitPartition();
  Status SelectLocalIp(const std::string& remote_ip,
                       const int remote_port,
                       std::string* const local_ip);

  pthread_rwlock_t partitions_rw_;
  std::unordered_map<PartitionInfo, std::shared_ptr<SyncMasterPartition>, hash_partition_info> sync_master_partitions_;
  std::unordered_map<PartitionInfo, std::shared_ptr<SyncSlavePartition>, hash_partition_info> sync_slave_partitions_;

  slash::Mutex  write_queue_mu_;
  // every host owns a queue
  std::unordered_map<std::string, std::unordered_map<uint32_t, std::queue<WriteTask>>> write_queues_;  // map<ip+port, map<partition_id, queue<WriteTask>>>

  PikaReplClient* pika_repl_client_;
  PikaReplServer* pika_repl_server_;
};

} // namespace replica

#endif  //  PIKA_RM_H
