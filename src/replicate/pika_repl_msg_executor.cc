// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "pika_repl_msg_executor.h"

class TaskArg {
 public:
  TaskArg(const PeerID& _peer_id, IOClosure* _done, MessageExecutor* _executor)
    : peer_id(_peer_id), done(_done), executor(_executor) { }

  virtual ~TaskArg() {
    if (done != nullptr) {
      done->Run();
    }
  }

 public:
  PeerID peer_id;
  IOClosure* done;
  MessageExecutor* executor;
}

class RequestTaskArg : public TaskArg {
 public:
  RequestTaskArg(const PeerID& _peer_id.
                 InnerMessage::InnerRequest* _req,
                 IOClosure* _done,
                 MessageExecutor* _executor)
      : TaskArg(_peer_id, _done, _executor), req(_req) { }

  ~RequestTaskArg() {

  }

 public:
  InnerMessage::InnerRequest* req;
};

class ResponseTaskArg : public TaskArg {
 public:
  ResponseTaskArg(const PeerID& _peer_id,
                  InnerMessage::InnerResponse* _response,
                  IOClosure* _done,
                  MessageExecutor* _executor)
      : TaskArg(_peer_id, _done, _executor), guard(_response) { }
  ~ResponseTaskArg() { }

 public:
  InnerMessage::InnerResponse* response;
};

class AppendTaskArg : public ResponseTaskArg {
 public:
  AppendTaskArg(const PeerID& _peer_id,
                InnerMessage::InnerResponse* _response;
                void* _arg,
                IOClosure* _done,
                MessageExecutor* _executor)
      : ResponseTaskArg(_peer_id, _response, _done, _executor), arg(_arg), { }
  ~AppendTaskArg() {
    if (arg != nullptr) {
      delete arg;
      arg == nullptr;
    }
  }

 public:
  void* arg;
};

class TaskArgGuard {
 public:
  TaskArgGuard(TaskArg* task_arg) : task_arg_(task_arg) { }

  ~TaskArgGuard() {
    if (task_arg_ != nullptr) {
      delete task_arg_;
      task_arg_ = nullptr;
    }
  }

 private:
  TaskArg* task_arg_;
};

BgWorkers::BgWorkers(int worker_num) {
  for (int i = 0; i < worker_num; ++i) {
    bg_workers_.push_back(new ReplBgWorker(PIKA_SYNC_BUFFER_SIZE));
  }
}

int BgWorkers::StartThread() {
  int res;
  for (size_t i = 0; i < bg_workers_.size(); ++i) {
    res = bg_workers_[i]->StartThread();
    if (res != pink::kSuccess) {
      break;
    }
  }
  return res;
}

int BgWorkers::Stop() {
  for (size_t i = 0; i < bg_workers_.size(); ++i) {
    bg_workers_[i]->StopThread();
  }
  return 0;
}


Executor::Executor(ReplicaManager* rm, int bg_worker_num)
  : rm_(rm) {
  threads_ = new pink::ThreadPool(PIKA_REPL_SERVER_TP_SIZE, 100000);
  concurrent_append_ = new BgWorkers(bg_worker_num);
}

Executor::~Executor() {
  if (concurrent_append_!= nullptr) {
    delete concurrent_append_;
    concurrent_append_ = nullptr;
  }
  if (threads_ != nullptr) {
    delete threads_;
    threads_ = nullptr;
  }
};

int Executor::Start() {
  res = threads_->start_thread_pool();
  if (res != pink::kSuccess) {
    LOG(FATAL) << "Start ThreadPool Error: " << res
               << (res == pink::kCreateThreadError ? ": create thread error " : ": other error");
  }
  res = concurrent_append_->StartThread();
  if (res != pink::kSuccess) {
    LOG(FATAL) << "Start Pika Repl Worker Thread Error: " << res
               << (res == pink::kCreateThreadError ? ": create thread error " : ": other error");
  }
  return res;
}

int Executor::Stop() {
  threads_->stop_thread_pool();
  concurrent_append_->StopThread();
  return 0;
}

void Executor::HandleRequest(const PeerID& peer_id, InnerMessage::InnerRequest* req, IOClosure* done) {
  RequestTaskArg* task_arg = new RequestTaskArg(peer_id, req, done, this);
  switch (req->type()) {
    case InnerMessage::kMetaSync:
    {
      threads_->Schedule(&Executor::HandleMetaSyncRequest, task_arg);
      break;
    }
    case InnerMessage::kTrySync:
    {
      threads_->Schedule(&Executor::HandleTrySyncRequest, task_arg);
      break;
    }
    case InnerMessage::kDBSync:
    {
      threads_->Schedule(&Executor::HandleDBSyncRequest, task_arg);
      break;
    }
    case InnerMessage::kBinlogSync:
    {
      threads_->Schedule(&Executor::HandleBinlogSyncRequest, task_arg);
      break;
    }
    //case InnerMessage::kRemoveSlaveNode:
    //{
    //  threads_->Schedule(&Executor::HandleRemoveSlaveNodeRequest, task_arg);
    //  break;
    //}
    default:
      break;
  }
}

void Executor::HandleResponse(const PeerID& peer_id, InnerMessage::InnerResponse* response, IOClosure* done) {
  if (response == nullptr) {
    // SyncError
    rm_->SyncError();
    return;
  }
  ResponseTaskArg* task_arg = new ResponseTaskArg(peer_id, response, done, this);
  switch (response->type()) {
    case InnerMessage::kMetaSync:
    {
      threads_->Schedule(&Executor::HandleMetaSyncResponse, static_cast<void*>(task_arg));
      break;
    }
    case InnerMessage::kDBSync:
    {
      threads_->Schedule(&Executor::HandleDBSyncResponse, static_cast<void*>(task_arg));
      break;
    }
    case InnerMessage::kTrySync:
    {
      threads_->Schedule(&Executor::HandleTrySyncResponse, static_cast<void*>(task_arg));
      break;
    }
    case InnerMessage::kBinlogSync:
    {
      DispatchBinlogRes(peer_id, response);
      break;
    }
    //case InnerMessage::kRemoveSlaveNode:
    //{
    //  threads_->Schedule(&Executor::HandleRemoveSlaveNodeResponse, static_cast<void*>(task_arg));
    //  break;
    //}
    default:
      break;
  }
  return 0;
}

void Executor::HandleMetaSyncRequest(void* arg) {
  RequestTaskArg* task_arg = static_cast<RequestTaskArg*>(arg);
  TaskArgGuard guard(task_arg);

  const InnerMessage::InnerRequest* req = task_arg->req;
  ReplicaManager* rm = task_arg->executor->rm_;

  InnerMessage::InnerRequest::MetaSync meta_sync_request = req->meta_sync();
  InnerMessage::Node node = meta_sync_request.node();
  std::string masterauth = meta_sync_request.has_auth() ? meta_sync_request.auth() : "";

  InnerMessage::InnerResponse response;
  response.set_type(InnerMessage::kMetaSync);
  if (!rm->conf_->requirepass().empty()
    && rm->conf_->requirepass() != masterauth) {
    response.set_code(InnerMessage::kError);
    response.set_reply("Auth with master error, Invalid masterauth");
  } else {
    LOG(INFO) << "Receive MetaSync, Slave ip: " << node.ip() << ", Slave port:" << node.port();
    std::vector<TableStruct> table_structs = rm->conf_->table_structs();
    bool success = rm->OnSlaveConnected(node.ip(), node.port(), conn->fd(), table_structs);
    const std::string ip_port = slash::IpPortString(node.ip(), node.port());
    //rm->ReplServerUpdateClientConnMap(ip_port, conn->fd());
    if (!success) {
      response.set_code(InnerMessage::kOther);
      response.set_reply("Slave AlreadyExist");
    } else {
      //g_pika_server->BecomeMaster();
      response.set_code(InnerMessage::kOk);
      InnerMessage::InnerResponse_MetaSync* meta_sync = response.mutable_meta_sync();
      meta_sync->set_classic_mode(rm->conf_->classic_mode());
      for (const auto& table_struct : table_structs) {
        InnerMessage::InnerResponse_MetaSync_TableInfo* table_info = meta_sync->add_tables_info();
        table_info->set_table_name(table_struct.table_name);
        table_info->set_partition_num(table_struct.partition_num);
      }
    }
  }

  std::string reply_str;
  bool should_response = response.SerializeToString(&reply_str);
  task_arg->done->SetResponse(std::move(reply_str), should_response);
  //LOG(WARNING) << "Process MetaSync request serialization failed";
}

void Executor::HandleTrySyncRequest(void* arg) {
  RequestTaskArg* task_arg = static_cast<RequestTaskArg*>(arg);
  TaskArgGuard guard(task_arg);

  const InnerMessage::InnerRequest* req = task_arg->req;
  ReplicaManager* rm = task_arg->executor->rm_;

  InnerMessage::InnerRequest::TrySync try_sync_request = req->try_sync();
  InnerMessage::Partition partition_request = try_sync_request.partition();
  InnerMessage::BinlogOffset slave_boffset = try_sync_request.binlog_offset();
  InnerMessage::Node node = try_sync_request.node();
  std::string table_name = partition_request.table_name();
  uint32_t partition_id = partition_request.partition_id();
  std::string partition_name;

  InnerMessage::InnerResponse response;
  InnerMessage::InnerResponse::TrySync* try_sync_response = response.mutable_try_sync();
  try_sync_response->set_reply_code(InnerMessage::InnerResponse::TrySync::kError);
  InnerMessage::Partition* partition_response = try_sync_response->mutable_partition();
  partition_response->set_table_name(table_name);
  partition_response->set_partition_id(partition_id);

  response.set_type(InnerMessage::Type::kTrySync);

  std::shared_ptr<ReplicationGroupNode> node = rm->GetReplicationGroupNode(ReplicationGroupID(table_name, partition_id));
  if (node == nullptr) {
    response.set_code(InnerMessage::kError);
    response.set_reply("Partition not found");
    LOG(WARNING) << "Table Name: " << table_name << " Partition ID: "
                 << partition_id << " Not Found, TrySync Error";
  } else {
    partition_name = node->PartitionName();
    LOG(INFO) << "Receive Trysync, Slave ip: " << node.ip() << ", Slave port:"
              << node.port() << ", Partition: " << partition_name << ", filenum: "
              << slave_boffset.filenum() << ", pro_offset: " << slave_boffset.offset();
    response.set_code(InnerMessage::kOk);

    node->HandleTrySyncRequest(req, response)
  }

  std::string reply_str;
  bool should_response = response.SerializeToString(&reply_str);
  task_arg->done->SetResponse(std::move(reply_str), should_response);
  //LOG(WARNING) << "Handle Try Sync Failed";
}

void Executor::HandleDBSyncRequest(void* arg) {
  RequestTaskArg* task_arg = static_cast<RequestTaskArg*>(arg);
  TaskArgGuard guard(task_arg);

  const std::shared_ptr<InnerMessage::InnerRequest> req = task_arg->req;
  ReplicaManager* rm = task_arg->executor->rm_;

  InnerMessage::InnerRequest::DBSync db_sync_request = req->db_sync();
  InnerMessage::Partition partition_request = db_sync_request.partition();
  InnerMessage::BinlogOffset slave_boffset = db_sync_request.binlog_offset();
  std::string table_name = partition_request.table_name();
  uint32_t partition_id = partition_request.partition_id();
  std::string partition_name = table_name + "_" + std::to_string(partition_id);

  InnerMessage::InnerResponse response;
  response.set_code(InnerMessage::kOk);
  response.set_type(InnerMessage::Type::kDBSync);
  InnerMessage::InnerResponse::DBSync* db_sync_response = response.mutable_db_sync();
  InnerMessage::Partition* partition_response = db_sync_response->mutable_partition();
  partition_response->set_table_name(table_name);
  partition_response->set_partition_id(partition_id);

  LOG(INFO) << "Handle partition DBSync Request";

  std::shared_ptr<ReplicationGroupNode> node = rm->GetReplicationGroupNode(ReplicationGroupID(table_name, partition_id));
  if (node == nullptr) {
    LOG(WARNING) << "ReplicationGroupNode Partition: " << table_name << ":" << partition_id
                 << ", NotFound";
  } else {
    node->HandleDBSyncRequest(req, &response);
  }

  std::string reply_str;
  bool should_response = response.SerializeToString(&reply_str);
  task_arg->done->SetResponse(std::move(reply_str), should_response);
  //LOG(WARNING) << "Handle DBSync Failed";
}

void Executor::HandleBinlogSyncRequest(void* arg) {
  RequestTaskArg* task_arg = static_cast<RequestTaskArg*>(arg);
  TaskArgGuard guard(task_arg);

  const std::shared_ptr<InnerMessage::InnerRequest> req = task_arg->req;
  ReplicaManager* rm = task_arg->executor->rm_;
  if (!req->has_binlog_sync()) {
    LOG(WARNING) << "Pb parse error";
    //conn->NotifyClose();
    return;
  }
  const std::string& table_name = binlog_req.table_name();
  uint32_t partition_id = binlog_req.partition_id();

  bool is_first_send = binlog_req.first_send();
  int32_t session_id = binlog_req.session_id();
  const InnerMessage::BinlogOffset& ack_range_start = binlog_req.ack_range_start();
  const InnerMessage::BinlogOffset& ack_range_end = binlog_req.ack_range_end();
  BinlogOffset b_range_start(ack_range_start.filenum(), ack_range_start.offset());
  BinlogOffset b_range_end(ack_range_end.filenum(), ack_range_end.offset());
  LogicOffset l_range_start(ack_range_start.term(), ack_range_start.index());
  LogicOffset l_range_end(ack_range_end.term(), ack_range_end.index());
  LogOffset range_start(b_range_start, l_range_start);
  LogOffset range_end(b_range_end, l_range_end);

  std::shared_ptr<ReplicationGroupNode> node = rm->GetReplicationGroupNode(ReplicationGroupID(table_name, partition_id));
  if (node == nullptr) {
    LOG(WARNING) << "Sync Master Partition: " << table_name << ":" << partition_id
                 << ", NotFound";
    return;
  }

  node->HandleBinlogSyncRequest(req, nullptr);

  g_pika_server->SignalAuxiliary();
  return;
}

//void PikaReplServerConn::HandleRemoveSlaveNodeRequest(void* arg) {
//  RequestTaskArg* task_arg = static_cast<RequestTaskArg*>(arg);
//  const std::shared_ptr<InnerMessage::InnerRequest> req = task_arg->req;
//  std::shared_ptr<pink::PbConn> conn = task_arg->conn;
//  if (!req->remove_slave_node_size()) {
//    LOG(WARNING) << "Pb parse error";
//    conn->NotifyClose();
//    delete task_arg;
//    return;
//  }
//  const InnerMessage::InnerRequest::RemoveSlaveNode& remove_slave_node_req = req->remove_slave_node(0);
//  const InnerMessage::Node& node = remove_slave_node_req.node();
//  const InnerMessage::Partition& partition = remove_slave_node_req.partition();
//
//  std::string table_name = partition.table_name();
//  uint32_t partition_id = partition.partition_id();
//  std::shared_ptr<SyncMasterPartition> node =
//      g_pika_rm->GetSyncMasterPartitionByName(PartitionInfo(table_name, partition_id));
//  if (!node) {
//    LOG(WARNING) << "Sync Master Partition: " << table_name << ":" << partition_id
//        << ", NotFound";
//  }
//  Status s = node->RemoveSlaveNode(node.ip(), node.port());
//
//  InnerMessage::InnerResponse response;
//  response.set_code(InnerMessage::kOk);
//  response.set_type(InnerMessage::Type::kRemoveSlaveNode);
//  InnerMessage::InnerResponse::RemoveSlaveNode* remove_slave_node_response = response.add_remove_slave_node();
//  InnerMessage::Partition* partition_response = remove_slave_node_response->mutable_partition();
//  partition_response->set_table_name(table_name);
//  partition_response->set_partition_id(partition_id);
//  InnerMessage::Node* node_response = remove_slave_node_response->mutable_node();
//  node_response->set_ip(g_pika_server->host());
//  node_response->set_port(g_pika_server->port());
//
//  std::string reply_str;
//  if (!response.SerializeToString(&reply_str)
//    || conn->WriteResp(reply_str)) {
//    LOG(WARNING) << "Remove Slave Node Failed";
//    conn->NotifyClose();
//    delete task_arg;
//    return;
//  }
//  conn->NotifyWrite();
//  delete task_arg;
//}

void Executor::HandleMetaSyncResponse(void* arg) {
  ResponseTaskArg* task_arg = static_cast<ResponseTaskArg*>(arg);
  TaskArgGuard guard(task_arg);

  InnerMessage::InnerResponse* response = task_arg->res;
  ReplicaManager* rm = task_arg->executor->rm_;

  if (response->code() == InnerMessage::kOther) {
    std::string reply = response->has_reply() ? response->reply() : "";
    // keep sending MetaSync
    LOG(WARNING) << "Meta Sync Failed: " << reply << " will keep sending MetaSync msg";
    return;
  }

  if (response->code() != InnerMessage::kOk) {
    std::string reply = response->has_reply() ? response->reply() : "";
    LOG(WARNING) << "Meta Sync Failed: " << reply;
    rm->SyncError();
    task_arg->done->NotifyClose();
    return;
  }

  const InnerMessage::InnerResponse_MetaSync meta_sync = response->meta_sync();
  if (rm->conf_->classic_mode() != meta_sync.classic_mode()) {
    LOG(WARNING) << "Self in " << (rm->conf_->classic_mode() ? "classic" : "sharding")
                 << " mode, but master in " << (meta_sync.classic_mode() ? "classic" : "sharding")
                 << " mode, failed to establish master-slave relationship";
    rm->SyncError();
    task_arg->done->NotifyClose();
    return;
  }

  std::vector<TableStruct> master_table_structs;
  for (int idx = 0; idx < meta_sync.tables_info_size(); ++idx) {
    InnerMessage::InnerResponse_MetaSync_TableInfo table_info = meta_sync.tables_info(idx);
    master_table_structs.push_back({table_info.table_name(),
            static_cast<uint32_t>(table_info.partition_num()), {0}});
  }

  std::vector<TableStruct> self_table_structs = rm->conf_->table_structs();
  if (!IsTableStructConsistent(self_table_structs, master_table_structs)) {
    LOG(WARNING) << "Self table structs(number of databases: " << self_table_structs.size()
                 << ") inconsistent with master(number of databases: " << master_table_structs.size()
                 << "), failed to establish master-slave relationship";
    rm->SyncError();
    task_arg->done->NotifyClose();
    return;
  }

  rm->OnMetaSyncResponsed();
  LOG(INFO) << "Finish to handle meta sync response";
  delete task_arg;
}

static bool Executor::IsTableStructConsistent(
    const std::vector<TableStruct>& current_tables,
    const std::vector<TableStruct>& expect_tables) {
  if (current_tables.size() != expect_tables.size()) {
    return false;
  }
  for (const auto& table_struct : current_tables) {
    if (find(expect_tables.begin(), expect_tables.end(),
                table_struct) == expect_tables.end()) {
      return false;
    }
  }
  return true;
}

void Executor::HandleDBSyncResponse(void* arg) {
  ResponseTaskArg* task_arg = static_cast<ResponseTaskArg*>(arg);
  TaskArgGuard guard(task_arg);

  InnerMessage::InnerResponse* response = task_arg->res;
  ReplicaManager* rm = task_arg->executor->rm_;

  const InnerMessage::InnerResponse_DBSync db_sync_response = response->db_sync();
  int32_t session_id = db_sync_response.session_id();
  const InnerMessage::Partition partition_response = db_sync_response.partition();
  std::string table_name = partition_response.table_name();
  uint32_t partition_id  = partition_response.partition_id();

  std::shared_ptr<ReplicationGroupNode> node =
    rm->GetReplicationGroupNode(ReplicationGroupID(table_name, partition_id));
  if (node == nullptr {
    LOG(WARNING) << "Partition: " << table_name << ":" << partition_id << " Not Found";
    return;
  }

  node->HandleDBSyncResponse(response);
}

void Executor::HandleTrySyncResponse(void* arg) {
  ResponseTaskArg* task_arg = static_cast<ResponseTaskArg*>(arg);
  TaskArgGuard guard(task_arg);

  InnerMessage::InnerResponse* response = task_arg->res;
  ReplicaManager* rm = task_arg->executor->rm_;

  if (response->code() != InnerMessage::kOk) {
    std::string reply = response->has_reply() ? response->reply() : "";
    LOG(WARNING) << "TrySync Failed: " << reply;
    return;
  }

  const InnerMessage::InnerResponse_TrySync& try_sync_response = response->try_sync();
  const InnerMessage::Partition& partition_response = try_sync_response.partition();
  std::string table_name = partition_response.table_name();
  uint32_t partition_id  = partition_response.partition_id();

  std::shared_ptr<ReplicationGroupNode> node =
    rm->GetReplicationGroupNode(ReplicationGroupID(table_name, partition_id));
  if (node == nullptr) {
    LOG(WARNING) << "Partition: " << node.ReplicationGroupID().ToString() << " Not Found";
    return;
  }

  node->HandleTrySyncResponse(response);
}

void Executor::DispatchBinlogRes(const PeerID& peer_id, InnerMessage::InnerResponse* res) {
  // partition to a bunch of binlog chips
  std::unordered_map<ReplicationGroupID, std::vector<int>*, hash_partition_info> par_binlog;
  for (int i = 0; i < res->binlog_sync_size(); ++i) {
    const InnerMessage::InnerResponse::BinlogSync& binlog_res = res->binlog_sync(i);
    // hash key: table + partition_id
    ReplicationGroupID p_info(binlog_res.partition().table_name(),
                         binlog_res.partition().partition_id());
    if (par_binlog.find(p_info) == par_binlog.end()) {
      par_binlog[p_info] = new std::vector<int>();
    }
    par_binlog[p_info]->push_back(i);
  }

  std::shared_ptr<ReplicationGroupNode> node = nullptr;
  for (auto& binlog_nums : par_binlog) {
    ReplicationGroupID group_id(binlog_nums.first.table_name, binlog_nums.first.partition_id); 
    node = rm->GetReplicationGroupNode(ReplicationGroup_id);
    if (node == nullptr) {
      LOG(WARNING) << "Follower Partition: " << ReplicationGroup_id.ToString() << " not exist";
      break;
    }
    node->SetLastRecvTime(slash::NowMicros());
    AppendTaskArg* task_arg = new AppendTaskArg(peer_id,
                                                res,
                                                reinterpret_cast<void*>(binlog_nums.second),
                                                nullptr,
                                                this);
    concurrent_append_->Schedule(group_id.ToString(), &Executor::HandleBinlogAppend, task_arg);
  }
}

static void ParseBinlogOffset(
    const InnerMessage::BinlogOffset pb_offset,
    LogOffset* offset) {
  offset->b_offset.filenum = pb_offset.filenum();
  offset->b_offset.offset = pb_offset.offset();
  offset->l_offset.term = pb_offset.term();
  offset->l_offset.index = pb_offset.index();
}

void Executor::HandleBinlogAppend(void* arg) {
  AppendTaskArg* task_arg = static_cast<AppendTaskArg*>(arg);
  TaskArgGuard guard(task_arg);

  InnerMessage::InnerResponse* res = task_arg->res;
  PeerID peer_id = task_arg->peer_id;
  ReplicaManager* rm = task_arg->executor->rm_;
  std::vector<int>* index = static_cast<std::vector<int>* >(task_arg->res_private_data);


  // table_name and partition_id in the vector are same in the bgworker,
  // because DispatchBinlogRes() have been order them. 

  std::shared_ptr<ReplicationGroupNode> node = rm->GetReplicationGroupNode(ReplicationGroupID(table_name, partition_id));
  if (node == nullptr) {
    LOG(WARNING) << "Partition " << table_name << "_" << partition_id << " Not Found";
    return;
  }
  node->HandleBinlogSyncResponse(peer_id, res, index);
}

//void PikaReplClientConn::HandleRemoveSlaveNodeResponse(void* arg) {
//  ResponseTaskArg* task_arg = static_cast<ResponseTaskArg*>(arg);
//  std::shared_ptr<pink::PbConn> conn = task_arg->conn;
//  std::shared_ptr<InnerMessage::InnerResponse> response = task_arg->res;
//  if (response->code() != InnerMessage::kOk) {
//    std::string reply = response->has_reply() ? response->reply() : "";
//    LOG(WARNING) << "Remove slave node Failed: " << reply;
//    delete task_arg;
//    return;
//  }
//  delete task_arg;
//}

