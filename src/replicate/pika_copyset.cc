// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "pika_ReplicationGroupNode.h"

namespace replica {

using ReplStateCode = ReplicationGroupNodeContext::ReplStateCode::Code;
using RoleStateCode = ReplicationGroupNodeContext::RoleState::Code;

static ParseBinlogOffset(
    const InnerMessage::BinlogOffset pb_offset,
    LogOffset* offset) {
  offset->b_offset.filenum = pb_offset.filenum();
  offset->b_offset.offset = pb_offset.offset();
  offset->l_offset.term = pb_offset.term();
  offset->l_offset.index = pb_offset.index();
}

/* PersistentContext */

PersistentContext::PersistentContext(const std::string path) : applied_index_(), path_(path), save_(NULL) {
  pthread_rwlock_init(&rwlock_, NULL);
}

PersistentContext::~PersistentContext() {
  pthread_rwlock_destroy(&rwlock_);
  delete save_;
}

Status PersistentContext::StableSave() {
  char *p = save_->GetData();
  memcpy(p, &(applied_index_.b_offset.filenum), sizeof(uint32_t));
  p += 4;
  memcpy(p, &(applied_index_.b_offset.offset), sizeof(uint64_t));
  p += 8;
  memcpy(p, &(applied_index_.l_offset.term), sizeof(uint32_t));
  p += 4;
  memcpy(p, &(applied_index_.l_offset.index), sizeof(uint64_t));
  return Status::OK();
}

Status PersistentContext::Init() {
  if (!slash::FileExists(path_)) {
    Status s = slash::NewRWFile(path_, &save_);
    if (!s.ok()) {
      LOG(FATAL) << "PersistentContext new file failed " << s.ToString();
    }
    StableSave();
  } else {
    Status s = slash::NewRWFile(path_, &save_);
    if (!s.ok()) {
      LOG(FATAL) << "PersistentContext new file failed " << s.ToString();
    }
  }
  if (save_->GetData() != NULL) {
    memcpy((char*)(&(applied_index_.b_offset.filenum)), save_->GetData(), sizeof(uint32_t));
    memcpy((char*)(&(applied_index_.b_offset.offset)), save_->GetData() + 4, sizeof(uint64_t));
    memcpy((char*)(&(applied_index_.l_offset.term)), save_->GetData() + 12, sizeof(uint32_t));
    memcpy((char*)(&(applied_index_.l_offset.index)), save_->GetData() + 16, sizeof(uint64_t));
    return Status::OK();
  } else {
    return Status::Corruption("PersistentContext init error");
  }
}

void PersistentContext::PrepareUpdateAppliedIndex(const LogOffset& offset) {
  slash::RWLock l(&rwlock_, true);
  applied_win_.Push(SyncWinItem(offset));
}

void PersistentContext::UpdateAppliedIndex(const LogOffset& offset) {
  slash::RWLock l(&rwlock_, true);
  LogOffset cur_offset;
  applied_win_.Update(SyncWinItem(offset), SyncWinItem(offset), &cur_offset);
  if (cur_offset > applied_index_) {
    applied_index_ = cur_offset;
    StableSave();
  }
}

void PersistentContext::Reset(const LogOffset& offset) {
  slash::RWLock l(&rwlock_, true);
  applied_index_ = offset;
  applied_win_.Reset();
  StableSave();
}

/* MemLog */

MemLog::MemLog() : last_offset_() {
}

int MemLog::Size() {
  return static_cast<int>(logs_.size());
}

// purge [begin, offset]
Status MemLog::PurgeLogs(const LogOffset& offset, std::vector<LogItem>* logs) {
  slash::MutexLock l_logs(&logs_mu_);
  int index = InternalFindLogByBinlogOffset(offset);
  if (index < 0) {
    return Status::NotFound("Cant find correct index");
  }
  logs->assign(logs_.begin(), logs_.begin() + index + 1);
  logs_.erase(logs_.begin(), logs_.begin() + index + 1);
  return Status::OK();
}

// keep mem_log [mem_log.begin, offset]
Status MemLog::TruncateTo(const LogOffset& offset) {
  slash::MutexLock l_logs(&logs_mu_);
  int index = InternalFindLogByBinlogOffset(offset);
  if (index < 0) {
    return Status::Corruption("Cant find correct index");
  }
  last_offset_ = logs_[index].offset;
  logs_.erase(logs_.begin() + index + 1, logs_.end());
  return Status::OK();
}

void MemLog::Reset(const LogOffset& offset) {
  slash::MutexLock l_logs(&logs_mu_);
  logs_.erase(logs_.begin(), logs_.end());
  last_offset_ = offset;
}

Status MemLog::GetRangeLogs(int start, int end, std::vector<LogItem>* logs) {
  slash::MutexLock l_logs(&logs_mu_);
  int log_size = static_cast<int>(logs_.size());
  if (start > end || start >= log_size || end >= log_size) {
    return Status::Corruption("Invalid index");
  }
  logs->assign(logs_.begin() + start, logs_.begin() + end + 1);
  return Status::OK();
}

bool MemLog::FindLogItem(const LogOffset& offset, LogOffset* found_offset) {
  slash::MutexLock l_logs(&logs_mu_);
  int index = InternalFindLogByLogicIndex(offset);
  if (index < 0) {
    return false;
  }
  *found_offset = logs_[index].offset;
  return true;
}

int MemLog::InternalFindLogByLogicIndex(const LogOffset& offset) {
  for (size_t i = 0; i < logs_.size(); ++i) {
    if (logs_[i].offset.l_offset.index > offset.l_offset.index) {
      return -1;
    }
    if (logs_[i].offset.l_offset.index == offset.l_offset.index) {
      return i;
    }
  }
  return -1;
}

int MemLog::InternalFindLogByBinlogOffset(const LogOffset& offset) {
  for (size_t i = 0; i < logs_.size(); ++i) {
    if (logs_[i].offset > offset) {
      return -1;
    }
    if (logs_[i].offset == offset) {
      return i;
    }
  }
  return -1;
}

// ReplicationGroupNodeContext

ReplicationGroupNodeContext::ReplicationGroupNodeContext(const Config& config)
  : ps(config.log_path),
  group_id(group_id),
  consensus_level(config.consensus_level) {
}

// ReplicationGroupNode

ReplicationGroupNode::ReplicationGroupNode(const Config& config) : context_(config)
  : fsm_caller_(config.state_machine) {
  stable_logger_ = std::make_shared<StableLog>(config.group_id.TableName(),
                                               config.group_id.PartitionId(),
                                               config.log_path);
  mem_logger_ = std::make_shared<MemLog>();
  pthread_rwlock_init(&term_rwlock_, NULL);
  if (context_.consensus_level != 0) {
    Init();
  }
}

void ReplicationGroupNode::RecorverContext() {
  // load committed and applied index,
  // load current term
  context_.ps.applied_context.Init();
  context_.ps.committed_index = context_.ps.applied_context.applied_index_;
  context_.ps.term = stable_logger_->Logger()->term();
  LOG(INFO) << context_.group_id.ToString()
            << "Restore applied index " << context_.ps.applied_context.applied_index_.ToString()
            << " current term " << context_.ps.term;
}

void ReplicationGroupNode::Init() {

  RecorverContext();

  // Return if there are no more logs waitting for applied.
  if (context_.ps.committed_index == LogOffset()) {
    return;
  }

  mem_logger_->SetLastOffset(context_.ps.committed_index);
  PikaBinlogReader binlog_reader;

  // Load the unapplied logs into memory.
  int res = binlog_reader.Seek(stable_logger_->Logger(),
                               context_.ps.committed_index.b_offset.filenum,
                               context_.ps.committed_index.b_offset.offset);
  while(1) {
    LogOffset offset;
    std::string binlog;
    Status s = binlog_reader.Get(&binlog, &(offset.b_offset.filenum), &(offset.b_offset.offset));
    if (s.IsEndFile()) {
      break;
    } else if (s.IsCorruption() || s.IsIOError()) {
      LOG(FATAL) << context_.group_id.ToString() << "Read Binlog error";
    }
    BinlogItem item;
    if (!PikaBinlogTransverter::BinlogItemWithoutContentDecode(TypeFirst, binlog, &item)) {
      LOG(FATAL) << context_.group_id.ToString() << "Binlog item decode failed";
    }
    offset.l_offset.term = item.term_id();
    offset.l_offset.index = item.logic_id();
    mem_logger_->AppendLog(MemLog::LogItem(offset,
                                           Task(binlog,
                                                nullptr,
                                                context_.group_id,
                                                PeerId(),
                                                Task::Type::kRedoType));
  }
}

Status ReplicationGroupNode::AddPeer(const std::string& ip, unsigned short port, int32_t& session_id) {
  PeerId peer_id(ip, port);
  if (!progress_set_.Exist(peer_id)) {
    session_id = GenSessionId();
    if (session_id == -1) {
      return Status::Corruption("Partition: " + context_.group_id.ToString() + ", Gen Session id Failed");
    }
    auto progress = std::make_shared<Progress>(peer_id, session_id);
    progress_set_.InsertVoter(peer_id, progress);
    return Status::OK();
  }
  session_id = progress_set_.Progress(peer_id).context.session_id;
  return Status::OK();
}

Status ReplicationGroupNode::RemovePeer(constr PeerID& peer_id) {
  return progress_set_.Remove(peer_id);
}

Status ReplicationGroupNode::GetInfo(std::stringstream& stream) {
  // binlog offset section
  uint32_t filenum = 0;
  uint64_t offset = 0;
  stable_logger_->Logger()->GetProducerStatus(&filenum, &offset);
  stream << GroupID().ToString() << " binlog_offset="
         << filenum << " " << offset;

  // safety purge section
  std::string safety_purge;
  GetSafetyPurgeBinlog(&safety_purge);
  stream << ",safety_purge=" << (s.ok() ? safety_purge : "error") << "\r\n";

  if (context_.consensus_level != 0 {
    LogOffset last_log = ConsensusLastIndex();
    stream << "  consensus_last_log=" << last_log.ToString() << "\r\n";
  }

  // TODO: Lock
  if (context_.vs.role == kStateLeader) {
    stream << "  Role: Master" << "\r\n";
    progress_set_.GetInfo(stream);
  } else if (context_vs.role == kStateFollower) {
    stream << "  Role: Slave\r\n";
    stream << "  master: " << context_.vs.leader_id.ToString() << "\r\n";
    stream << "  slave status: " << context_.vs.repl_state.ToString() << "\r\n";
  }
  return Status::OK();
}

LogOffset ReplicationGroupNode::ConsensusLastIndex() {
  return mem_logger_->last_offset();
}

Status ReplicationGroupNode::IsSafeToBeRemoved() {
  slash::RWLock l(&mutex_, false);

  if (context_.vs.role.Code() == RoleStateCode::kStateLeader) {
    // Can not remove a leader with followers.
    // TODO: transfer-leadership
    if (progress_set_.NumOfMembers() > 1) {
      return Status::Corruption("the leader of " + context_.group_id.ToString() + " is in syncing");
    }
  } else if (context_.vs.role.Code() == RoleStateCode::kStateFollower) {
    const auto& repl_state = context_.vs.repl_state;
    if (repl_state.Code() != ReplStateCode::kNoConnect
        && repl_state.Code() != ReplStateCode::kError) {
      return Status::Corruption("the follower of " + context_.group_id.ToString() + " is in " + repl_state.ToString() + " state");
    }
  }
  return Status::OK();
}

Status ReplicationGroupNode::IsReady() {
  // Engouh follower
  return progress_set_.IsReady();
}

Status ReplicationGroupNode::Propose(const Task& task) {
  LogOffset log_offset;

  uint32_t filenum = 0, term = 0;
  uint64_t offset = 0, logic_id = 0;
  {
    stable_logger_->Logger()->Lock();
    // build BinlogItem
    Status s = stable_logger_->Logger()->GetProducerStatus(&filenum, &offset, &term, &logic_id);
    if (!s.ok()) {
      stable_logger_->Logger()->Unlock();
      return s;
    }
    BinlogItem item;
    item.set_exec_time(time(nullptr));
    item.set_term_id(term);
    item.set_logic_id(logic_id + 1);
    item.set_filenum(filenum);
    item.set_offset(offset);
    // make sure stable log and mem log consistent
    s = InternalAppendLog(item, task);
    if (!s.ok()) {
      stable_logger_->Logger()->Unlock();
      return s;
    }
    stable_logger_->Logger()->Unlock();
  }

  //g_pika_server->SignalAuxiliary();
  return Status::OK();
}

Status ReplicationGroupNode::Advance(const LogOffset& offset) {
  // Update applied index
  context_.ps.applied_context.UpdateAppliedIndex(offset);
  return Status::OK();
}

Status ReplicationGroupNode::InternalAppendLog(const BinlogItem& item, const Task& task) {
  LogOffset log_offset;
  Status s = InternalAppendBinlog(item, cmd_ptr, &log_offset);
  if (!s.ok()) {
    return s;
  }
  //if (context_.consensus_level == 0) {
  //  return Status::OK();
  //}
  mem_logger_->AppendLog(MemLog::LogItem(log_offset, task));
  return Status::OK();
}

Status ReplicationGroupNode::InternalAppendBinlog(const BinlogItem& item,
    const Task& task) {
  std::string binlog = PikaBinlogTransverter::BinlogEncode(
      BinlogType::TypeFirst,
      item.exec_time,
      item.term_id,
      item.logic_id,
      item.filenum,
      item.offset,
      task.log,
      {});
  Status s = stable_logger_->Logger()->Put(binlog);
  if (!s.ok()) {
    //std::string table_name = cmd_ptr->table_name().empty()
    //  ? g_pika_conf->default_table() : cmd_ptr->table_name();
    //std::shared_ptr<Table> table = g_pika_server->GetTable(table_name);
    //if (table) {
    //  table->SetBinlogIoError();
    //}
    return s;
  }
  uint32_t filenum;
  uint64_t offset;
  stable_logger_->Logger()->GetProducerStatus(&filenum, &offset);
  *log_offset = LogOffset(BinlogOffset(filenum, offset),
                          LogicOffset(item.term_id(), item.logic_id()));
  return Status::OK();
}

void ReplicationGroupNode::ReportUnreachable(const Peerid& peer_id) {
  slash::RWLock l(&mutex_, true);

  context_.vs.repl_state.SetCode(ReplStateCode::kNoConnect);
  context_.last_meta_sync_timestamp_ = time(NULL);

  auto progress = progress_set_.Progress(peer_id);
  if (progress == nullptr) {
    return;
  }
  progress->UpdateState(Progress::StateType::kStateProbe);
}

int32_t ReplicationGroupNode::GenSessionId() {
  slash::MutexLock ml(&session_mu_);
  return session_id_++;
}

Status ReplicationGroupNode::HandleDBSyncRequest(
    const InnerMessage::InnerRequest::InnerRequest* request,
    InnerMessage::InnerResponse::InnerResponse* response) {

  InnerMessage::InnerRequest::DBSync db_sync_request = request->db_sync();
  InnerMessage::InnerResponse::DBSync* db_sync_response = response->mutable_db_sync();
  InnerMessage::Node node = db_sync_request.node();
  InnerMessage::BinlogOffset follower_boffset = db_sync_request.binlog_offset();
  int32_t session_id;
  Status s = copyset_node->AddPeer(node.ip(), node.port(), &session_id);
  if (!s.ok()) {
    response.set_code(InnerMessage::kError);
    db_sync_response->set_session_id(-1);
    LOG(WARNING) << "Partition: " << context.group_id.ToString() << " Handle DBSync Request Failed, " << s.ToString();
  } else {
    db_sync_response->set_session_id(session_id);
    LOG(INFO) << "Partition: " << context.group_id.ToString() << " Handle DBSync Request Success, Session: " << session_id;
  }
  auto fsm_caller = fsm_caller_.lock();
  if (fsm_caller == nullptr) {
    return Status::NotFound("State machine");
  }
  fsm_caller->TryDBSync(mode.ip(),
                        node.port(),
                        context_.group_id.table_name,
                        context_.group_id.partition_id,
                        follower_boffset.filenum());
  return Status::OK();
}

Status ReplicationGroupNode::HandleDBSyncResponse(
    const InnerMessage::InnerResponse::InnerResponse* response) {
  // TODO: Lock
  if (response->code() != InnerMessage::kOk) {
    context_.vs.repl_state.SetCode(ReplStateCode::kError);
    std::string reply = response->has_reply() ? response->reply() : "";
    LOG(WARNING) << "DBSync Failed: " << reply;
    return Status::Corruption("DBSync response not OK");
  }

  const InnerMessage::InnerResponse_DBSync db_sync_response = response->db_sync();
  int32_t session_id = db_sync_response.session_id();

  context_.vs.session_id = session_id;
  context_.vs.repl_state.SetCode(ReplStateCode::kWaitDBSync);
  LOG(INFO) << "Partition: " << context_.group_id.ToString() << " Need Wait To Sync";
  return Status::OK();
}

void ReplicationGroupNode::HandleTrySyncRequest(const InnerMessage::InnerRequest* request,
                              InnerMessage::InnerResponse* response);
  InnerMessage::InnerRequest::TrySync try_sync_request = request->try_sync();
  InnerMessage::InnerResponse::TrySync* try_sync_response = response.mutable_try_sync();

  bool success = TrySyncOffsetCheck(request, response, try_sync_response);
  if (!success) {
    return;
  }
  TrySyncUpdateFollowerNode(try_sync_request, try_sync_response);
}

void ReplicationGroupNode::HandleTrySyncResponse(const InnerMessage::InnerResponse* response) {
  LogicOffset logic_last_offset;
  if (response->has_consensus_meta()) {
    const InnerMessage::ConsensusMeta& meta = response->consensus_meta();
    if (meta.term() > CurrentTerm()) {
      LOG(INFO) << "Update " << context_.group_id.ToString()
                << " term from " << CurrentTerm() << " to " << meta.term();
      UpdateTerm(meta.term());
    } else if (meta.term() < CurrentTerm()) /*outdated pb*/{
      LOG(WARNING) << "Drop outdated trysync response " << context_.group_id.ToString()
                   << " recv term: " << meta.term()  << " local term: " << CurrentTerm();
      return;
    }

    if (response->consensus_meta().reject()) {
      Status s = TrySyncConsensusCheck(response->consensus_meta(), partition, slave_partition);
      if (!s.ok()) {
        LOG(WARNING) << "Consensus Check failed " << s.ToString();
      }
      return;
    }

    logic_last_offset = ConsensusLastIndex().l_offset;
  }

  std::string partition_name = context_.group_id.ToString();
  if (try_sync_response.reply_code() == InnerMessage::InnerResponse::TrySync::kOk) {
    BinlogOffset boffset;
    int32_t session_id = try_sync_response.session_id();
    Logger()->GetProducerStatus(&boffset.filenum, &boffset.offset);
    SetMasterSessionId(session_id);
    LogOffset offset(boffset, logic_last_offset);
    SendPartitionBinlogSync(offset, offset, true);
    SetReplState(ReplStateCode::kConnected);
    // after connected, update receive time first to avoid connection timeout
    SetLastRecvTime(slash::NowMicros());

    LOG(INFO)    << "Partition: " << partition_name << " TrySync Ok";
  } else if (try_sync_response.reply_code() == InnerMessage::InnerResponse::TrySync::kSyncPointBePurged) {
    SetReplState(ReplStateCode::kTryDBSync);
    LOG(INFO)    << "Partition: " << partition_name << " Need To Try DBSync";
  } else if (try_sync_response.reply_code() == InnerMessage::InnerResponse::TrySync::kSyncPointLarger) {
    SetReplState(ReplStateCode::kError);
    LOG(WARNING) << "Partition: " << partition_name << " TrySync Error, Because the invalid filenum and offset";
  } else if (try_sync_response.reply_code() == InnerMessage::InnerResponse::TrySync::kError) {
    SetReplState(ReplStateCode::kError);
    LOG(WARNING) << "Partition: " << partition_name << " TrySync Error";
  }
}

Status ReplicationGroupNode::SendPartitionDBSync(const BinlogOffset& boffset) {
  const std::string& table_name = context_.group_id.table_name;
  uint32_t partition_id = context_.group_id.partition_id;
  InnerMessage::InnerRequest request;
  request.set_type(InnerMessage::kDBSync);
  InnerMessage::InnerRequest::DBSync* db_sync = request.mutable_db_sync();
  InnerMessage::Node* node = db_sync->mutable_node();
  node->set_ip(LocalIp());
  node->set_port(LocalPort());
  InnerMessage::Partition* partition = db_sync->mutable_partition();
  partition->set_table_name(table_name);
  partition->set_partition_id(partition_id);

  InnerMessage::BinlogOffset* binlog_offset = db_sync->mutable_binlog_offset();
  binlog_offset->set_filenum(boffset.filenum);
  binlog_offset->set_offset(boffset.offset);

  std::string to_send;
  if (!request.SerializeToString(&to_send)) {
    LOG(WARNING) << "Serialize Partition DBSync Request Failed, to Master " << context_.vs.leader_id.ToString();
    return Status::Corruption("Serialize Failed");
  }

  WriteTask task;
  task.from = context_.local_id;
  task.to = context_.vs.leader_id;
  task.builder = std::make_shared<SimpleMessageBuilder>(to_send);
  write_tasks_.append(task);
  return Status::OK();
}

Status ReplicationGroupNode::SendPartitionTrySync(const BinlogOffset& boffset) {
  const std::string& table_name = context_.group_id.table_name;
  uint32_t partition_id = context_.group_id.partition_id;
  InnerMessage::InnerRequest request;
  request.set_type(InnerMessage::kTrySync);
  InnerMessage::InnerRequest::TrySync* try_sync = request.mutable_try_sync();
  InnerMessage::Node* node = try_sync->mutable_node();
  node->set_ip(LocalIp());
  node->set_port(LocalPort());
  InnerMessage::Partition* partition = try_sync->mutable_partition();
  partition->set_table_name(table_name);
  partition->set_partition_id(partition_id);

  InnerMessage::BinlogOffset* binlog_offset = try_sync->mutable_binlog_offset();
  binlog_offset->set_filenum(boffset.filenum);
  binlog_offset->set_offset(boffset.offset);

  if (context_.consensus_level != 0) {
    LogOffset last_index = ConsensusLastIndex();
    uint32_t term = CurrentTerm();
    LOG(INFO) << context_.group_id.ToString() << " TrySync Increase self term from " << term << " to " << term + 1;
    term++;
    UpdateTerm(term);
    InnerMessage::ConsensusMeta* consensus_meta = request.mutable_consensus_meta();
    consensus_meta->set_term(term);
    InnerMessage::BinlogOffset* pb_offset = consensus_meta->mutable_log_offset();
    pb_offset->set_filenum(last_index.b_offset.filenum);
    pb_offset->set_offset(last_index.b_offset.offset);
    pb_offset->set_term(last_index.l_offset.term);
    pb_offset->set_index(last_index.l_offset.index);
  }

  std::string to_send;
  if (!request.SerializeToString(&to_send)) {
    LOG(WARNING) << "Serialize Partition TrySync Request Failed, to Master " << context_.vs.leader_id.ToString();
    return Status::Corruption("Serialize Failed");
  }

  WriteTask task;
  task.from = context_.local_id;
  task.to = context_.vs.leader_id;
  task.builder = std::make_shared<SimpleMessageBuilder>(to_send);
  write_tasks_.append(task);
  return Status::OK();
}

Status ReplicationGroupNode::SendPartitionBinlogSync(const LogOffset& ack_start,
                                            const LogOffset& ack_end,
                                            bool is_first_send) {
  const std::string& table_name = context_.group_id.table_name;
  uint32_t partition_id = context_.group_id.partition_id;
  InnerMessage::InnerRequest request;
  request.set_type(InnerMessage::kBinlogSync);
  InnerMessage::InnerRequest::BinlogSync* binlog_sync = request.mutable_binlog_sync();
  InnerMessage::Node* node = binlog_sync->mutable_node();
  node->set_ip(LocalIp());
  node->set_port(LocalPort());
  binlog_sync->set_table_name(table_name);
  binlog_sync->set_partition_id(partition_id);
  binlog_sync->set_first_send(is_first_send);

  InnerMessage::BinlogOffset* ack_range_start = binlog_sync->mutable_ack_range_start();
  ack_range_start->set_filenum(ack_start.b_offset.filenum);
  ack_range_start->set_offset(ack_start.b_offset.offset);
  ack_range_start->set_term(ack_start.l_offset.term);
  ack_range_start->set_index(ack_start.l_offset.index);

  InnerMessage::BinlogOffset* ack_range_end = binlog_sync->mutable_ack_range_end();
  ack_range_end->set_filenum(ack_end.b_offset.filenum);
  ack_range_end->set_offset(ack_end.b_offset.offset);
  ack_range_end->set_term(ack_end.l_offset.term);
  ack_range_end->set_index(ack_end.l_offset.index);

  int32_t session_id = context_.vs.session_id;
  binlog_sync->set_session_id(session_id);

  std::string to_send;
  if (!request.SerializeToString(&to_send)) {
    LOG(WARNING) << "Serialize Partition BinlogSync Request Failed, to Master " << context_.vs.leader_id.ToString();
    return Status::Corruption("Serialize Failed");
  }

  WriteTask task;
  task.from = context_.local_id;
  task.to = context_.vs.leader_id;
  task.builder = std::make_shared<SimpleMessageBuilder>(to_send);
  write_tasks_.append(task);
  return Status::OK();
}

Status ReplicationGroupNode::TrySyncConsensusCheck(const InnerMessage::ConsensusMeta& consensus_meta) {
  std::vector<LogOffset> hints;
  for (int i = 0; i < consensus_meta.hint_size(); ++i) {
    InnerMessage::BinlogOffset pb_offset = consensus_meta.hint(i);
    LogOffset offset;
    offset.b_offset.filenum = pb_offset.filenum();
    offset.b_offset.offset = pb_offset.offset();
    offset.l_offset.term = pb_offset.term();
    offset.l_offset.index = pb_offset.index();
    hints.push_back(offset);
  }
  LogOffset reply_offset;
  Status s = FollowerNegotiate(hints, &reply_offset);
  if (!s.ok()) {
    SetReplState(ReplStateCode::kError);
  } else {
    SetReplState(ReplStateCode::kTryConnect);
  }
  return s;
}

Status ReplicationGroupNode::UpdateProgressContext(const PeerId& peer_id, const Progress::Context& ctx) {
  std::shared_ptr<Progress> progress = progress_set_.Progress(peer_id);
  if (progress == nullptr) {
    return Status::NotFound("progress not found");
  }
  return progress.UpdateContext(ctx);
}

void ReplicationGroupNode::HandleBinlogSyncResponse(
    const PeerID& peer_id,
    const InnerMessage::InnerResponse::InnerResponse* response
    const std::vector<int>* index) {

  LogOffset pb_begin, pb_end;
  bool only_keepalive = false;

  // find the first not keepalive binlogsync
  for (size_t i = 0; i < index->size(); ++i) {
    const InnerMessage::InnerResponse::BinlogSync& binlog_res = response->binlog_sync((*index)[i]);
    if (!binlog_res.binlog().empty()) {
      ParseBinlogOffset(binlog_res.binlog_offset(), &pb_begin);
      break;
    }
  }

  // find the last not keepalive binlogsync
  for (int i = index->size() - 1; i >= 0; i--) {
    const InnerMessage::InnerResponse::BinlogSync& binlog_res = response->binlog_sync((*index)[i]);
    if (!binlog_res.binlog().empty()) {
      ParseBinlogOffset(binlog_res.binlog_offset(), &pb_end);
      break;
    }
  }

  if (pb_begin == LogOffset()) {
    only_keepalive = true;
  }

  LogOffset ack_start;
  if (only_keepalive) {
    ack_start = LogOffset();
  } else {
    ack_start = pb_begin;
  }

  // TODO Lock
  if (response->has_consensus_meta()) {
    const InnerMessage::ConsensusMeta& meta = response->consensus_meta();
    if (meta.term() > CurrentTerm()) {
      LOG(INFO) << "Update " << context_.group_id.ToString()
                << CurrentTerm() << " to " << meta.term();
      UpdateTerm(meta.term());
    } else if (meta.term() < CurrentTerm()) /*outdated pb*/{
      LOG(WARNING) << "Drop outdated binlog sync response " << context_.group_id.ToString()
                   << " recv term: " << meta.term()  << " local term: " << CurrentTerm();
      return;
    }
    if (!only_keepalive) {
      LogOffset last_offset = ConsensusLastIndex();
      LogOffset prev_offset;
      ParseBinlogOffset(response->consensus_meta().log_offset(), &prev_offset);
      if (last_offset.l_offset.index != 0 &&
          (last_offset.l_offset != prev_offset.l_offset
          || last_offset.b_offset != prev_offset.b_offset)) {
        LOG(WARNING) << "last_offset " << last_offset.ToString()
                     << " NOT equal to pb prev_offset " << prev_offset.ToString();
        context_.vs.repl_state.SetCode(ReplStateCode::kTryConnect);
        return;
      }
    }
  }

  for (size_t i = 0; i < index->size(); ++i) {
    const InnerMessage::InnerResponse::BinlogSync& binlog_res = response->binlog_sync((*index)[i]);
    // if pika are not current a slave or partition not in
    // BinlogSync state, we drop remain write binlog task
    
    //if ((g_pika_conf->classic_mode() && !(g_pika_server->role() & PIKA_ROLE_SLAVE))
    //  || ((context_.vs.repl_state != ReplStateCode::kConnected)
    //    && (context_.vs.repl_state != ReplStateCode::kWaitDBSync))) {
    //  return;
    //}

    if (context_.vs.session_id != binlog_res.session_id()) {
      LOG(WARNING) << "Check SessionId Mismatch: " << context_.vs.leader_id.ToString()
                   << " partition: " << context_.group_id.ToString()
                   << " expected_session: " << binlog_res.session_id() << ", actual_session:"
                   << context_.vs.session_id;
      context_.vs.repl_state.SetCode(ReplStateCode::kTryConnect);
      return;
    }

    // empty binlog treated as keepalive packet
    if (binlog_res.binlog().empty()) {
      continue;
    }
    BinlogItem item;
    if (!PikaBinlogTransverter::BinlogItemWithoutContentDecode(TypeFirst, binlog_res.binlog(), &item)) {
      LOG(WARNING) << "Binlog item decode failed";
      context_.vs.repl_state.SetCode(ReplStateCode::kTryConnect);
      return;
    }
    //const char* redis_parser_start = binlog_res.binlog().data() + BINLOG_ENCODE_LEN;
    //int redis_parser_len = static_cast<int>(binlog_res.binlog().size()) - BINLOG_ENCODE_LEN;
    //int processed_len = 0;
    //pink::RedisParserStatus ret = worker->redis_parser_.ProcessInputBuffer(
    //  redis_parser_start, redis_parser_len, &processed_len);
    //if (ret != pink::kRedisParserDone) {
    //  LOG(WARNING) << "Redis parser failed";
    //  context_.vs.repl_state = ReplStateCode::kTryConnect;
    //  return;
    //}

    // Append to local log storage
    Status s = ProposeReplicaLog(peer_id, binlog_res.binlog(), item);
    if (!s.ok()) {
      LOG(WARNING) << "ProposeReplicaLog failed: " << s.ToString();
      context_.vs.repl_state.SetCode(ReplStateCode::kTryConnect);
      return;
    }
  }

  if (response->has_consensus_meta()) {
    LogOffset leader_commit;
    ParseBinlogOffset(response->consensus_meta().commit(), &leader_commit);
    // Update follower commit && apply
    ProcessLocalUpdate(leader_commit);
  }

  LogOffset ack_end;
  if (only_keepalive) {
    ack_end = LogOffset();
  } else {
    LogOffset productor_status;
    // Reply Ack to master immediately
    stable_logger_->Logger()->GetProducerStatus(&productor_status.b_offset.filenum,
                                                &productor_status.b_offset.offset,
                                                &productor_status.l_offset.term,
                                                &productor_status.l_offset.index);
    ack_end = productor_status;
    ack_end.l_offset.term = pb_end.l_offset.term;
  }

  SendPartitionBinlogSync(ack_start, ack_end);
}

Status ReplicationGroupNode::ProposeReplicaLog(const PeerID& peer_id,
                                               const std::string& log,
                                               const BinlogItem& attribute) {
  // precheck if prev_offset match && drop this log if this log exist
  LogOffset last_index = mem_logger_->last_offset();
  if (attribute.logic_id() < last_index.l_offset.index) {
    LOG(WARNING) << context_.group_id.ToString()
                 << "Drop log from leader logic_id " << attribute.logic_id()
                 << " cur last index " << last_index.l_offset.index;
    return Status::OK();
  }

  {
    stable_logger_->Logger()->Lock();
    Status s = InternalAppendLog(attribute,
                                 Task(log,
                                      nullptr,
                                      context_.group_id,
                                      peer_id,
                                      Task::Type::kReplicaType));
    stable_logger_->Logger()->Unlock();
  }

  return Status::OK();
}

Status ReplicationGroupNode::ProcessLocalUpdate(const LogOffset& leader_commit) {
  //if (context_.consensus_level == 0) {
  //  return Status::OK();
  //}

  LogOffset last_index = mem_logger_->last_offset();
  LogOffset committed_index = last_index < leader_commit ? last_index : leader_commit;

  LogOffset updated_committed_index;
  bool need_update = UpdateCommittedIndex(committed_index, &updated_committed_index);
  if (need_update) {
    Status s = ScheduleApplyLog(updated_committed_index);
    if (!s.ok()) {
      return s;
    }
  }
  return Status::OK();
}

Status ReplicationGroupNode::ScheduleApplyLog(const LogOffset& committed_index) {
  // logs from PurgeLogs goes to InternalApply in order
  slash::MutexLock l(&order_mu_);
  std::vector<MemLog::LogItem> logs;
  Status s = mem_logger_->PurgeLogs(committed_index, &logs);
  if (!s.ok()) {
    return Status::NotFound("committed index not found " + committed_index.ToString());
  }
  for (auto log : logs) {
    context_.ps.applied_context.PrepareUpdateAppliedIndex(log.offset);
  }
  fsm_caller_->OnApply(logs);
  return Status::OK();
}

//Status ReplicationGroupNode::ScheduleApplyFollowerLog(const LogOffset& committed_index) {
//  // logs from PurgeLogs goes to InternalApply in order
//  slash::MutexLock l(&order_mu_);
//  std::vector<MemLog::LogItem> logs;
//  Status s = mem_logger_->PurgeLogs(committed_index, &logs);
//  if (!s.ok()) {
//    return Status::NotFound("committed index not found " + committed_index.ToString());
//  }
//  for (auto log : logs) {
//    context_->PrepareUpdateAppliedIndex(log.offset);
//    InternalApplyFollower(log);
//  }
//  return Status::OK();
//}

void ReplicationGroupNode::HandleBinlogSyncRequest(
    const InnerMessage::InnerRequest::InnerRequest* request,
    InnerMessage::InnerResponse::InnerResponse* response) {

  if (request->has_consensus_meta()) {
    const InnerMessage::ConsensusMeta& meta = request->consensus_meta();
    if (meta.term() > CurrentTerm()) {
      LOG(INFO) << "Update " << context_.group_id.ToString(); 
                << " term from " << CurrentTerm() << " to " << meta.term();
      UpdateTerm(meta.term());
    } else if (meta.term() < CurrentTerm()) /*outdated pb*/{
      LOG(WARNING) << "Drop outdated binlog sync req " <<  context_.group_id.ToString()
                   << " recv term: " << meta.term()  << " local term: " << CurrentTerm();
      return;
    }
  }

  const InnerMessage::InnerRequest::BinlogSync& binlog_req = request->binlog_sync();
  const InnerMessage::Node& node = binlog_req.node();
  bool is_first_send = binlog_req.first_send();
  int32_t session_id = binlog_req.session_id();
  
  // Update progress
  PeerId peer_id(node.ip(), node.port());
  auto ctx = Progress::Context(peer_id, session_id);
  ctx.SetLastRecvTime(slash::NowMicros());
  Status s = UpdateProgressContext(peer_id, ctx);
  if (!s.ok()) {
    LOG(WARNING) << "UpdateProgressContext failed " << peer_id.ToString();
                 << ", " << PartitionName() << " " << s.ToString();
    return;
  }

  if (is_first_send) {
    if (range_start.b_offset != range_end.b_offset) {
      LOG(WARNING) << "first binlogsync request pb argument invalid";
      return;
    }

    Status s = ActivateFollowerProgress(peer_id, range_start);
    if (!s.ok()) {
      LOG(WARNING) << "Activate Binlog Sync failed " << peer_id.ToString() << " " << s.ToString();
      return;
    }
    return;
  }

  // not the first_send the range_ack cant be 0
  // set this case as ping
  if (range_start.b_offset == BinlogOffset() && range_end.b_offset == BinlogOffset()) {
    return;
  }
  s = UpdateFollowerProgress(peer_id, range_start, range_end);
  if (!s.ok()) {
    LOG(WARNING) << "Update binlog ack failed " << context_.group_id << " " << s.ToString();
    return;
  }

  //g_pika_server->SignalAuxiliary();
  return;
}

Status ReplicationGroupNode::StepProgress(const PeerId& peer_id,
    const LogOffset& start, const LogOffset& end) {
  // 1. Update current progress, get the last commiited index
  LogOffset committed_index;
  Status s = UpdateFollowerProgress(peer_id, start, end, &committed_index);
  if (!s.ok()) {
    return s;
  }

  //if (context_.consensus_level == 0) {
  //  return Status::OK();
  //}

  // do not commit log which is not current term log
  if (committed_index.l_offset.term != term()) {
    LOG_EVERY_N(INFO, 1000) << "Will not commit log term which is not equals to current term"
                            << " To updated committed_index" << committed_index.ToString() << " current term " << term()
                            << " from " << ip << " " <<  port << " start " << start.ToString() << " end " << end.ToString();
    return Status::OK();
  }

  LogOffset updated_committed_index;
  if (UpdateCommittedIndex(committed_index, &updated_committed_index)) {
    //S = ScheduleApplyLog(updated_committed_index);
    //// updateslave could be invoked by many thread
    //// not found means a late offset pass in ScheduleApplyLog
    //// an early offset is not found
    //If (!s.ok() && !s.IsNotFound()) {
    //  return s;
    //}
    s = fsm_caller_->AppliedTo(updated_committed_index);
    if (!s.ok() && !s.IsNotFound()) {
      return s;
    }
  }

  return Status::OK();
}

bool ReplicationGroupNode::UpdateCommittedIndex(const LogOffset& follower_committed_index, LogOffset* updated_committed_index) {

  if (follower_committed_index <= context_.ps.committed_index) {
    return false;
  }
  context_.ps.committed_index = follower_committed_index;
  *updated_committed_index = follower_committed_index;
  return true;
}

Status ReplicationGroupNode::ActivateFollowerProgress(const PeerId& peer_id, const LogOffset& offset) {
  auto progress = progress_set_.Progress(peer_id);
  if (progress == nullptr) {
    return Status::NotFound(PeerId.ToString());
  }

  Status s = progress->Initialize(Logger(), offset);
  if (!s.ok()) {
    LOG(WARNING) << "" << s.ToString();
    return s;
  }
  std::vector<WriteTask> writeTask;
  s =  progress->PrepareInfligtBinlog(writeTask);
  if (!s.ok()) {
    return s;
  }
  write_tasks_.insert(write_tasks_.end(), writeTask.begin(), writeTask.end());
}

Status ReplicationGroupNode::UpdateFollowerProgress(const PeerId& peer_id, const LogOffset& range_start, const LogOffset& range_end) {
  Status s = StepProgress(peer_id, range_start, range_end);
  if (!s.ok()) {
    return s;
  }
  auto progress = progress_set_.Progress(peer_id);
  if (progress == nullptr) {
    return Status::NotFound(PeerId.ToString());
  }
  std::vector<WriteTask> writeTask;
  s =  progress->PrepareInfligtBinlog(writeTask);
  if (!s.ok()) {
    return s;
  }
  write_tasks_.insert(write_tasks_.end(), writeTask.begin(), writeTask.end());
}

bool ReplicationGroupNode::TrySyncOffsetCheck(
    const InnerMessage::InnerRequest* request,
    InnerMessage::InnerResponse* response,
    InnerMessage::InnerResponse::TrySync* try_sync_response) {
  if (request->has_consensus_meta()) {
    const InnerMessage::ConsensusMeta& meta = request->consensus_meta();
    // need to response to outdated pb, new follower count on this response to update term
    if (meta.term() > CurrentTerm()) {
      LOG(INFO) << "Update " << partition_name
                << " term from " << copyset_node->CurrentTerm()
                << " to " << meta.term();
      UpdateTerm(meta.term());
    }
    return ConsensusOffsetCheck(request->consensus_meta(), &response, try_sync_response);
  }
  return OffsetCheck(try_sync_request, try_sync_response);
}

bool ReplicationGroupNode::TrySyncUpdateFollowerNode(
    const InnerMessage::InnerRequest::TrySync* try_sync_request,
    InnerMessage::InnerResponse::TrySync* try_sync_response) {
  InnerMessage::Node node = try_sync_request.node();
  std::string partition_name = PartitionName();

  int32_t session_id;
  Status s = AddPeer(node.ip(), node.port(), &session_id);
  if (!s.ok()) {
    try_sync_response->set_reply_code(InnerMessage::InnerResponse::TrySync::kError);
    LOG(WARNING) << "Partition: " << partition_name << " TrySync Failed, " << s.ToString();
    return false;
  }
  try_sync_response->set_reply_code(InnerMessage::InnerResponse::TrySync::kOk);
  try_sync_response->set_session_id(session_id);
  LOG(INFO) << "Partition: " << partition_name << " TrySync Success, Session: " << session_id;
  return true;
}

bool ReplicationGroupNode::ConsensusOffsetCheck(
    const std::shared_ptr<ReplicationGroupNode>& copyset_node,
    const InnerMessage::ConsensusMeta& meta,
    InnerMessage::InnerResponse* response,
    InnerMessage::InnerResponse::TrySync* try_sync_response) {
  LogOffset last_log_offset;
  last_log_offset.b_offset.filenum = meta.log_offset().filenum();
  last_log_offset.b_offset.offset = meta.log_offset().offset();
  last_log_offset.l_offset.term = meta.log_offset().term();
  last_log_offset.l_offset.index = meta.log_offset().index();
  std::string partition_name = PartitionName();
  bool reject = false;
  std::vector<LogOffset> hints;
  Status s = LeaderNegotiate(last_log_offset, &reject, &hints);
  if (!s.ok()) {
    if (s.IsNotFound()) {
      LOG(INFO) << "Partition: " << partition_name << " need full sync";
      try_sync_response->set_reply_code(InnerMessage::InnerResponse::TrySync::kSyncPointBePurged);
      return false;
    } else {
      LOG(WARNING) << "Partition:" << partition_name << " error " << s.ToString();
      try_sync_response->set_reply_code(InnerMessage::InnerResponse::TrySync::kError);
      return false;
    }
  }
  try_sync_response->set_reply_code(InnerMessage::InnerResponse::TrySync::kOk);
  uint32_t term = copyset_node->CurrentTerm();
  BuildConsensusMeta(reject, hints, term, response);
  if (reject) {
    return false;
  }
  return true;
}

Status ReplicationGroupNode::LeaderNegotiate(
    const LogOffset& f_last_offset, bool* reject, std::vector<LogOffset>* hints) {
  uint64_t f_index = f_last_offset.l_offset.index;
  LOG(INFO) << context_.group_id.ToString()
            << "LeaderNeotiate follower last offset "
            << f_last_offset.ToString()
            << " first_offsert " << stable_logger_->first_offset().ToString()
            << " last_offset " << mem_logger_->last_offset().ToString();
  *reject = true;
  if (f_index > mem_logger_->last_offset().l_offset.index) {
    // hints starts from last_offset() - 100;
    Status s = GetLogsBefore(mem_logger_->last_offset().b_offset, hints);
    if (!s.ok()) {
      LOG(WARNING) << f_index << " is larger than last index "
                   << mem_logger_->last_offset().ToString()
                   << " get logs before last index failed " << s.ToString();
      return s;
    }
    LOG(INFO) << context_.group_id.ToString()
              << "follower index larger then last_offset index, get logs before "
              << mem_logger_->last_offset().ToString();
    return Status::OK();
  }
  if (f_index < stable_logger_->first_offset().l_offset.index) {
    // need full sync
    LOG(INFO) << context_.group_id.ToString()
              << f_index << " not found current first index"
              << stable_logger_->first_offset().ToString();
    return Status::NotFound("logic index");
  }
  if (f_last_offset.l_offset.index == 0) {
    *reject = false;
    return Status::OK();
  }

  LogOffset found_offset;
  Status s = FindLogicOffset(f_last_offset.b_offset, f_index, &found_offset);
  if (!s.ok()) {
    if (s.IsNotFound()) {
      LOG(INFO) << context_.group_id.ToString()
                << f_last_offset.ToString() << " not found " << s.ToString();
      return s;
    } else {
      LOG(WARNING) << context_.group_id.ToString()
                   << "find logic offset failed" << s.ToString();
      return s;
    }
  }

  if (found_offset.l_offset.term != f_last_offset.l_offset.term
      || !(f_last_offset.b_offset == found_offset.b_offset)) {
    Status s = GetLogsBefore(found_offset.b_offset, hints);
    if (!s.ok()) {
      LOG(WARNING) << context_.group_id.ToString()
                   << "Try to get logs before " << found_offset.ToString() << " failed";
      return s;
    }
    return Status::OK();
  }

  LOG(INFO) << context_.group_id.ToString()
            << "Found equal offset " << found_offset.ToString();
  *reject = false;
  return Status::OK();
}

// memlog order: committed_index , [committed_index + 1, memlogger.end()]
Status ReplicationGroupNode::FollowerNegotiate(const std::vector<LogOffset>& hints, LogOffset* reply_offset) {
  if (hints.empty()) {
    return Status::Corruption("hints empty");
  }
  LOG(INFO) << context_.group_id.ToString()
            << "FollowerNegotiate from " << hints[0].ToString() << " to " << hints[hints.size() - 1].ToString();
  if (mem_logger_->last_offset().l_offset.index < hints[0].l_offset.index) {
    *reply_offset = mem_logger_->last_offset();
    return Status::OK();
  }
  if (committed_index().l_offset.index > hints[hints.size() - 1].l_offset.index) {
    return Status::Corruption("invalid hints all smaller than committed_index");
  }
  if (mem_logger_->last_offset().l_offset.index >  hints[hints.size() - 1].l_offset.index) {
    LogOffset truncate_offset = hints[hints.size() - 1];
    // trunck to hints end
    Status s = TruncateTo(truncate_offset);
    if (!s.ok()) {
      return s;
    }
  }

  LogOffset committed = committed_index();
  for (int i = hints.size() - 1; i >= 0; i--) {
    if (hints[i].l_offset.index < committed.l_offset.index) {
      return Status::Corruption("hints less than committed index");
    }
    if (hints[i].l_offset.index == committed.l_offset.index) {
      if (hints[i].l_offset.term == committed.l_offset.term) {
        Status s = TruncateTo(hints[i]);
        if (!s.ok()) {
          return s;
        }
        *reply_offset = mem_logger_->last_offset();
        return Status::OK();
      }
    }
    LogOffset found_offset;
    bool res = mem_logger_->FindLogItem(hints[i], &found_offset);
    if (!res) {
      return Status::Corruption("hints not found " + hints[i].ToString());
    }
    if (found_offset.l_offset.term == hints[i].l_offset.term) {
      // trunk to found_offsett
      Status s = TruncateTo(found_offset);
      if (!s.ok()) {
        return s;
      }
      *reply_offset = mem_logger_->last_offset();
      return Status::OK();
    }
  }

  Status s = TruncateTo(hints[0]);
  if (!s.ok()) {
    return s;
  }
  *reply_offset = mem_logger_->last_offset();
  return Status::OK();
}

bool ReplicationGroupNode::OffsetCheck(
    const InnerMessage::InnerRequest::TrySync& try_sync_request,
    InnerMessage::InnerResponse::TrySync* try_sync_response) {
  InnerMessage::Node node = try_sync_request.node();
  InnerMessage::BinlogOffset follower_boffset = try_sync_request.binlog_offset();
  std::string partition_name = PartitionName();

  BinlogOffset boffset;
  Status s = stable_logger_->Logger()->GetProducerStatus(&(boffset.filenum), &(boffset.offset));
  if (!s.ok()) {
    try_sync_response->set_reply_code(InnerMessage::InnerResponse::TrySync::kError);
    LOG(WARNING) << "Handle TrySync, Partition: "
                 << partition_name << " Get binlog offset error, TrySync failed";
    return false;
  }
  InnerMessage::BinlogOffset* copyset_node_boffset = try_sync_response->mutable_binlog_offset();
  copyset_node_boffset->set_filenum(boffset.filenum);
  copyset_node_boffset->set_offset(boffset.offset);

  if (boffset.filenum < follower_boffset.filenum()
    || (boffset.filenum == follower_boffset.filenum() && boffset.offset < follower_boffset.offset())) {
    try_sync_response->set_reply_code(InnerMessage::InnerResponse::TrySync::kSyncPointLarger);
    LOG(WARNING) << "Follower offset is larger than mine, Follower ip: "
                 << node.ip() << ", Follower port: " << node.port() << ", Partition: "
                 << partition_name << ", filenum: " << follower_boffset.filenum()
                 << ", pro_offset_: " << follower_boffset.offset();
    return false;
  }

  std::string confile = NewFileName(stable_logger_->Logger()->filename(), follower_boffset.filenum());
  if (!slash::FileExists(confile)) {
    LOG(INFO) << "Partition: " << partition_name << " binlog has been purged, may need full sync";
    try_sync_response->set_reply_code(InnerMessage::InnerResponse::TrySync::kSyncPointBePurged);
    return false;
  }

  PikaBinlogReader reader;
  reader.Seek(stable_logger_->Logger(), follower_boffset.filenum(), follower_boffset.offset());
  BinlogOffset seeked_offset;
  reader.GetReaderStatus(&(seeked_offset.filenum), &(seeked_offset.offset));
  if (seeked_offset.filenum != follower_boffset.filenum() || seeked_offset.offset != follower_boffset.offset()) {
    try_sync_response->set_reply_code(InnerMessage::InnerResponse::TrySync::kError);
    LOG(WARNING) << "Follower offset is not a start point of cur log, Follower ip: "
                 << node.ip() << ", Follower port: " << node.port() << ", Partition: "
                 << partition_name << ", cloest start point, filenum: " << seeked_offset.filenum
                 << ", offset: " << seeked_offset.offset;
    return false;
  }
  return true;
}

void ReplicationGroupNode::BuildConsensusMeta(
    const bool& reject,
    const std::vector<LogOffset>& hints,
    const uint32_t& term,
    InnerMessage::InnerResponse* response) {
  InnerMessage::ConsensusMeta* consensus_meta = response->mutable_consensus_meta();
  consensus_meta->set_term(term);
  consensus_meta->set_reject(reject);
  if (!reject) {
    return;
  }
  for (auto hint : hints) {
    InnerMessage::BinlogOffset* offset = consensus_meta->add_hint();
    offset->set_filenum(hint.b_offset.filenum);
    offset->set_offset(hint.b_offset.offset);
    offset->set_term(hint.l_offset.term);
    offset->set_index(hint.l_offset.index);
  }
}

Status ReplicationGroupNode::GetLogsBefore(const BinlogOffset& start_offset, std::vector<LogOffset>* hints) {
  BinlogOffset traversal_end = start_offset;
  BinlogOffset traversal_start(traversal_end.filenum, 0);
  traversal_start.filenum =
    traversal_start.filenum == 0 ? 0 : traversal_start.filenum - 1;
  std::map<uint32_t, std::string> binlogs;
  if (!stable_logger_->GetBinlogFiles(&binlogs)) {
    return Status::Corruption("Get binlog files failed");
  }
  if (binlogs.find(traversal_start.filenum) == binlogs.end()) {
    traversal_start.filenum = traversal_end.filenum;
  }
  std::vector<LogOffset> res;
  Status s = GetBinlogOffset(traversal_start, traversal_end, &res);
  if (!s.ok()) {
    return s;
  }
  if (res.size() > 100) {
    res.assign(res.end() - 100, res.end());
  }
  *hints = res;
  return Status::OK();
}

Status ReplicationGroupNode::TruncateTo(const LogOffset& offset) {
  LOG(INFO) << context_.group_id.ToString() << "Truncate to " << offset.ToString();
  LogOffset founded_offset;
  Status s = FindLogicOffset(offset.b_offset, offset.l_offset.index, &founded_offset);
  if (!s.ok()) {
    return s;
  }
  LOG(INFO) << context_.group_id.ToString() << " Founded truncate pos " << founded_offset.ToString();
  LogOffset committed = committed_index();
  stable_logger_->Logger()->Lock();
  if (founded_offset.l_offset.index == committed.l_offset.index) {
    mem_logger_->Reset(committed);
  } else {
    Status s  = mem_logger_->TruncateTo(founded_offset);
    if (!s.ok()) {
      stable_logger_->Logger()->Unlock();
      return s;
    }
  }
  s = stable_logger_->TruncateTo(founded_offset);
  if (!s.ok()) {
    stable_logger_->Logger()->Unlock();
    return s;
  }
  stable_logger_->Logger()->Unlock();
  return Status::OK();
}

// get binlog offset range [start_offset, end_offset]
// start_offset 0,0 end_offset 1,129, result will include binlog (1,129)
// start_offset 0,0 end_offset 1,0, result will NOT include binlog (1,xxx)
// start_offset 0,0 end_offset 0,0, resulet will NOT include binlog(0,xxx)
Status ReplicationGroupNode::GetBinlogOffset(
    const BinlogOffset& start_offset,
    const BinlogOffset& end_offset,
    std::vector<LogOffset>* log_offset) {
  PikaBinlogReader binlog_reader;
  int res = binlog_reader.Seek(stable_logger_->Logger(),
      start_offset.filenum, start_offset.offset);
  if (res) {
    return Status::Corruption("Binlog reader init failed");
  }
  while(1) {
    BinlogOffset b_offset;
    std::string binlog;
    Status s = binlog_reader.Get(&binlog, &(b_offset.filenum), &(b_offset.offset));
    if (s.IsEndFile()) {
      return Status::OK();
    } else if (s.IsCorruption() || s.IsIOError()) {
      return Status::Corruption("Read Binlog error");
    }
    BinlogItem item;
    if (!PikaBinlogTransverter::BinlogItemWithoutContentDecode(TypeFirst, binlog, &item)) {
      return Status::Corruption("Binlog item decode failed");
    }
    LogOffset offset;
    offset.b_offset = b_offset;
    offset.l_offset.term = item.term_id();
    offset.l_offset.index = item.logic_id();
    if (offset.b_offset > end_offset) {
      return Status::OK();
    }
    log_offset->push_back(offset);
  }
  return Status::OK();
}

Status ReplicationGroupNode::FindLogicOffset(const BinlogOffset& start_offset, uint64_t target_index, LogOffset* found_offset) {
  LogOffset possible_offset;
  Status s = GetBinlogOffset(start_offset, &possible_offset);
  if (!s.ok() || possible_offset.l_offset.index != target_index) {
    if (!s.ok()) {
      LOG(INFO) << context_.group_id.ToString() << "GetBinlogOffset res: " << s.ToString();
    } else {
      LOG(INFO) << context_.group_id.ToString() << "GetBInlogOffset res: " << s.ToString()
                << " possible_offset " << possible_offset.ToString()
                << " target_index " << target_index;
    }
    return FindLogicOffsetBySearchingBinlog(start_offset, target_index, found_offset);
  }
  *found_offset = possible_offset;
  return Status::OK();
}

Status ReplicationGroupNode::FindLogicOffsetBySearchingBinlog(
    const BinlogOffset& hint_offset, uint64_t target_index, LogOffset* found_offset) {
  LOG(INFO) << context_.group_id.ToString()
            << "FindLogicOffsetBySearchingBinlog hint offset " << hint_offset.ToString()
            << " target_index " << target_index;
  BinlogOffset start_offset;
  std::map<uint32_t, std::string> binlogs;
  if (!stable_logger_->GetBinlogFiles(&binlogs)) {
    return Status::Corruption("Get binlog files failed");
  }
  if (binlogs.empty()) {
    return Status::NotFound("Binlogs is empty");
  }
  if (binlogs.find(hint_offset.filenum) == binlogs.end()) {
    start_offset = BinlogOffset(binlogs.crbegin()->first, 0);
  } else {
    start_offset = hint_offset;
  }

  uint32_t found_filenum;
  Status s = FindBinlogFileNum(binlogs, target_index, start_offset.filenum, &found_filenum);
  if (!s.ok()) {
    return s;
  }

  LOG(INFO) << context_.group_id.ToString() << "FindBinlogFilenum res " << found_filenum;
  BinlogOffset traversal_start(found_filenum, 0);
  BinlogOffset traversal_end(found_filenum + 1, 0);
  std::vector<LogOffset> offsets;
  s = GetBinlogOffset(traversal_start, traversal_end, &offsets);
  if (!s.ok()) {
    return s;
  }
  for (auto& offset : offsets) {
    if (offset.l_offset.index == target_index) {
      LOG(INFO) << context_.group_id.ToString() << "Founded " << target_index << " " << offset.ToString();
      *found_offset = offset;
      return Status::OK();
    }
  }
  return Status::NotFound("Logic index not found");
}

Status ReplicationGroupNode::FindBinlogFileNum(
    const std::map<uint32_t, std::string> binlogs,
    uint64_t target_index, uint32_t start_filenum,
    uint32_t* founded_filenum) {
  // low boundary & high boundary
  uint32_t lb_binlogs = binlogs.begin()->first;
  uint32_t hb_binlogs = binlogs.rbegin()->first;
  bool first_time_left = false;
  bool first_time_right = false;
  uint32_t filenum = start_filenum;
  while(1) {
    LogOffset first_offset;
    Status s = GetBinlogOffset(BinlogOffset(filenum, 0), &first_offset);
    if (!s.ok()) {
      return s;
    }
    if (target_index < first_offset.l_offset.index) {
      if (first_time_right) {
        // last filenum
        filenum = filenum -1;
        break;
      }
      // move left
      first_time_left = true;
      if (filenum == 0 || filenum  - 1 < lb_binlogs) {
        return Status::NotFound(std::to_string(target_index) + " hit low boundary");
      }
      filenum = filenum - 1;
    } else if (target_index > first_offset.l_offset.index) {
      if (first_time_left) {
        break;
      }
      // move right
      first_time_right = true;
      if (filenum + 1 > hb_binlogs) {
        break;
      }
      filenum = filenum + 1;
    } else {
      break;
    }
  }
  *founded_filenum = filenum;
  return Status::OK();
}

} // namespace replica
